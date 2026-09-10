"""数据出口装配：Consumer = 可嵌入宿主的消费循环。

Consumer 不是一个独立进程——脚本、FastAPI 服务、独立 worker 皆可嵌入，
进程边界归使用方。职责：init persist_hook（协议生命周期，框架保证调用
时序）→ init handler（数据出口）→ 启动 consumer/DLQ → 消费循环 →
优雅停机（flush 缓冲、leave group、关资源）→ 健康快照。
健康数据经 health_snapshot() 暴露；端点实现归使用方。
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from streamgate.consumer.dlq import DlqProducer
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.consumer.options import (
    ConsumerOptions,
    DlqOptions,
    env_float,
    env_int,
    env_str,
    resolve_tuning,
)
from streamgate.obs.logging import logger
from streamgate.obs.metrics import ConsumeMetrics
from streamgate.protocols import BatchHandler
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    collect_consumer_health,
)
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService

DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_TIMEOUT_SECONDS = 5.0
DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"

# 旧参数 → 迁移指引（收到即报错，指向 CHANGELOG 迁移指南）
_MIGRATION_HINT = (
    "the consumer API was redesigned in 1.0.0 — see the CHANGELOG.md "
    "1.0.0 migration guide: ConsumeSpec/ConsumerWorker/RecordWriter are "
    "replaced by Consumer(bootstrap_servers, topic, group_id, handler, ...)"
)


async def _lifecycle_start(component: object) -> None:
    """启动可选组件（鸭子类型防御：未实现 start() 的历史实现跳过）。"""
    start: Callable[[], Awaitable[None]] | None = getattr(component, "start", None)
    if start is not None:
        await start()


async def _lifecycle_close(component: object) -> None:
    """释放可选组件（鸭子类型防御：未实现 close() 的历史实现跳过）。"""
    close: Callable[[], Awaitable[None]] | None = getattr(component, "close", None)
    if close is not None:
        await close()


def _require_identity(
    explicit: str | None, env_key: str, arg_name: str
) -> str:
    """必填项解析：显式传值 > 环境变量；仍缺失即报错（含修复指引）。"""
    resolved = explicit or env_str(env_key)
    if not resolved:
        raise ValueError(f"{arg_name} is required: pass {arg_name}= or set {env_key}")
    return resolved


class Consumer:
    """数据出口：一个可嵌入宿主的消费循环。

    从 Kafka 读出消息，交给使用方给的处理函数；位点、重试、自愈、
    毒批隔离、优雅停机全是框架机制。四个必填项各回答一个问题：

    - bootstrap_servers：数据从哪个 Kafka 集群来（未传 → KAFKA__BOOTSTRAP_SERVERS）
    - topic：从哪个 topic 读（未传 → KAFKA__TOPIC）
    - group_id：消费组身份（位点归属；未传 → CONSUMER__GROUP_ID）
    - handler：数据到哪去——唯一出口契约 ``handler(batch, context)``：
      正常返回 = 整批处理成功（框架提交位点）；抛异常 = 按 ErrorClassifier
      分类处置（RETRY 退避重试 / POISON 定位隔离 / FATAL 停机）。
      实现须幂等。使用方对象实现 start()/close() 时生命周期由框架托管。

    batch_size=1 即单条实时；batch_size=N + flush_timeout 即攒批
    （未传 → CONSUMER__BATCH_SIZE / CONSUMER__FLUSH_TIMEOUT_SECONDS）。
    高级项（expected_type / probe / classifier / persist_hook / dlq /
    tuning 等）收口在 ``options=ConsumerOptions(...)``，不传即全默认。
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
        handler: BatchHandler | None = None,
        batch_size: int | None = None,
        flush_timeout: float | None = None,
        options: ConsumerOptions | None = None,
        **legacy: object,
    ) -> None:
        if legacy:
            raise TypeError(
                f"Consumer received unknown legacy parameter(s): "
                f"{sorted(legacy)}; {_MIGRATION_HINT}"
            )
        if handler is None:
            raise TypeError(
                "handler is required: pass an async callable "
                "handler(batch, context); only code injection is supported"
            )
        self._bootstrap_servers: str = (
            bootstrap_servers
            or env_str("KAFKA__BOOTSTRAP_SERVERS")
            or DEFAULT_BOOTSTRAP_SERVERS
        )
        self._topic: str = _require_identity(topic, "KAFKA__TOPIC", "topic")
        self._group_id: str = _require_identity(
            group_id, "CONSUMER__GROUP_ID", "group_id"
        )
        self._handler = handler
        self._batch_size: int = (
            batch_size
            if batch_size is not None
            else env_int("CONSUMER__BATCH_SIZE", DEFAULT_BATCH_SIZE)
        )
        if self._batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {self._batch_size}")
        self._flush_timeout: float = (
            flush_timeout
            if flush_timeout is not None
            else env_float(
                "CONSUMER__FLUSH_TIMEOUT_SECONDS", DEFAULT_FLUSH_TIMEOUT_SECONDS
            )
        )
        if self._flush_timeout <= 0:
            raise ValueError(
                f"flush_timeout must be > 0, got {self._flush_timeout}"
            )
        self._options = options or ConsumerOptions()
        self.runtime: ConsumeRuntime | None = None
        self._stop_event: asyncio.Event | None = None

    # ---- 装配 ----

    def _build_dlq(self, dlq_options: DlqOptions) -> DlqProducer | None:
        """DLQ 隔离 producer（启动失败仅记日志，懒启动在 quarantine 兜底）。"""
        if not dlq_options.resolved_enabled():
            return None
        return DlqProducer(
            self._bootstrap_servers,
            topic=dlq_options.resolved_topic(),
            send_retries=dlq_options.resolved_send_retries(),
            message_type=dlq_options.message_type,
        )

    async def prepare(self) -> ConsumeRuntime:
        """初始化全部组件（幂等入口）。"""
        if self.runtime is not None:
            return self.runtime

        options = self._options
        # 托管钩子/旁路组件生命周期（AdmissionPolicy 协议契约：框架保证
        # 调用时序，与 IngestGateway.start() 对齐；handler 实现对象的
        # start()/close() 同步托管）
        if options.persist_hook is not None:
            await _lifecycle_start(options.persist_hook)
        if options.health_probe is not None:
            await _lifecycle_start(options.health_probe)
        await _lifecycle_start(self._handler)

        # 初始化 Kafka consumer（启动失败不 crash，交给消费循环重连）
        tuning = resolve_tuning(options.tuning)
        consumer = KafkaConsumerService(
            self._bootstrap_servers,
            self._topic,
            self._group_id,
            auto_offset_reset=tuning.auto_offset_reset,
            max_poll_records=tuning.max_poll_records,
            session_timeout_ms=tuning.session_timeout_ms,
            max_poll_interval_ms=tuning.max_poll_interval_ms,
        )
        try:
            await consumer.start()
        except Exception as e:
            logger.error(
                "consumer_startup_failed_degraded",
                error=str(e),
                bootstrap_servers=self._bootstrap_servers,
            )

        dlq = self._build_dlq(options.dlq)
        if dlq is not None:
            await dlq.start()

        metrics = ConsumeMetrics(
            window_seconds=options.resolved_metrics_window_seconds(),
        )
        self.runtime = ConsumeRuntime(
            group_id=self._group_id,
            handler=self._handler,
            probe=options.probe,
            batch_size=self._batch_size,
            flush_timeout_seconds=self._flush_timeout,
            tuning=tuning,
            expected_type=options.expected_type,
            collapse_key=options.collapse_key,
            log_context=options.log_context,
            backlog_ttl_seconds=options.resolved_backlog_ttl_seconds(),
            consumer=consumer,
            codec=options.codec or JsonEnvelopeCodec(),
            persist_hook=options.persist_hook,
            health_probe=options.health_probe,
            dlq=dlq,
            metrics=metrics,
            error_classifier=options.classifier,
        )
        return self.runtime

    # ---- 停机 ----

    def request_stop(self) -> None:
        """请求停机（信号处理/宿主调用）。"""
        if self.runtime is not None:
            self.runtime.running = False
        if self._stop_event is not None:
            self._stop_event.set()

    def _register_stop_signals(
        self, runtime: ConsumeRuntime, stop_event: asyncio.Event
    ) -> None:
        """注册停机信号处理（优雅停机）。"""

        def _signal_handler() -> None:
            logger.info("shutdown_signal_received")
            runtime.running = False
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                # Windows 不支持 add_signal_handler，用 signal.signal 兜底
                signal.signal(sig, lambda s, f: _signal_handler())

    # ---- 运行 ----

    async def run(self) -> None:
        """消费循环入口：组件装配 + 信号处理 + 消费循环（阻塞至停机）。

        嵌入宿主即可运行，进程边界归使用方。健康 HTTP 等呈现不在此装配：
        宿主并行启动自己的暴露实现，并在 run() 返回后自行关闭。
        """
        runtime = await self.prepare()

        stop_event = asyncio.Event()
        self._stop_event = stop_event
        self._register_stop_signals(runtime, stop_event)

        consume_task = asyncio.create_task(consume_loop(runtime))

        # 等待停机信号
        await stop_event.wait()

        # 等待消费循环退出（最多 30 秒）
        try:
            await asyncio.wait_for(consume_task, timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning("shutdown_timeout_consume_loop_did_not_finish")

        await self.shutdown()
        logger.info("app_stopped")

    async def shutdown(self) -> None:
        """关闭资源（幂等）。"""
        runtime = self.runtime
        if runtime is None:
            return
        if runtime.consumer is not None:
            await runtime.consumer.stop()
        if runtime.dlq is not None:
            await runtime.dlq.stop()
        # 钩子先于旁路 health_probe 释放：RedisExistenceAdmission.close() 已
        # 关闭内部 cache 及 backfill；close() 幂等，双保险亦安全
        if runtime.persist_hook is not None:
            await _lifecycle_close(runtime.persist_hook)
        if runtime.health_probe is not None:
            await _lifecycle_close(runtime.health_probe)
        await _lifecycle_close(runtime.handler)

    # ---- 观测 ----

    async def health_snapshot(self) -> ConsumerHealthResponse:
        """健康快照（数据归框架，暴露方式归使用方）。"""
        runtime = self.runtime
        if runtime is None:
            return ConsumerHealthResponse(
                status="stopped",
                kafka="disconnected",
                output=None,
                redis=None,
                pending_count=0,
                lag=0,
                quarantined_count=0,
            )
        return await collect_consumer_health(runtime)


__all__ = ["Consumer"]
