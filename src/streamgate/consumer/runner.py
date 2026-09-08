"""消费服务装配与生命周期（机制内核，无 HTTP 呈现）。

职责：init persist_policy（协议生命周期，框架保证调用时序）→ init sink
（spec.sink，唯一落库路径）→ 启动 consumer/DLQ → 消费循环 →
优雅停机（flush 缓冲、leave group、关资源）→ 健康快照。
健康数据经 health_snapshot() 暴露；端点实现归使用方。
"""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from streamgate.config import ConsumerConfig, KafkaConfig
from streamgate.consumer.dlq import DlqProducer
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.obs.logging import logger
from streamgate.protocols import ErrorClassifier, MessageCodec
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    collect_consumer_health,
)
from streamgate.specs import ConsumeSpec
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService

# 积压告警阈值基准（无注入载体 TTL 时的默认预算，行为与历史一致）
DEFAULT_EXISTENCE_TTL_SECONDS = 18000


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


class ConsumerWorker:
    def __init__(
        self,
        spec: ConsumeSpec,
        *,
        kafka_config: KafkaConfig,
        consumer_config: ConsumerConfig,
        codec: MessageCodec | None = None,
        cache: object | None = None,
        existence_ttl_seconds: int | None = None,
        error_classifier: ErrorClassifier | None = None,
    ) -> None:
        """装配消费内核。

        cache：可选健康探测组件（鸭子类型：实现 check_health() 即被
        /health 快照观测，如使用方自建的准入缓存）。实现 start()/close()
        时生命周期由框架托管（prepare 启动、shutdown 释放）。
        error_classifier：写侧异常分类器注入点；未注入用
        DefaultErrorClassifier（只认通用异常——接 DB 务必注入对应分类器，
        参考 examples/sqlite_sink/）。
        """
        self.spec = spec
        self._kafka_config = kafka_config
        self._consumer_config = consumer_config
        self._cache = cache
        self._codec = codec or JsonEnvelopeCodec()
        self._existence_ttl_seconds = (
            existence_ttl_seconds
            if existence_ttl_seconds is not None
            else DEFAULT_EXISTENCE_TTL_SECONDS
        )
        self._error_classifier = error_classifier
        self.runtime: ConsumeRuntime | None = None
        self._stop_event: asyncio.Event | None = None

    # ---- 装配 ----

    def _build_dlq(self) -> DlqProducer | None:
        """DLQ 隔离 producer（启动失败仅记日志，懒启动在 quarantine 兜底）。"""
        enabled = (
            self.spec.dlq
            if self.spec.dlq is not None
            else self._consumer_config.dlq_enabled
        )
        if not enabled:
            return None
        return DlqProducer(
            self._kafka_config,
            topic=self.spec.dlq_topic or None,
            send_retries=self._consumer_config.dlq_send_retries,
            message_type=self.spec.dlq_message_type,
        )

    async def prepare(self) -> ConsumeRuntime:
        """初始化全部组件（幂等入口）。"""
        if self.runtime is not None:
            return self.runtime

        # 托管策略生命周期（AdmissionPolicy 协议契约：框架保证调用时序，
        # 与 IngestGateway.start() 对齐；旁路注入的 cache 同步托管）
        if self.spec.persist_policy is not None:
            await _lifecycle_start(self.spec.persist_policy)
        if self._cache is not None:
            await _lifecycle_start(self._cache)

        writer = self.spec.sink
        if writer is not None:
            await writer.start()

        # 初始化 Kafka consumer（启动失败不 crash，交给消费循环重连）
        consumer = KafkaConsumerService(
            self._kafka_config,
            self._consumer_config,
            topic=self.spec.topic or None,
        )
        try:
            await consumer.start()
        except Exception as e:
            logger.error(
                "consumer_startup_failed_degraded",
                error=str(e),
                bootstrap_servers=self._kafka_config.bootstrap_servers,
            )

        dlq = self._build_dlq()
        if dlq is not None:
            await dlq.start()

        self.runtime = ConsumeRuntime(
            spec=self.spec,
            consumer_config=self._consumer_config,
            writer=writer,
            consumer=consumer,
            codec=self._codec,
            existence_ttl_seconds=self._existence_ttl_seconds,
            persist_policy=self.spec.persist_policy,
            cache=self._cache,
            dlq=dlq,
            error_classifier=self._error_classifier,
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
        """独立进程入口：组件装配 + 信号处理 + 消费循环（阻塞至停机）。

        健康 HTTP 等呈现不在此装配：宿主并行启动自己的暴露实现，
        并在 run() 返回后自行关闭。
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
        # 策略先于旁路 cache 释放：RedisExistenceAdmission.close() 已关闭
        # 内部 cache 及 backfill；cache.close() 幂等，双保险亦安全
        if runtime.persist_policy is not None:
            await _lifecycle_close(runtime.persist_policy)
        if runtime.cache is not None:
            await _lifecycle_close(runtime.cache)
        if runtime.writer is not None:
            await runtime.writer.close()

    # ---- 观测 ----

    async def health_snapshot(self) -> ConsumerHealthResponse:
        """健康快照（数据归框架，暴露方式归使用方）。"""
        runtime = self.runtime
        if runtime is None:
            return ConsumerHealthResponse(
                status="stopped",
                kafka="disconnected",
                database=None,
                redis=None,
                pending_count=0,
                lag=0,
                quarantined_count=0,
            )
        return await collect_consumer_health(runtime)


__all__ = ["ConsumerWorker"]
