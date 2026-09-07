"""消费服务装配与生命周期（机制内核，无 HTTP 呈现）。

职责：init sink（可选）→ 启动缓存/consumer/DLQ → 消费循环 →
优雅停机（flush 缓冲、leave group、关资源）→ 健康快照。
健康数据经 health_snapshot() 暴露；端点实现归使用方。
"""

import asyncio
import signal

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)

from streamgate.cache.existence import RedisExistenceCache
from streamgate.config import ConsumerConfig, DbConfig, KafkaConfig, RedisConfig
from streamgate.consumer.dlq import DlqProducer
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.db.engines import async_session_factory, create_write_engine
from streamgate.db.upsert import UpsertWriter
from streamgate.obs.logging import logger
from streamgate.protocols import MessageCodec, RecordWriter
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    collect_consumer_health,
)
from streamgate.specs import ConsumeSpec
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService


class ConsumerWorker:
    def __init__(
        self,
        spec: ConsumeSpec,
        *,
        kafka_config: KafkaConfig,
        consumer_config: ConsumerConfig,
        db_config: DbConfig | None = None,
        redis_config: RedisConfig | None = None,
        cache: RedisExistenceCache | None = None,
        writer: RecordWriter | None = None,
        engine: AsyncEngine | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        codec: MessageCodec | None = None,
        existence_ttl_seconds: int | None = None,
    ) -> None:
        self.spec = spec
        self._kafka_config = kafka_config
        self._consumer_config = consumer_config
        self._db_config = db_config
        self._redis_config = redis_config
        self._cache = cache
        self._writer = writer
        self._engine = engine
        self._session_factory = session_factory
        self._codec = codec or JsonEnvelopeCodec()
        self._existence_ttl_seconds = (
            existence_ttl_seconds
            if existence_ttl_seconds is not None
            else (redis_config.existence_ttl_seconds if redis_config is not None else 18000)
        )
        self.runtime: ConsumeRuntime | None = None
        self._stop_event: asyncio.Event | None = None

    # ---- 装配 ----

    def _resolve_writer(self) -> RecordWriter | None:
        """落地目标解析：显式 sink 优先；upserts 非空时构建内置 UpsertWriter；
        on_record 模式返回 None（无写侧）。"""
        if self._writer is not None:
            return self._writer
        if not self.spec.upserts:
            return None
        if self._db_config is None:
            raise ValueError(
                "db_config is required when ConsumeSpec.upserts is set"
            )
        engine = self._engine or create_write_engine(self._db_config)
        factory = self._session_factory or async_session_factory(engine)
        return UpsertWriter(self._db_config, engine, factory, self.spec.upserts)

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

        writer = self._resolve_writer()
        if writer is not None:
            await writer.start()

        cache = self._cache
        if cache is not None:
            await cache.start()

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
            cache=cache,
            dlq=dlq,
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
        if runtime.cache is not None:
            await runtime.cache.close()
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
