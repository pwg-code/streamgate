"""IngestGateway：传输无关的接收编排内核（背压 → 准入 → 发送 → 发送结果钩子）。

HTTP 鉴权/路由/OpenAPI 等呈现职责归使用方适配层：process() 返回传输无关的
IngestOutcome，不抛 HTTP 异常、不构造 Response。日志事件名与字段为稳定观测
契约（与历史 HTTP 版逐字一致）。
"""

import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Generic, cast

from streamgate.config import BackpressureConfig, KafkaConfig, MetricsConfig
from streamgate.ingest.admission.in_memory import InMemoryAdmission
from streamgate.ingest.admission.no_admission import NoAdmission
from streamgate.ingest.producer import KafkaProducerService
from streamgate.obs.logging import logger
from streamgate.obs.metrics import IngestMetrics
from streamgate.protocols import (
    AdmissionPolicy,
    BackpressureSignal,
    BackpressureSnapshot,
    DecisionKind,
    IngestOutcome,
    JsonObject,
    MessageCodec,
)
from streamgate.resilience.backpressure import ManualBackpressureSignal
from streamgate.resilience.health import (
    IngestHealthResponse,
    collect_ingest_health,
)
from streamgate.specs import IngestBinding, IngestRecordT
from streamgate.transport.codec import JsonEnvelopeCodec


def _iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _optional_hook(
    admission: AdmissionPolicy[IngestRecordT], name: str
) -> Callable[[IngestRecordT], Awaitable[object]] | None:
    """按名解析可选钩子：旧版策略缺新钩子时返回 None（向后兼容）。"""
    candidate: object | None = getattr(admission, name, None)
    if not callable(candidate):
        return None
    return cast("Callable[[IngestRecordT], Awaitable[object]]", candidate)


class IngestGateway(Generic[IngestRecordT]):
    """接收机制内核：start/close 管资源生命周期，process 跑单条接收链路。"""

    def __init__(
        self,
        *,
        binding: IngestBinding[IngestRecordT],
        kafka_config: KafkaConfig,
        backpressure_config: BackpressureConfig,
        signal: BackpressureSignal | None = None,
        codec: MessageCodec | None = None,
        metrics_config: MetricsConfig | None = None,
    ) -> None:
        self.binding = binding
        self._kafka_config = kafka_config
        self._backpressure_config = backpressure_config
        self._codec = codec or JsonEnvelopeCodec()
        self._signal = signal or ManualBackpressureSignal()
        self._admission = self._resolve_admission()
        self._producer: KafkaProducerService | None = None
        self._started = False
        # 速率指标窗（构造时刻即进程启动锚点；不配置走默认 60s 窗口）
        metrics = metrics_config if metrics_config is not None else MetricsConfig()
        self._metrics = IngestMetrics(window_seconds=metrics.window_seconds)

    # ---- 组件解析（声明期）----

    @property
    def admission(self) -> AdmissionPolicy[IngestRecordT]:
        """准入策略实例（使用方查询端点复用时直接取用）。"""
        return self._admission

    def _resolve_admission(self) -> AdmissionPolicy[IngestRecordT]:
        admission = self.binding.admission
        if not isinstance(admission, str):
            return admission
        if admission == "none":
            return NoAdmission()
        if admission == "in-memory":
            # 纯内存唯一性准入（仅单进程有效；多实例拓扑请注入
            # 共享存储载体，Redis 参考实现见 examples/redis_admission/）
            binding = self.binding
            return InMemoryAdmission(
                entity_key=binding.entity_key,
                slot_key=binding.slot_key,
                summary=binding.summary,
            )
        raise ValueError(f"unknown admission shortcut: {admission!r}")

    # ---- 生命周期（宿主在 startup/shutdown 调用）----

    async def start(self) -> None:
        """启动 producer/admission/背压信号（懒连接，不抛；故障在调用时降级）。"""
        if self._started:
            return
        producer = KafkaProducerService(
            self._kafka_config,
            topic=self.binding.topic or None,
        )
        try:
            await producer.start()
        except Exception as e:
            logger.error("kafka_startup_failed", error=str(e))
            logger.warning("startup_with_degraded_kafka")
        self._producer = producer
        await self._admission.start()
        self._warn_if_trip_exceeds_ttl()
        await self._signal.start()
        self._started = True

    async def close(self) -> None:
        """释放资源（幂等）。"""
        if not self._started:
            return
        await self._signal.close()
        producer = self._producer
        if producer is not None:
            await producer.stop()
        await self._admission.close()
        self._started = False

    def _warn_if_trip_exceeds_ttl(self) -> None:
        """背压拒绝（consumption-backpressure）：启动期 TTL 预算校验。"""
        existence_ttl = self._admission.existence_ttl_seconds
        if (
            self._backpressure_config.enabled
            and existence_ttl is not None
            and self._backpressure_config.trip_seconds >= existence_ttl
        ):
            logger.warning(
                "backpressure_config_trip_not_less_than_existence_ttl",
                trip_seconds=self._backpressure_config.trip_seconds,
                existence_ttl_seconds=existence_ttl,
            )

    # ---- 接收主链路 ----

    async def process(
        self,
        record: IngestRecordT,
        *,
        overwrite: bool | None = None,
        source: str = "unknown",
    ) -> IngestOutcome:
        """接收单条记录：背压 → 准入 → Kafka 发送 → 发送结果钩子。

        overwrite=None 时按 binding.is_overwrite(record) 解析。
        未 start() 直接抛 RuntimeError（装配错误，非运行时降级）。
        """
        if not self._started:
            raise RuntimeError("IngestGateway is not started, call start() first")
        self._metrics.received.record()  # 入口总流量（含后续被拒的调用）
        binding = self.binding
        log_ctx = (
            binding.log_context(record) if binding.log_context is not None else {}
        )
        received_at = datetime.now(timezone.utc)
        if overwrite is None:
            overwrite = (
                bool(binding.is_overwrite(record))
                if binding.is_overwrite is not None
                else False
            )

        # ---- 背压拒绝：必须在存在性校验/占位之前（拒绝期零写入）----
        snapshot = await self._signal.snapshot()
        if snapshot.rejecting:
            return self._backpressure_outcome(snapshot, log_ctx, source, overwrite)

        # ---- 条件写入：检查 + 原子占位（overwrite=true 跳过，防 409 死循环）----
        if not overwrite:
            verdict = await self._admit_verdict(record, log_ctx, source)
            if verdict is not None:
                return verdict

        return await self._send_and_accept(
            record, overwrite=overwrite, received_at=received_at,
            source=source, log_ctx=log_ctx,
        )

    async def health(self) -> IngestHealthResponse:
        """健康快照（数据归框架，暴露方式归使用方）。"""
        return await collect_ingest_health(
            self._producer, self._admission, metrics=self._metrics
        )

    # ---- 内部分支 ----

    def _backpressure_outcome(
        self,
        snapshot: BackpressureSnapshot,
        log_ctx: JsonObject,
        source: str,
        overwrite: bool,
    ) -> IngestOutcome:
        """背压拒绝（拒绝期零写入：不占位、不写 Redis、不写 Kafka）。"""
        self._metrics.backpressure_rejected.record()
        binding = self.binding
        error_code = binding.backpressure_error_codes.get(
            snapshot.reason or "", binding.backpressure_default_code
        )
        logger.warning(
            "ingest_rejected_backpressure",
            **log_ctx,
            source=source,
            overwrite=overwrite,
            error_code=error_code,
        )
        return IngestOutcome.backpressure(
            snapshot.reason,
            error_code=error_code,
            detail=binding.backpressure_detail,
            retry_after=self._backpressure_config.retry_after_seconds,
        )

    async def _admit_verdict(
        self,
        record: IngestRecordT,
        log_ctx: JsonObject,
        source: str,
    ) -> IngestOutcome | None:
        """存在性校验 + 原子占位。返回 Outcome = 409/4xx/5xx 短路；None = 继续。"""
        decision = await self._admission.admit(record)
        if decision.kind is DecisionKind.CONFLICT:
            self._metrics.admission_conflict.record()
            summary = decision.summary or {}
            logger.info(
                "ingest_conflict",
                **log_ctx,
                source=source,
                existing_summary_keys=sorted(summary.keys()),
            )
            return IngestOutcome.conflict(summary)
        if decision.kind is DecisionKind.REJECT:
            info = decision.reject
            assert info is not None
            if info.log_event:
                logger.warning(info.log_event, **log_ctx, source=source, **info.log_fields)
            return IngestOutcome.rejected(info)
        return None

    async def _send_and_accept(
        self,
        record: IngestRecordT,
        *,
        overwrite: bool,
        received_at: datetime,
        source: str,
        log_ctx: JsonObject,
    ) -> IngestOutcome:
        """Kafka 发送 → 失败 502（on_send_failed，默认占位保留自愈）→ 成功钩子
        → overwrite 摘要写 → 成功日志。"""
        binding = self.binding
        entity = str(binding.entity_key(record))
        slot = str(binding.slot_key(record))
        key = (
            binding.partition_key(record)
            if binding.partition_key is not None
            else f"{entity}_{slot}"
        )
        message = self._codec.encode(
            binding.message_type,
            record.model_dump(mode="json"),
            _iso_z(received_at),
            source,
        )
        if not await self._produce(
            record, key=key, message=message, log_ctx=log_ctx, source=source
        ):
            return IngestOutcome.kafka_unavailable(
                error_code=binding.kafka_unavailable_code,
                detail=binding.kafka_unavailable_detail,
            )
        cache_updated = (
            await self._overwrite_summary_write(record) if overwrite else True
        )

        logger.info(
            "ingest_request",
            **(
                binding.request_log_context(record)
                if binding.request_log_context is not None
                else log_ctx
            ),
            received_at=_iso_z(received_at),
            source=source,
            overwrite=overwrite,
        )
        return IngestOutcome.accepted(received_at, cache_updated=cache_updated)

    async def _produce(
        self,
        record: IngestRecordT,
        *,
        key: str,
        message: JsonObject,
        log_ctx: JsonObject,
        source: str,
    ) -> bool:
        """Kafka 发送 + 指标 + 发送结果钩子。False = 失败（已记录日志与钩子）。"""
        producer = self._require_producer()
        t0 = time.monotonic()
        try:
            await producer.send(key=key, message=message)
        except Exception as e:
            self._metrics.produce_failure.record()
            logger.error(
                "kafka_send_failed",
                **log_ctx,
                error=str(e),
                source=source,
                error_code=self.binding.kafka_unavailable_code,
            )
            await self._notify_send_failed(record)
            return False
        self._metrics.produce_success.record()
        self._metrics.produce_latency.record_latency(
            (time.monotonic() - t0) * 1000.0  # send 调用到 broker 确认耗时（毫秒）
        )
        await self._notify_send_success(record)
        return True

    async def _notify_send_success(self, record: IngestRecordT) -> None:
        """每次发送成功的通知钩子（best-effort：钩子异常不影响已成功的请求）。"""
        hook = _optional_hook(self._admission, "on_send_success")
        if hook is None:
            return
        try:
            await hook(record)
        except Exception as e:
            logger.warning("admission_send_success_hook_failed", error=str(e))

    async def _notify_send_failed(self, record: IngestRecordT) -> None:
        """发送失败钩子（best-effort：默认保留占位，实现可释放换取立即重发）。"""
        hook = _optional_hook(self._admission, "on_send_failed")
        if hook is None:
            return
        try:
            await hook(record)
        except Exception as e:
            logger.warning("admission_send_failed_hook_failed", error=str(e))

    async def _overwrite_summary_write(self, record: IngestRecordT) -> bool:
        """overwrite 路径摘要写；旧版策略回退 on_accepted（向后兼容）。"""
        hook = _optional_hook(self._admission, "on_overwrite_accepted")
        if hook is None:
            legacy = _optional_hook(self._admission, "on_accepted")
            if legacy is None:
                return True
            return bool(await legacy(record))
        return bool(await hook(record))

    def _require_producer(self) -> KafkaProducerService:
        """start() 完成前不可达；与 KafkaProducerService 未启动语义一致。"""
        if self._producer is None:
            raise RuntimeError("KafkaProducerService is not started")
        return self._producer


__all__ = ["IngestGateway"]
