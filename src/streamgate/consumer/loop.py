"""消费循环：批量缓冲（size/timeout 双触发）、毒丸跳过并提交、
paused 指数退避自愈、ErrorClassifier 分类 → RETRY/POISON/FATAL 三路处置、
DLQ 探针定位隔离、停机 flush。"""

import asyncio
import time
from collections.abc import Callable, Hashable
from datetime import datetime, timezone

from aiokafka.errors import CommitFailedError
from aiokafka.structs import OffsetAndMetadata, TopicPartition

from streamgate.consumer.classifier import DefaultErrorClassifier
from streamgate.consumer.dlq import (
    BisectOutcome,
    BufferedMessage,
    DlqProducer,
    DlqSendError,
    QuarantineRequest,
    _build_quarantine_request,
    _original_message,
    locate_and_quarantine,
)
from streamgate.consumer.options import ResolvedRuntimeTuning
from streamgate.obs.logging import logger
from streamgate.obs.metrics import DEFAULT_METRICS, ConsumeMetrics, MetricsSink
from streamgate.protocols import (
    AdmissionPolicy,
    BatchHandler,
    ConsumeContext,
    Envelope,
    ErrorClassifier,
    ErrorKind,
    JsonObject,
    MessageCodec,
    Probe,
)
from streamgate.transport.kafka import KafkaConsumerService, KafkaRecord


def _backlog_level(age_seconds: float, backlog_ttl_seconds: int) -> str:
    """积压时长告警阈值：WARN > TTL/2（窗口打开前最后干预机会）；ERROR > TTL*0.8。"""
    if age_seconds > backlog_ttl_seconds * 0.8:
        return "error"
    if age_seconds > backlog_ttl_seconds / 2:
        return "warning"
    return "ok"


class ConsumeRuntime:
    """消费循环运行时状态（非线程安全，单 event loop 内安全）。"""

    def __init__(
        self,
        *,
        group_id: str,
        handler: BatchHandler,
        probe: Probe | None,
        batch_size: int,
        flush_timeout_seconds: float,
        tuning: ResolvedRuntimeTuning,
        expected_type: str | None,
        collapse_key: Callable[[JsonObject], Hashable] | None,
        log_context: Callable[[JsonObject], JsonObject] | None,
        backlog_ttl_seconds: int,
        consumer: KafkaConsumerService | None,
        codec: MessageCodec,
        persist_hook: AdmissionPolicy[JsonObject] | None = None,
        health_probe: object | None = None,
        dlq: DlqProducer | None = None,
        metrics_sink: MetricsSink | None = None,
        metrics: ConsumeMetrics | None = None,
        error_classifier: ErrorClassifier | None = None,
    ) -> None:
        self.group_id = group_id
        self.handler = handler
        self.probe = probe
        self.batch_size = batch_size
        self.flush_timeout_seconds = flush_timeout_seconds
        self.tuning = tuning
        self.expected_type = expected_type
        self.collapse_key = collapse_key
        self.log_context = log_context
        self.backlog_ttl_seconds = backlog_ttl_seconds
        self.consumer = consumer
        self.codec = codec
        self.persist_hook = persist_hook
        self.health_probe = health_probe
        self.dlq = dlq
        self.metrics_sink = metrics_sink or DEFAULT_METRICS
        # 速率指标窗（默认构造 60s 窗口；构造时刻即进程启动锚点）
        self.metrics = metrics if metrics is not None else ConsumeMetrics()
        self.error_classifier = error_classifier or DefaultErrorClassifier()
        self.buffer: list[BufferedMessage] = []
        self.last_handle_at: datetime | None = None
        self.last_commit_at: datetime | None = None
        # --- 积压时长监控 ---
        self.backlog_oldest_ts: float | None = None      # 最老未处理消息（epoch 秒）
        self.last_backlog_check_at: datetime | None = None
        self.running: bool = True
        self.paused: bool = False
        self.recover_attempt: int = 0
        self.reconnect_attempt: int = 0
        # --- 坏数据隔离（DLQ）---
        self.quarantined_count: int = 0               # 进程生命周期累计隔离条数

    @property
    def pending_count(self) -> int:
        return len(self.buffer)

    def context_snapshot(self) -> ConsumeContext:
        """handler 只读快照（位点/commit 不开放）。"""
        backlog_age = (
            time.time() - self.backlog_oldest_ts
            if self.backlog_oldest_ts is not None
            else None
        )
        return ConsumeContext(
            lag=self.consumer.lag if self.consumer is not None else 0,
            pending_count=self.pending_count,
            paused=self.paused,
            quarantined_count=self.quarantined_count,
            backlog_age_seconds=backlog_age,
        )


def _note_backlog_ts(runtime: ConsumeRuntime, timestamp_ms: int | None) -> None:
    """记录进入缓冲区的消息时间戳，保留最老值。

    近似语义：缓冲区最老消息时间戳 = 最老未处理消息
    （paused 期间不再 poll，缓冲区即积压下界；Kafka 侧未拉取消息只会更晚到达）。
    """
    if timestamp_ms is None:
        return
    ts = timestamp_ms / 1000.0
    if runtime.backlog_oldest_ts is None or ts < runtime.backlog_oldest_ts:
        runtime.backlog_oldest_ts = ts


def _check_backlog_age(runtime: ConsumeRuntime) -> None:
    """周期性积压检查：指标输出 + 阈值告警。"""
    if runtime.backlog_oldest_ts is not None:
        age = time.time() - runtime.backlog_oldest_ts
        ttl = runtime.backlog_ttl_seconds
        fields = {
            "backlog_age_seconds": round(age, 1),
            "pending_count": runtime.pending_count,
            "backlog_ttl_seconds": ttl,
        }
        logger.info("backlog_age_seconds", **fields)
        level = _backlog_level(age, ttl)
        if level == "error":
            logger.error("backlog_age_critical", **fields)
        elif level == "warning":
            logger.warning("backlog_age_warn", **fields)


def _dedup_by_collapse_key(
    batch: list[BufferedMessage],
    collapse_key: Callable[[JsonObject], Hashable],
) -> list[BufferedMessage]:
    """按 collapse_key 去重保留最后一条（与处理侧防御性去重同序）。"""
    seen: dict[Hashable, BufferedMessage] = {}
    for m in batch:
        seen[collapse_key(m.data)] = m  # 后者覆盖前者
    return list(seen.values())


async def _notify_handled(runtime: ConsumeRuntime, batch: list[BufferedMessage]) -> None:
    """整批处理成功 → 权威刷新/on_persisted 钩子（best-effort，策略内部兜底异常）。"""
    policy = runtime.persist_hook
    if policy is None:
        return
    records = (
        _dedup_by_collapse_key(batch, runtime.collapse_key)
        if runtime.collapse_key is not None
        else batch
    )
    for m in records:
        try:
            await policy.on_persisted(m.data)
        except Exception as e:
            logger.warning("on_persisted_failed", error=str(e))


async def _commit_quietly(runtime: ConsumeRuntime, batch_size: int) -> None:
    """处理后提交位点：CommitFailed 只记日志不重试处理。"""
    consumer = runtime.consumer
    assert consumer is not None  # 装配点保证（prepare 必建 consumer）
    try:
        await consumer.commit()
    except CommitFailedError as e:
        logger.warning(
            "offset_commit_failed_after_handle",
            error=str(e),
            batch_size=batch_size,
        )


def _note_handle_success(
    runtime: ConsumeRuntime, batch_size: int, duration_ms: float
) -> None:
    """处理成功埋点：处理条数 + 单批耗时（仅成功批次记延迟，防超时污染 avg/max）。"""
    runtime.metrics.handled.record(batch_size)
    runtime.metrics.handle_latency.record_latency(duration_ms)


def _note_handle_failure(runtime: ConsumeRuntime, batch_size: int) -> None:
    """处理失败埋点：失败条数 + 重试次数（每次捕获异常各计一次）。"""
    runtime.metrics.handle_failed.record(batch_size)
    runtime.metrics.retries.record(1)


def _abort_fatal(runtime: ConsumeRuntime, exc: Exception) -> None:
    """FATAL 处置：停机告警（消费循环退出，交由进程管理器/人工介入）。"""
    logger.error("consumer_fatal_error_stopping", error=str(exc))
    runtime.running = False


async def _try_handle_batch(runtime: ConsumeRuntime) -> bool:
    """尝试处理当前缓冲区。返回 True 表示成功，False 表示失败。

    唯一处理路径：handler(batch, context) 正常返回 = 整批处理成功
    （提交位点）；抛异常 = 按 ErrorClassifier 分类处置——
    RETRY 退避重试 → POISON 立即转定位隔离 → FATAL 停机。
    """
    if not runtime.buffer:
        return True
    batch: list[BufferedMessage] = list(runtime.buffer)
    batch_size = len(batch)
    records = [m.data for m in batch]
    context = runtime.context_snapshot()
    logger.info("batch_handle_start", batch_size=batch_size)

    poison_error: Exception | None = None
    for attempt in range(runtime.tuning.max_retries):
        try:
            t0 = time.monotonic()
            await runtime.handler(records, context)
            duration_ms = round((time.monotonic() - t0) * 1000, 1)
            _note_handle_success(runtime, batch_size, duration_ms)
            runtime.last_handle_at = datetime.now(timezone.utc)
            runtime.buffer.clear()                      # 先清 buffer（已处理成功）
            runtime.backlog_oldest_ts = None            # 积压已清（下一批重新计）
            await _notify_handled(runtime, batch)
            await _commit_quietly(runtime, batch_size)  # 再 commit，失败只记日志
            runtime.last_commit_at = datetime.now(timezone.utc)
            logger.info(
                "batch_handle_success",
                batch_size=batch_size,
                duration_ms=duration_ms,
            )
            return True
        except Exception as e:
            _note_handle_failure(runtime, batch_size)
            kind = runtime.error_classifier.classify(e, attempt)
            backoff = runtime.tuning.retry_backoff_base * (2**attempt)
            logger.error(
                "batch_handle_failed",
                error=str(e),
                batch_size=batch_size,
                retry_count=attempt + 1,
                max_retries=runtime.tuning.max_retries,
                backoff_seconds=backoff,
                error_kind=kind.value,
            )
            if kind is ErrorKind.FATAL:
                _abort_fatal(runtime, e)
                return False
            if kind is ErrorKind.POISON:
                poison_error = e  # 毒批：重试无意义，立即转定位隔离
                break
            if attempt < runtime.tuning.max_retries - 1:
                await asyncio.sleep(backoff)

    if poison_error is not None:
        return await _handle_poison_batch(runtime, batch, poison_error)
    logger.error(
        "batch_handle_exhausted_retries",
        batch_size=batch_size,
        max_retries=runtime.tuning.max_retries,
    )
    return False  # RETRY 耗尽：paused 自愈


def _log_quarantined(
    runtime: ConsumeRuntime, record: BufferedMessage, request: QuarantineRequest
) -> None:
    """单条隔离日志（含使用方注入的 log_context 扩展字段）。"""
    log_ctx = (
        runtime.log_context(record.data) if runtime.log_context is not None else {}
    )
    logger.error(
        "consumer_record_quarantined",
        **log_ctx,
        partition=request.partition,
        offset=request.offset,
        reason=request.category,
        error=request.error,
    )


async def _quarantine_whole_batch(
    runtime: ConsumeRuntime,
    batch: list[BufferedMessage],
    poison_error: Exception,
) -> bool:
    """无探针的 POISON 处置：整批隔离后提交位点（不重试毒批）。

    隔离中途失败（DlqSendError）→ 不提交位点，整批转 paused 自愈
    （下轮重试整批隔离；已隔离条目会重复进 DLQ，留档幂等可接受）。
    """
    dlq = runtime.dlq
    assert dlq is not None  # 调用点保证（DLQ 关闭走 paused 分支，不进本函数）
    error = str(poison_error)
    quarantined: list[tuple[BufferedMessage, QuarantineRequest]] = []
    try:
        for m in batch:
            request = _build_quarantine_request(m, ErrorKind.POISON, error)
            await dlq.quarantine(request)
            quarantined.append((m, request))
    except DlqSendError:
        # dlq_send_failed 已由 DlqProducer 记日志
        return False
    runtime.buffer.clear()
    runtime.backlog_oldest_ts = None
    runtime.last_handle_at = datetime.now(timezone.utc)
    runtime.quarantined_count += len(quarantined)
    runtime.metrics.handle_failed.record(len(quarantined))
    for m, request in quarantined:
        _log_quarantined(runtime, m, request)
    await _commit_quietly(runtime, len(batch))
    runtime.last_commit_at = datetime.now(timezone.utc)
    logger.info(
        "batch_quarantined_without_probe",
        batch_size=len(batch),
        quarantined=len(quarantined),
    )
    return True


async def _handle_poison_batch(
    runtime: ConsumeRuntime,
    batch: list[BufferedMessage],
    poison_error: Exception,
) -> bool:
    """POISON 处置矩阵（分类器已判定 POISON，重试无意义）：

    - 已提供 probe：逐条探针定位——好条已处理、坏条精确隔离，位点推进；
    - 未提供 probe：整批隔离后提交位点；
    - DLQ 关闭（逃生门）：转 paused 自愈旧行为。
    """
    dlq = runtime.dlq
    if dlq is None:
        return False  # 逃生门 DlqOptions.enabled=false / 未接线：paused 旧行为
    logger.info(
        "handle_failure_classified",
        category=ErrorKind.POISON.value,
        error=str(poison_error),
    )
    if runtime.probe is None:
        return await _quarantine_whole_batch(runtime, batch, poison_error)
    logger.warning(
        "batch_bisect_triggered",
        batch_size=len(batch),
        error=str(poison_error),
    )
    try:
        outcome = await locate_and_quarantine(
            runtime.probe, dlq, batch, ErrorKind.POISON, str(poison_error)
        )
    except DlqSendError:
        # dlq_send_failed 已由 DlqProducer 记日志；批次不提交 offset、转 paused
        # 下轮整体重试（好条幂等重处理，坏条重复隔离仅 DLQ 多一条，可接受）
        return False
    if outcome is None:
        return False  # 对照探针失败 → 疑似出口组件故障 → paused（不隔离任何数据）
    return await _finalize_locate_outcome(runtime, batch, outcome)


async def _finalize_locate_outcome(
    runtime: ConsumeRuntime,
    batch: list[BufferedMessage],
    outcome: BisectOutcome,
) -> bool:
    """定位完成：好条已处理、坏条已隔离，按成功等价收尾。"""
    runtime.buffer.clear()
    runtime.backlog_oldest_ts = None
    runtime.last_handle_at = datetime.now(timezone.utc)
    runtime.quarantined_count += len(outcome.quarantined)
    # 好条计处理成功；被隔离条视为处理失败（不计延迟：未真正完成处理）
    runtime.metrics.handled.record(len(outcome.handled))
    runtime.metrics.handle_failed.record(len(outcome.quarantined))
    for q in outcome.quarantined:
        _log_quarantined(runtime, q.message, q.request)
    if outcome.handled:
        # 只刷新实际处理完成的好条；被隔离条不动占位（
        # 占位残留 + 后续 409 是正确终态，阻止坏数据静默循环重灌）
        await _notify_handled(runtime, outcome.handled)
    await _commit_quietly(runtime, len(batch))
    runtime.last_commit_at = datetime.now(timezone.utc)
    logger.info(
        "batch_handle_success_with_quarantine",
        batch_size=len(batch),
        handled=len(outcome.handled),
        quarantined=len(outcome.quarantined),
    )
    return True


def _flush_timeout_due(runtime: ConsumeRuntime) -> bool:
    """缓冲区非空且距上次处理超过 flush_timeout_seconds。"""
    if not (runtime.buffer and runtime.last_handle_at):
        return False
    elapsed = (
        datetime.now(timezone.utc) - runtime.last_handle_at
    ).total_seconds()
    return elapsed >= runtime.flush_timeout_seconds


async def _trigger_flush(runtime: ConsumeRuntime) -> bool:
    """触发一次批处理；失败转 paused（超时/条数触发路径共用；FATAL 已停机则不再标 paused）。"""
    success = await _try_handle_batch(runtime)
    if not success and runtime.running:
        runtime.paused = True
        logger.error("consumer_paused_due_to_handle_failures")
    return success


async def _paused_recovery_tick(runtime: ConsumeRuntime) -> None:
    """暂停状态下的恢复处理（指数退避，上限 reconnect_max）。"""
    tuning = runtime.tuning
    backoff = tuning.backoff_seconds(runtime.recover_attempt)
    logger.info(
        "consumer_paused_retry",
        attempt=runtime.recover_attempt + 1,
        backoff_seconds=backoff,
    )
    await asyncio.sleep(backoff)
    if await _try_handle_batch(runtime):
        runtime.paused = False
        runtime.recover_attempt = 0
        logger.info("consumer_resumed_after_recovery")
    else:
        runtime.recover_attempt += 1


async def _ensure_consumer_ready(runtime: ConsumeRuntime) -> bool:
    """consumer 未启动时重连（指数退避）。返回 False = 本轮跳过后续步骤。"""
    consumer = runtime.consumer
    if consumer is None:
        logger.error("consumer_not_initialized")
        await asyncio.sleep(1.0)
        return False
    if consumer.started:
        return True
    tuning = runtime.tuning
    try:
        await consumer.start()
        runtime.reconnect_attempt = 0
        return True
    except Exception as e:
        runtime.reconnect_attempt += 1
        backoff = tuning.backoff_seconds(runtime.reconnect_attempt - 1)
        logger.error(
            "kafka_reconnect_failed",
            error=str(e),
            attempt=runtime.reconnect_attempt,
            backoff_seconds=backoff,
        )
        await asyncio.sleep(backoff)
        return False


async def _poll_records(runtime: ConsumeRuntime) -> list[KafkaRecord] | None:
    """拉取一批消息；poll 异常时退避 1s（返回 None = 本轮跳过）。"""
    consumer = runtime.consumer
    assert consumer is not None  # 仅在 _ensure_consumer_ready 通过后调用
    try:
        return await consumer.poll()
    except Exception as e:
        logger.error("kafka_poll_error", error=str(e))
        await asyncio.sleep(1.0)
        return None


async def _quarantine_type_mismatch(
    runtime: ConsumeRuntime,
    record: KafkaRecord,
    envelope: Envelope,
) -> None:
    """未知 type 前向兼容——隔离至 DLQ 推进位点，不 crash 不循环。"""
    if runtime.dlq is not None:
        request = _build_type_mismatch_request(record, envelope)
        try:
            await runtime.dlq.quarantine(request)
            runtime.quarantined_count += 1
            return
        except DlqSendError:
            logger.error(
                "unknown_type_dlq_failed",
                partition=record.partition,
                offset=record.offset,
                message_type=envelope.type,
            )
            return
    logger.error(
        "unknown_type_skipped",
        partition=record.partition,
        offset=record.offset,
        message_type=envelope.type,
    )


async def _process_record(
    runtime: ConsumeRuntime,
    record: KafkaRecord,
    poison_offsets: dict[TopicPartition, OffsetAndMetadata],
) -> None:
    """单条消息处理：毒丸/未知 type → 推进位点；合法消息 → 入缓冲区。"""
    value = record.value
    envelope = runtime.codec.decode(value)
    if envelope is None or value is None:
        logger.error(
            "poison_message_skipped",
            partition=record.partition,
            offset=record.offset,
            raw_message=value[:500] if value else "",
        )
        tp = TopicPartition(record.topic, record.partition)
        poison_offsets[tp] = OffsetAndMetadata(record.offset + 1, "")
        return
    if (
        runtime.expected_type is not None
        and envelope.type is not None
        and envelope.type != runtime.expected_type
    ):
        await _quarantine_type_mismatch(runtime, record, envelope)
        tp = TopicPartition(record.topic, record.partition)
        poison_offsets[tp] = OffsetAndMetadata(record.offset + 1, "")
        return
    # 消息源信息随数据入缓冲区：DLQ 隔离需要 partition/offset/key 与
    # 原始消息全文（完整转发），data 本身不含这些（decode 只提取 data 字段）
    runtime.buffer.append(
        BufferedMessage(
            data=envelope.data,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            raw_value=value,
        )
    )
    runtime.metrics.consumed.record(1)  # 合法消息才计入消费速率
    _note_backlog_ts(runtime, record.timestamp)  # 追最老未处理消息


async def _consume_records(
    runtime: ConsumeRuntime,
    records: list[KafkaRecord],
) -> None:
    """整批消息处理 + 毒丸位点提交（推进消费位点，避免重启后重复处理）。"""
    poison_offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for record in records:
        await _process_record(runtime, record, poison_offsets)
    if poison_offsets:
        consumer = runtime.consumer
        assert consumer is not None
        try:
            await consumer.commit(poison_offsets)
            logger.info("poison_offsets_committed", count=len(poison_offsets))
        except CommitFailedError as e:
            logger.warning("poison_offset_commit_failed", error=str(e))


async def _periodic_backlog_check(runtime: ConsumeRuntime) -> None:
    """周期性积压/池指标检查（含 paused 状态，出口故障期最需要）+ lag 真实化。"""
    now_dt = datetime.now(timezone.utc)
    if (
        runtime.last_backlog_check_at is not None
        and (now_dt - runtime.last_backlog_check_at).total_seconds()
        < runtime.tuning.backlog_check_interval
    ):
        return
    runtime.last_backlog_check_at = now_dt
    _check_backlog_age(runtime)
    # lag 真实化（与积压检查同周期计算，健康接口只读缓存值）
    consumer = runtime.consumer
    if consumer is not None:
        try:
            await consumer.refresh_lag()
        except Exception:
            logger.debug("lag_refresh_skipped", exc_info=True)


async def consume_loop(runtime: ConsumeRuntime) -> None:
    """主消费循环。"""
    runtime.running = True
    # 初始化为当前时间，避免首批消息不足 batch_size 时超时检查因 last_handle_at=None 永不触发
    runtime.last_handle_at = datetime.now(timezone.utc)

    logger.info(
        "consumer_loop_started",
        group_id=runtime.group_id,
        batch_size=runtime.batch_size,
        flush_timeout=runtime.flush_timeout_seconds,
    )

    while runtime.running:
        await _periodic_backlog_check(runtime)

        if runtime.paused:
            # 暂停状态下尝试恢复处理（指数退避，上限 reconnect_max）
            await _paused_recovery_tick(runtime)
            continue

        # 若 consumer 未启动，尝试重连（指数退避，上限 reconnect_max）
        if not await _ensure_consumer_ready(runtime):
            continue

        # 检查是否该触发批处理（超时）
        if _flush_timeout_due(runtime) and not await _trigger_flush(runtime):
            continue

        # 拉取消息
        records = await _poll_records(runtime)
        if records is not None:
            await _consume_records(runtime, records)

        # 检查是否该触发批处理（条数）
        if len(runtime.buffer) >= runtime.batch_size:
            await _trigger_flush(runtime)

    # 优雅停机：处理完缓冲区剩余数据
    if runtime.buffer:
        logger.info("shutdown_flushing_buffer", pending=len(runtime.buffer))
        await _try_handle_batch(runtime)

    logger.info("consumer_loop_stopped")


def _build_type_mismatch_request(
    record: KafkaRecord, envelope: Envelope
) -> QuarantineRequest:
    return QuarantineRequest(
        partition=record.partition,
        offset=record.offset,
        key=record.key,
        original_message=_original_message(record.value),
        category="unknown_type",
        error=f"unexpected envelope type: {envelope.type}",
        stage="decode",
    )
