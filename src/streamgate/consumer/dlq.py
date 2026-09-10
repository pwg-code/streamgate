"""Kafka DLQ（死信队列）producer + 单条探针定位。

消费端隔离单条坏数据的出口：把探针定位判定的坏消息连同失败原因
完整转发到独立死信 topic，供人工留档/处置（不建自动消费/重放）。

与 ingest 侧 KafkaProducerService 的差异（刻意从简，勿"补齐"）：
- 无自愈监控任务：发送重试耗尽即抛 DlqSendError，由消费循环既有 paused
  机制兜底（隔离动作必须成功才允许推进 offset，杜绝静默丢失）；
- 单飞行假设：仅被消费循环（单 event loop 顺序调用）使用，无并发锁；
- 失败即销毁实例重建：aiokafka 内部 fatal 状态无法自愈（对齐 ingest producer 经验）。

注意：禁止 import streamgate.ingest.*（进程隔离契约，lint-imports 门禁）。
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from streamgate.obs.logging import logger
from streamgate.protocols import ErrorKind, JsonObject, Probe

DLQ_SEND_RETRY_INTERVAL_SECONDS = 1.0  # 发送尝试间隔（隔离路径不在吞吐热区，固定值即可）


@dataclass
class QuarantineRequest:
    """单条隔离请求：来源消息定位 + 原始消息 + 失败原因。"""

    partition: int
    offset: int
    key: str | None
    original_message: JsonObject
    category: str
    error: str
    stage: str = "bisect"


class DlqSendError(RuntimeError):
    """DLQ 发送重试耗尽。调用方必须：不提交 offset、整批转 paused。"""


@dataclass
class BufferedMessage:
    """缓冲区条目：data 为解析后的载荷 dict（handler/probe 输入），其余字段保留
    Kafka 消息源信息（DLQ 隔离时定位原消息、完整转发用）。"""

    data: JsonObject
    partition: int
    offset: int
    key: str | None
    raw_value: str


def build_dlq_payload(
    request: QuarantineRequest, message_type: str = "streamgate_dlq"
) -> JsonObject:
    """构造 DLQ 消息体（纯函数，便于离线校验结构；键名是 DLQ 消费方契约，勿改）。"""
    return {
        "type": message_type,
        "quarantined_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "reason": {
            "category": request.category,
            "error": request.error,
            "stage": request.stage,
        },
        "source": {
            "partition": request.partition,
            "offset": request.offset,
            "key": request.key,
        },
        "original_message": request.original_message,
    }


class DlqProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        *,
        topic: str | None = None,
        send_retries: int = 3,
        message_type: str = "streamgate_dlq",
        request_timeout_ms: int = 10000,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        if not self._topic:
            raise ValueError(
                "dlq topic is required: set DlqOptions.topic "
                "or env KAFKA__DLQ_TOPIC"
            )
        self._retries = send_retries
        self._message_type = message_type
        self._request_timeout_ms = request_timeout_ms
        self._producer: AIOKafkaProducer | None = None
        self._started: bool = False
        self._closed: bool = False

    async def start(self) -> None:
        """启动（幂等）。失败仅记 ERROR 日志不抛：quarantine 内懒启动兜底，
        消费循环照常起跑（对齐 consumer 启动期 Kafka 失败降级先例）。"""
        if self._closed or self._started:
            return
        try:
            await self._connect()
        except Exception as e:
            logger.error("dlq_producer_startup_failed", error=str(e), topic=self._topic)

    async def _connect(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            acks="all",
            request_timeout_ms=self._request_timeout_ms,
            enable_idempotence=True,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )
        try:
            await producer.start()
        except Exception:
            try:
                await producer.stop()
            except Exception:
                pass  # 清理失败无碍：实例已弃用，重建时换新对象
            raise
        self._producer = producer
        self._started = True
        logger.info("dlq_producer_connected", topic=self._topic)

    async def _close_safely(self) -> None:
        if self._producer is None:
            self._started = False
            return
        try:
            await self._producer.stop()
        except Exception as e:
            logger.debug("dlq_producer_cleanup_error", error=str(e))
        finally:
            self._producer = None
            self._started = False

    async def stop(self) -> None:
        self._closed = True
        await self._close_safely()
        logger.info("dlq_producer_disconnected")

    async def quarantine(self, request: QuarantineRequest) -> None:
        """隔离单条消息至 DLQ。成功静默返回；重试耗尽抛 DlqSendError。

        不变量：本方法返回 ⟺ 消息已确认写入 DLQ（acks=all）——
        调用方（消费循环）以此决定是否推进 offset。
        """
        value = json.dumps(
            build_dlq_payload(request, self._message_type),
            ensure_ascii=False,
            default=str,
        )
        last_error: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                await self._send_once(request, value)
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    "dlq_send_attempt_failed",
                    attempt=attempt,
                    retries=self._retries,
                    partition=request.partition,
                    offset=request.offset,
                    error=str(e),
                )
                # 失败即销毁实例：aiokafka producer 进入 fatal 状态后无法自愈
                await self._close_safely()
                if attempt < self._retries:
                    await asyncio.sleep(DLQ_SEND_RETRY_INTERVAL_SECONDS)
        logger.error(
            "dlq_send_failed",
            retries=self._retries,
            topic=self._topic,
            partition=request.partition,
            offset=request.offset,
            error=str(last_error),
        )
        raise DlqSendError(
            f"DLQ send failed after {self._retries} attempts "
            f"(partition={request.partition}, offset={request.offset}): {last_error}"
        )

    async def _send_once(self, request: QuarantineRequest, value: str) -> None:
        """单次发送（懒启动：覆盖启动期 broker 不可用）。"""
        if not self._started:
            await self._connect()
        producer = self._producer
        if producer is None:
            raise RuntimeError("DLQ producer not connected")
        await producer.send_and_wait(
            topic=self._topic,
            key=request.key,
            value=value,
        )


# ---- 单条探针定位与对照验证——防误隔离的核心 ----


@dataclass
class QuarantinedRecord:
    """被隔离的消息与其隔离请求（供调用方记日志/计数）。"""

    message: BufferedMessage
    request: QuarantineRequest


@dataclass
class BisectOutcome:
    """定位结果。不变式：完成时 handled + quarantined 恰好覆盖传入 batch 的全部条目。"""

    handled: list[BufferedMessage] = field(default_factory=list)
    quarantined: list[QuarantinedRecord] = field(default_factory=list)


class _ProbeAborted(Exception):
    """对照探针失败：疑似出口组件故障，中止整个定位过程。"""


def _original_message(raw_value: str | None) -> JsonObject:
    """还原原始 Kafka 消息（完整 envelope：type/received_at/source/data）。

    进缓冲区的消息必然通过过 decode（JSON 合法），此处防御性兜底：
    万一解析失败，原样内嵌，DLQ 留档不丢内容。
    """
    try:
        if raw_value is None:
            raise TypeError  # 与历史行为一致：None 非法 JSON，原样内嵌
        parsed = json.loads(raw_value)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    return {"raw": raw_value}


def _build_quarantine_request(
    message: BufferedMessage,
    category: ErrorKind,
    error: str,
) -> QuarantineRequest:
    return QuarantineRequest(
        partition=message.partition,
        offset=message.offset,
        key=message.key,
        original_message=_original_message(message.raw_value),
        category=category.value,
        error=error,
    )


async def _quarantine_one(
    dlq: DlqProducer,
    bad: BufferedMessage,
    category: ErrorKind,
    outcome: BisectOutcome,
    error: str,
) -> None:
    """隔离单条（隔离失败抛 DlqSendError，向上穿透中止本轮，绝不跳过）。"""
    request = _build_quarantine_request(bad, category, error)
    await dlq.quarantine(request)
    outcome.quarantined.append(QuarantinedRecord(message=bad, request=request))


async def _probe_reference(
    probe: Probe,
    reference: BufferedMessage,
    bad: BufferedMessage,
) -> None:
    """对照验证：对一条已成功处理的记录复跑 probe。

    对照成功 ⟹ 出口此刻可用 ⟹ 失败原因是该条数据自身；
    对照失败 ⟹ 疑似出口组件故障（误隔离风险）⟹ 中止全局转 paused。
    """
    try:
        await probe(reference.data)
    except Exception as e:
        raise _ProbeAborted(
            f"reference probe failed for offset={reference.offset} "
            f"(locating offset={bad.offset}), suspected outlet failure: {e}"
        ) from e


async def _locate(
    probe: Probe,
    dlq: DlqProducer,
    records: list[BufferedMessage],
    category: ErrorKind,
    outcome: BisectOutcome,
) -> None:
    """逐条定位一段记录。

    每条调用 probe：成功 = 已处理（记入 outcome.handled）；
    失败 = 该条无法处理，先对照验证（复跑最近一条成功记录；批内尚无
    成功记录时取下一条作对照，其成功结果同样计入 handled），再精确隔离。
    批内仅剩 1 条且无对照时：分类器已明确判定 POISON（排除了瞬态类），
    直接隔离——否则单条坏数据将陷入 paused→重试→paused 死循环。
    抛 _ProbeAborted（疑似出口故障）或 DlqSendError（DLQ 不可用）。

    探针调用刻意"1 次尝试不重试"：瞬时抖动由对照验证兜底区分，
    不做退避重试。
    """
    pending = list(records)
    reference: BufferedMessage | None = None
    while pending:
        record = pending.pop(0)
        try:
            await probe(record.data)
        except Exception as e:
            bad_error = str(e)
            if reference is not None:
                await _probe_reference(probe, reference, record)
            elif pending:
                control = pending.pop(0)
                try:
                    await probe(control.data)
                except Exception as ce:
                    raise _ProbeAborted(
                        f"reference probe failed for offset={control.offset} "
                        f"(locating offset={record.offset}), "
                        f"suspected outlet failure: {ce}"
                    ) from ce
                outcome.handled.append(control)
                reference = control
            elif category is ErrorKind.POISON:
                await _quarantine_one(dlq, record, category, outcome, bad_error)
                continue
            else:
                raise _ProbeAborted(
                    f"no probe reference available (batch size 1) "
                    f"for offset={record.offset}"
                )
            await _quarantine_one(dlq, record, category, outcome, bad_error)
            continue
        outcome.handled.append(record)
        reference = record


async def locate_and_quarantine(
    probe: Probe,
    dlq: DlqProducer,
    batch: list[BufferedMessage],
    category: ErrorKind,
    error: str,
) -> BisectOutcome | None:
    """单条探针定位入口。error 为触发定位的原始批级错误，用于运维上下文
    （真正写进 DLQ 的是各单条自身的新失败原因）。"""
    outcome = BisectOutcome()
    try:
        await _locate(probe, dlq, batch, category, outcome)
    except _ProbeAborted as e:
        # 对照失败：可能已有部分条目处理完成（幂等，paused 重试时无害重跑），
        # 但本轮绝不隔离任何数据、不推进位点
        logger.error(
            "bisect_aborted_probe_failed",
            batch_size=len(batch),
            handled=len(outcome.handled),
            error=str(e),
        )
        return None
    return outcome
