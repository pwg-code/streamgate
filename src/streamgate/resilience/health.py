"""健康快照契约：数据归框架（collect_* 纯函数），暴露端点归使用方。

/health 的响应结构为公共契约（kafka/db/redis/quarantined_count/backlog）；
本模块不构建任何 HTTP 工件，使用方自行选择暴露方式（路由/文件/TCP）。
"""

import asyncio
import time
from collections.abc import Awaitable
from datetime import datetime, timezone
from typing import Protocol

from pydantic import BaseModel, Field

from streamgate.obs.logging import logger


class _ProducerHealthLike(Protocol):
    """ingest Kafka producer 的健康观测面（结构化匹配 KafkaProducerService）。

    以局部协议声明而非 import streamgate.ingest.producer：
    resilience 属基座层，不得依赖进程区（import-linter 门禁）。
    """

    async def check_health(self) -> bool: ...

    @property
    def last_failure_at(self) -> datetime | None: ...

    @property
    def reconnect_count(self) -> int: ...

    @property
    def down_duration_seconds(self) -> float | None: ...


class _ConsumerLike(Protocol):
    """Kafka consumer 的健康观测面（结构化匹配 KafkaConsumerService）。"""

    async def check_health(self) -> bool: ...

    @property
    def lag(self) -> int: ...


class _ConsumerRuntimeLike(Protocol):
    """消费运行时的健康观测面（结构化匹配 ConsumeRuntime）。

    成员声明为只读 property：协议可变属性按不变型匹配，property 按协变
    匹配，普通实例属性可满足只读 property 成员。
    """

    @property
    def consumer(self) -> _ConsumerLike | None: ...

    @property
    def writer(self) -> object | None: ...

    @property
    def cache(self) -> object | None: ...

    @property
    def paused(self) -> bool: ...

    @property
    def backlog_oldest_ts(self) -> float | None: ...

    @property
    def last_commit_at(self) -> datetime | None: ...

    @property
    def pending_count(self) -> int: ...

    @property
    def quarantined_count(self) -> int: ...


class _AdmissionHealthLike(Protocol):
    """准入策略的健康观测面（非泛型结构协议：任意 AdmissionPolicy[R] 均满足，
    规避泛型协议逆变在具体记录型实参下的不可赋值问题）。"""

    async def check_cache_health(self) -> bool: ...

    async def check_backfill_health(self) -> bool: ...


class IngestHealthResponse(BaseModel):
    status: str = Field(description="健康状态：healthy / degraded")
    kafka: str = Field(description="Kafka 连接状态：connected / disconnected")
    redis: str = Field(description="Redis 连接状态：connected / disconnected")
    database: str = Field(description="读取库连接状态：connected / disconnected")
    timestamp: datetime = Field(description="检查时间（UTC）")
    kafka_last_failure_at: datetime | None = Field(
        default=None,
        description="最近一次 Kafka send 失败时间（UTC）；无失败时 null",
    )
    kafka_reconnect_count: int = Field(
        default=0,
        description="当前掉线周期内重建次数；健康态为 0",
    )
    kafka_down_duration_seconds: float | None = Field(
        default=None,
        description="当前掉线持续时长（秒）；未掉线时 null",
    )


class ConsumerHealthResponse(BaseModel):
    status: str = Field(description="healthy / degraded / stopped")
    kafka: str = Field(description="connected / disconnected")
    database: str | None = Field(
        default=None,
        description="DB sink 连接状态；未接线 sink（on_record 模式）时为 null",
    )
    redis: str | None = Field(
        default=None,
        description="Redis 连接状态；未接线缓存时为 null",
    )
    last_commit_at: datetime | None = Field(default=None, description="上次提交时间")
    pending_count: int = Field(description="缓冲区待处理条数")
    lag: int = Field(description="Kafka 消费延迟")
    backlog_age_seconds: float | None = Field(
        default=None,
        description="最老未落地消息积压时长（秒）；无积压/未知时为 None",
    )
    quarantined_count: int = Field(
        description="进程生命周期内累计隔离至 DLQ 的坏数据条数",
    )


async def _probe_producer(
    producer: _ProducerHealthLike | None,
) -> tuple[bool, datetime | None, int, float | None]:
    """producer 健康探测：返回 (是否健康, 最近失败时间, 重连次数, 掉线时长)。"""
    if producer is None:
        return False, None, 0, None
    kafka_healthy = await producer.check_health()
    return (
        kafka_healthy,
        producer.last_failure_at,
        producer.reconnect_count,
        producer.down_duration_seconds,
    )


async def _probe_component(component: object | None) -> bool | None:
    """组件健康探测：None=组件未接线；未实现 check_health 的 sink 视为健康。"""
    if component is None:
        return None
    check = getattr(component, "check_health", None)
    if check is None:
        return True
    result: Awaitable[bool] = check()
    return bool(await result)


def _ingest_health_status(
    kafka_healthy: bool, redis_healthy: bool, db_healthy: bool
) -> str:
    return "healthy" if (kafka_healthy and redis_healthy and db_healthy) else "degraded"


def _ingest_health_response(
    kafka: tuple[bool, datetime | None, int, float | None],
    redis_healthy: bool,
    db_healthy: bool,
    timestamp: datetime,
) -> IngestHealthResponse:
    """按探测结果构造快照（字段顺序为公共契约）。"""
    kafka_healthy, kafka_last_failure_at, kafka_reconnect_count, kafka_down_duration_seconds = kafka
    status = _ingest_health_status(kafka_healthy, redis_healthy, db_healthy)
    logger.debug(
        "health_check",
        status=status,
        kafka=kafka_healthy,
        redis=redis_healthy,
        database=db_healthy,
    )
    return IngestHealthResponse(
        status=status,
        kafka="connected" if kafka_healthy else "disconnected",
        redis="connected" if redis_healthy else "disconnected",
        database="connected" if db_healthy else "disconnected",
        timestamp=timestamp,
        kafka_last_failure_at=kafka_last_failure_at,
        kafka_reconnect_count=kafka_reconnect_count,
        kafka_down_duration_seconds=kafka_down_duration_seconds,
    )


async def collect_ingest_health(
    producer: _ProducerHealthLike | None,
    admission: _AdmissionHealthLike | None,
) -> IngestHealthResponse:
    """ingest 侧健康快照：并发探测 Kafka / Redis / 回源库。"""
    kafka = await _probe_producer(producer)
    redis_task = (
        admission.check_cache_health() if admission is not None else _absent()
    )
    db_task = (
        admission.check_backfill_health() if admission is not None else _absent()
    )
    redis_healthy, db_healthy = await asyncio.gather(redis_task, db_task)
    return _ingest_health_response(
        kafka, bool(redis_healthy), bool(db_healthy), datetime.now(timezone.utc)
    )


async def _absent() -> bool:
    """未接线组件的哑探测（gather 需要同形协程）。"""
    return False


def _consumer_status(
    paused: bool, kafka_ok: bool, db_ok: bool | None, redis_ok: bool | None
) -> str:
    """状态判定：未接线组件（None）不参与判定；paused 恒 degraded。"""
    if paused:
        return "degraded"
    if not kafka_ok or db_ok is False or redis_ok is False:
        return "degraded"
    return "healthy"


def _state_str(ok: bool | None) -> str | None:
    if ok is None:
        return None
    return "connected" if ok else "disconnected"


async def collect_consumer_health(
    runtime: _ConsumerRuntimeLike,
) -> ConsumerHealthResponse:
    """consumer 侧健康快照：三子检查并发（DB 黑洞时快照不能挂死）。"""
    kafka_ok, db_ok, redis_ok = await asyncio.gather(
        _probe_kafka(runtime), _probe_component(runtime.writer),
        _probe_component(runtime.cache),
    )
    backlog_age_seconds = (
        time.time() - runtime.backlog_oldest_ts
        if runtime.backlog_oldest_ts is not None
        else None
    )
    return ConsumerHealthResponse(
        status=_consumer_status(runtime.paused, kafka_ok, db_ok, redis_ok),
        kafka="connected" if kafka_ok else "disconnected",
        database=_state_str(db_ok),
        redis=_state_str(redis_ok),
        last_commit_at=runtime.last_commit_at,
        pending_count=runtime.pending_count,
        lag=runtime.consumer.lag if runtime.consumer else 0,
        backlog_age_seconds=backlog_age_seconds,
        quarantined_count=runtime.quarantined_count,
    )


async def _probe_kafka(runtime: _ConsumerRuntimeLike) -> bool:
    consumer = runtime.consumer
    return consumer is not None and await consumer.check_health()


__all__ = [
    "ConsumerHealthResponse",
    "IngestHealthResponse",
    "collect_consumer_health",
    "collect_ingest_health",
]
