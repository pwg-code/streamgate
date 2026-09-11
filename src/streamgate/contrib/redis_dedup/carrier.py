"""RedisDedupCarrier：分布式判重载体（streamgate.contrib 正式功能，guarantee="distributed"）。

协议适配层：身份键原子占位（Lua）、idle-GC TTL、冷身份回源、并发闸门、
fail-closed；TTL/前缀/降级开关全参数化。

框架保证调用时序：admit → (Kafka 发送) → on_send_success（每次成功，通知型）/
on_send_failed（每次失败，默认保留占位 TTL 自愈）→ on_force_accepted（仅
force 路径）；消费侧 write 成功 → on_persisted（判重与消费侧的唯一耦合点）。
"""

import asyncio
from collections.abc import Callable
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel
from redis.exceptions import RedisError

from streamgate import logger
from streamgate.contrib.redis_dedup.cache import RedisDedupCache
from streamgate.contrib.redis_dedup.config import RedisConfig
from streamgate.protocols import (
    BackfillSource,
    Decision,
    DecisionKind,
    JsonObject,
    NoBackfill,
    RejectInfo,
)

_REDIS_ERRORS: tuple[type[Exception], ...] = (
    RedisError,
    asyncio.TimeoutError,
    OSError,
)

_UNDETERMINED_LOG_EVENT = "existence_undetermined"

RecordModelT = TypeVar("RecordModelT", bound=BaseModel)


class RejectReason(str, Enum):
    """存在性"无法判定"的拒绝原因（呈现层自行映射处置）。"""

    DEPENDENCY = "dependency"  # 依赖故障（Redis+DB 均不可用 / DB 回源失败）
    GATE_FULL = "gate_full"    # 冷身份回源并发闸门满
    REDIS_UNAVAILABLE = "redis_unavailable"  # Redis 故障 fail-closed（配置开启时）


class ColdPathGateFullError(Exception):
    """冷身份回源并发闸门满。存在性"无法判定"方向，非 DB 故障。"""


class RedisDedupCarrierConfig:
    """判重载体自身参数：fail-closed vs 降级是策略参数，不是框架级开关。"""

    def __init__(
        self,
        *,
        fail_closed_on_unavailable: bool = True,
        cold_path_max_concurrency: int = 5,
        gate_full_retry_after_seconds: int = 1,
        unavailable_retry_after_seconds: int = 5,
        log_context: Callable[[str], JsonObject] | None = None,
    ) -> None:
        self.fail_closed_on_unavailable = fail_closed_on_unavailable
        self.cold_path_max_concurrency = cold_path_max_concurrency
        self.gate_full_retry_after_seconds = gate_full_retry_after_seconds
        self.unavailable_retry_after_seconds = unavailable_retry_after_seconds
        self.log_context = log_context


class RedisDedupCarrier(Generic[RecordModelT]):
    """存在性判定 + 原子占位编排（推入路径）。

    作为 DedupCarrier 协议实例注入
    ``Producer(options=ProducerOptions(dedup=DedupOptions(key=..., carrier=...)))``。
    需要冷身份回源时传 backfill=SqlBackfill(...)（streamgate.contrib.sql_upsert）。
    """

    def __init__(
        self,
        cache: RedisDedupCache,
        key: Callable[[RecordModelT], str],
        summary: Callable[[RecordModelT], JsonObject] | None = None,
        backfill: BackfillSource | None = None,
        config: RedisDedupCarrierConfig | None = None,
        redis_config: RedisConfig | None = None,
    ) -> None:
        self._cache = cache
        self._backfill: BackfillSource = backfill or NoBackfill()
        self._key = key
        self._summary = summary
        self._config = config or RedisDedupCarrierConfig()
        self._redis_config = redis_config
        self._context = self._config.log_context or (
            lambda identity: {"identity": identity}
        )
        # 冷身份回源并发闸门（与读池宽度一致）。<=0 禁用（回滚手段）。
        self._cold_gate: asyncio.Semaphore | None = (
            asyncio.Semaphore(self._config.cold_path_max_concurrency)
            if self._config.cold_path_max_concurrency > 0
            else None
        )

    # ---- 生命周期 ----

    async def start(self) -> None:
        await self._cache.start()

    async def close(self) -> None:
        await self._cache.close()
        close = getattr(self._backfill, "close", None)
        if close is not None:
            await close()

    # ---- 健康探测 ----

    async def check_cache_health(self) -> bool:
        """Redis 缓存健康检查（/health 用；未启动时返回 False）。"""
        return await self._cache.check_health()

    async def check_backfill_health(self) -> bool:
        """回源库健康检查（/health 用；无回源时 False）。"""
        check = getattr(self._backfill, "check_health", None)
        if check is None:
            return False
        return bool(await check())

    async def check_backfill_health_detail(self) -> tuple[bool, str | None]:
        check = getattr(self._backfill, "check_health_detail", None)
        if check is None:
            return False, "no backfill source"
        return await check()

    async def check_cache_health_detail(self) -> tuple[bool, str | None]:
        """Redis 健康预检；返回 (是否可用, 错误信息)。"""
        return await self._cache.check_health_detail()

    # ---- DedupCarrier 协议 ----

    @property
    def guarantee(self) -> str:
        return "distributed"

    @property
    def existence_ttl_seconds(self) -> int | None:
        if self._redis_config is not None:
            return self._redis_config.identity_ttl_seconds
        return self._cache.identity_ttl_seconds

    @property
    def cache(self) -> RedisDedupCache:
        return self._cache

    def _summarize(self, record: RecordModelT) -> JsonObject:
        return self._summary(record) if self._summary is not None else {}

    async def admit(
        self, record: RecordModelT, *, force: bool = False
    ) -> Decision:
        identity = str(self._key(record))
        if force:
            # force 路径：写 Kafka 前 Redis 健康预检（fail-closed）。
            # 预检失败即拒绝：不写 Kafka、不写摘要，消灭半完成态
            if self._config.fail_closed_on_unavailable:
                redis_ok, redis_error = await self._cache.check_health_detail()
                if not redis_ok:
                    logger.warning(
                        "force_rejected_redis_unavailable",
                        **self._context(identity),
                        error=redis_error,
                    )
                    return self._reject_redis_unavailable(
                        event="force_rejected_redis_unavailable",
                        error=redis_error,
                    )
            return Decision.allow()

        result = await self._check_and_reserve(
            identity, self._summarize(record)
        )
        if result is None:
            # NOT_EXISTS：占位已生效（或降级无占位，仅配置关闭时），继续
            return Decision.allow()
        return result

    async def on_send_success(self, record: RecordModelT) -> None:
        """通知型钩子：占位已在 admit 原子写入，无需动作。"""
        return None

    async def on_send_failed(self, record: RecordModelT) -> None:
        """默认保留占位（TTL 过后冷路径回源自愈）；需"失败立即可重推"可在此
        删除 dedup key（自担模糊失败重复风险）。"""
        return None

    async def on_force_accepted(self, record: RecordModelT) -> bool:
        """force 摘要写：失败重试 1 次；仍失败 ERROR（供告警删 key）并返回 False。"""
        identity = str(self._key(record))
        meta = self._summarize(record)
        for attempt in (1, 2):
            try:
                await self._cache.write_summary(identity, meta)
                return True
            except _REDIS_ERRORS as e:
                if attempt == 2:
                    logger.error(
                        "force_summary_write_failed",
                        **self._context(identity),
                        error=str(e),
                    )
        return False

    async def on_persisted(self, record: RecordModelT) -> None:
        """落库成功 → 提交 offset 前的权威缓存刷新（失败 WARN 不影响提交）。"""
        identity = str(self._key(record))
        if not identity:
            return  # 毒行防御（写库侧会兜底报错）
        try:
            await self._cache.write_summary(identity, self._summarize(record))
        except Exception as e:
            logger.warning(
                "cache_refresh_failed",
                **self._context(identity),
                error=str(e),
            )

    # ---- 判定与占位 ----

    async def _check_and_reserve(
        self, identity: str, meta: JsonObject
    ) -> Decision | None:
        """返回 None = 放行（NOT_EXISTS）；否则为 DUPLICATE/REJECT 决策。"""
        # ---- 阶段 1：缓存判定 ----
        try:
            existing = await self._cache.get_summary(identity)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            return await self._direct_backfill_check(identity, e)
        if existing is not None:
            return Decision.duplicate(existing)

        # ---- 阶段 2：冷身份回源（并发闸门；无回源时直接占位）----
        try:
            backfill_map = await self._gated_load(identity)
        except ColdPathGateFullError:
            logger.warning("cold_path_gate_rejected", **self._context(identity))
            return self._reject_gate_full()
        except Exception as e:
            logger.error(
                "load_identity_failed", **self._context(identity), error=str(e)
            )
            return self._reject_dependency()

        if backfill_map is not None and identity in backfill_map:
            await self._backfill_best_effort(identity, backfill_map[identity])
            return Decision.duplicate(backfill_map[identity])

        # DB 确认空：推入路径直接原子占位
        return await self._reserve(identity, meta)

    async def _gated_load(self, scope: str) -> dict[str, JsonObject] | None:
        """闸门满时不触碰 DB，立即抛 ColdPathGateFullError。

        回源契约：scope = 身份键（单键载体），返回 {identity: summary} 单条目映射。
        单 event loop 内 locked() 检查与 acquire 之间无 await 点，无竞态窗口
        （asyncio.Semaphore 槽位空闲时 acquire 不挂起）。
        """
        if self._cold_gate is None:
            logger.info("cold_path_load", **self._context(scope))
            return await self._backfill.load(scope)
        if self._cold_gate.locked():
            raise ColdPathGateFullError(scope)
        async with self._cold_gate:
            logger.info("cold_path_load", **self._context(scope))
            return await self._backfill.load(scope)

    async def _reserve(
        self, identity: str, meta: JsonObject
    ) -> Decision | None:
        try:
            reserved, existing = await self._cache.reserve(identity, meta)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            # 占位失败 -> 无占位继续（ERROR 告警，删 key 修复）；仅配置关闭时可达
            logger.error(
                "reserve_failed_degraded",
                **self._context(identity),
                error=str(e),
            )
            return None
        if reserved:
            return None
        return Decision.duplicate(existing)  # 竞态输家

    async def _direct_backfill_check(
        self, identity: str, error: Exception
    ) -> Decision | None:
        """Redis 故障降级：直查回源库，不回填、不占位。"""
        logger.warning(
            "redis_degraded_direct_backfill",
            **self._context(identity),
            error=str(error),
        )
        try:
            backfill_map = await self._gated_load(identity)
        except ColdPathGateFullError:
            logger.warning("cold_path_gate_rejected", **self._context(identity))
            return self._reject_gate_full()
        except Exception as db_err:
            logger.error(
                "load_identity_failed_degraded",
                **self._context(identity),
                error=str(db_err),
            )
            return self._reject_dependency()
        if backfill_map is not None and identity in backfill_map:
            return Decision.duplicate(backfill_map[identity])
        return None

    async def _backfill_best_effort(
        self, identity: str, summary: JsonObject
    ) -> None:
        """回源命中后回填缓存；失败仅 WARN（判定结果已来自 DB，方向安全）。"""
        try:
            await self._cache.write_summary(identity, summary)
        except _REDIS_ERRORS as e:
            logger.warning(
                "cache_backfill_failed", **self._context(identity), error=str(e)
            )

    # ---- 拒绝决策构造 ----

    def _reject_redis_unavailable(
        self,
        *,
        event: str = "ingest_rejected_redis_unavailable",
        error: str | None = None,
    ) -> Decision:
        return Decision(
            DecisionKind.REJECT,
            reject=RejectInfo(
                reason=RejectReason.REDIS_UNAVAILABLE.value,
                retry_after=self._config.unavailable_retry_after_seconds,
                log_event=event,
                log_fields={"error": error} if error is not None else {},
            ),
        )

    def _reject_gate_full(self) -> Decision:
        return Decision(
            DecisionKind.REJECT,
            reject=RejectInfo(
                reason=RejectReason.GATE_FULL.value,
                retry_after=self._config.gate_full_retry_after_seconds,
                log_event=_UNDETERMINED_LOG_EVENT,
                log_fields={"reason": RejectReason.GATE_FULL.value},
            ),
        )

    def _reject_dependency(self) -> Decision:
        return Decision(
            DecisionKind.REJECT,
            reject=RejectInfo(
                reason=RejectReason.DEPENDENCY.value,
                retry_after=self._config.unavailable_retry_after_seconds,
                log_event=_UNDETERMINED_LOG_EVENT,
                log_fields={"reason": RejectReason.DEPENDENCY.value},
            ),
        )


__all__ = [
    "ColdPathGateFullError",
    "RedisDedupCarrier",
    "RedisDedupCarrierConfig",
    "RejectReason",
]
