"""RedisExistenceAdmission：唯一性准入的参考实现。

协议适配层：entity+slot 原子占位（Lua）、idle-GC TTL、
空实体哨兵、fail-closed、overwrite 预检；TTL/前缀/降级开关/错误码全参数化。

框架保证调用时序：admit → (Kafka 发送) → on_accepted；消费侧
write 成功 → on_persisted（准入与消费侧的唯一耦合点）。
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Generic

from streamgate.cache.existence import RedisExistenceCache
from streamgate.config import RedisConfig
from streamgate.obs.logging import logger
from streamgate.protocols import (
    BackfillSource,
    Decision,
    DecisionKind,
    JsonObject,
    NoBackfill,
    RecordT,
    RejectInfo,
)

try:
    # redis 是 extras 依赖（streamgate[redis]）：未安装时 RedisError 不可能被抛出，
    # 错误元组退化为其余两项即可（行为零变更）
    from redis.exceptions import RedisError

    _REDIS_ERRORS: tuple[type[Exception], ...] = (
        RedisError,
        asyncio.TimeoutError,
        OSError,
    )
except ImportError:  # pragma: no cover - 仅裸装（无 redis extras）时生效
    _REDIS_ERRORS = (asyncio.TimeoutError, OSError)

_UNDETERMINED_LOG_EVENT = "existence_undetermined"


class UndeterminedReason(str, Enum):
    """存在性"无法判定"的根因（供路由层区分 502 / 429）。"""

    DEPENDENCY = "dependency"  # 依赖故障（Redis+DB 均不可用 / DB 回源失败）
    GATE_FULL = "gate_full"    # 冷实体回源并发闸门满
    REDIS_UNAVAILABLE = "redis_unavailable"  # Redis 故障 fail-closed（配置开启时）


class ExistenceUnavailableError(Exception):
    """Redis 与 DB 均不可用（查询路径用）。

    reason 携带根因：依赖故障（dependency）或闸门满（gate_full），
    供路由层区分 502 / 429。
    """

    def __init__(self, message: str, *, reason: UndeterminedReason) -> None:
        super().__init__(message)
        self.reason = reason


class SlotSource(str, Enum):
    CACHE = "cache"
    DB = "db"
    DB_DEGRADED = "db-degraded"


class ColdPathGateFullError(Exception):
    """冷实体回源并发闸门满。存在性"无法判定"方向，非 DB 故障。"""


@dataclass
class EntitySlots:
    """整实体 slot 列表查询结果（查询端点用；空实体为空列表）。"""

    slots: list[str]
    source: SlotSource


@dataclass
class RedisExistenceAdmissionConfig:
    """准入策略自身参数：fail-closed vs 降级是策略参数，不是框架级开关。"""

    fail_closed_on_unavailable: bool = True
    cold_path_max_concurrency: int = 5   # 冷实体回源并发闸门（<=0 禁用）
    gate_full_retry_after_seconds: int = 1
    unavailable_retry_after_seconds: int = 5  # 依赖故障建议重试间隔（秒）
    # 错误码契约（使用方与调用方约定的稳定枚举）
    redis_unavailable_error_code: str = "REDIS_UNAVAILABLE"
    existence_unavailable_error_code: str = "EXISTENCE_UNAVAILABLE"
    gate_full_error_code: str = "COLD_PATH_GATE_FULL"
    reject_detail: str = "existence check unavailable, retry later"
    # 日志字段适配（默认 entity/slot；使用方可映射为调用方业务键名）
    log_context: Callable[[str, str], JsonObject] | None = None


class RedisExistenceAdmission(Generic[RecordT]):
    """存在性判定 + 原子占位编排（接收路径）与整实体 slot 查询。"""

    def __init__(
        self,
        cache: RedisExistenceCache,
        entity_key: Callable[[RecordT], str],
        slot_key: Callable[[RecordT], str],
        summary: Callable[[RecordT], JsonObject],
        backfill: BackfillSource | None = None,
        config: RedisExistenceAdmissionConfig | None = None,
        redis_config: RedisConfig | None = None,
    ) -> None:
        self._cache = cache
        self._backfill = backfill or NoBackfill()
        self._entity_key = entity_key
        self._slot_key = slot_key
        self._summary = summary
        self._config = config or RedisExistenceAdmissionConfig()
        self._redis_config = redis_config
        self._context = self._config.log_context or (
            lambda entity, slot: {"entity": entity, "slot": slot}
        )
        # 冷实体回源并发闸门（与读池宽度一致）。<=0 禁用（回滚手段）。
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

    @property
    def existence_ttl_seconds(self) -> int | None:
        if self._redis_config is not None:
            return self._redis_config.existence_ttl_seconds
        return None

    @property
    def cache(self) -> RedisExistenceCache:
        return self._cache

    # ---- AdmissionPolicy 协议 ----

    async def admit(self, record: RecordT, *, overwrite: bool = False) -> Decision:
        entity = str(self._entity_key(record))
        slot = str(self._slot_key(record))
        if overwrite:
            # overwrite 路径：写 Kafka 前 Redis 健康预检（fail-closed）。
            # 预检失败即拒绝：不写 Kafka、不写摘要，消灭 cache_updated=false 半完成态
            if self._config.fail_closed_on_unavailable:
                redis_ok, redis_error = await self._cache.check_health_detail()
                if not redis_ok:
                    logger.warning(
                        "overwrite_rejected_redis_unavailable",
                        **self._context(entity, slot),
                        error=redis_error,
                    )
                    return self._reject_redis_unavailable(
                        event="overwrite_rejected_redis_unavailable",
                        error=redis_error,
                    )
            return Decision.allow()

        result = await self._check_and_reserve(entity, slot, self._summary(record))
        if result is None:
            # NOT_EXISTS：占位已生效（或 E3 降级无占位，仅配置关闭时），继续
            return Decision.allow()
        return result

    async def on_accepted(self, record: RecordT) -> bool:
        """overwrite 摘要写：失败重试 1 次；仍失败 ERROR（供告警删 key）并返回 False。"""
        entity = str(self._entity_key(record))
        slot = str(self._slot_key(record))
        meta = self._summary(record)
        for attempt in (1, 2):
            try:
                await self._cache.write_summary(entity, slot, meta)
                return True
            except _REDIS_ERRORS as e:
                if attempt == 2:
                    logger.error(
                        "overwrite_summary_write_failed",
                        **self._context(entity, slot),
                        error=str(e),
                    )
        return False

    async def on_persisted(self, record: RecordT) -> None:
        """落库成功 → 提交 offset 前的权威缓存刷新（E5：失败 WARN 不影响提交）。"""
        entity = str(self._entity_key(record))
        slot = str(self._slot_key(record))
        if not entity or not slot:
            return  # 毒行防御（写库侧会兜底报错）
        try:
            await self._cache.write_summary(entity, slot, self._summary(record))
        except Exception as e:
            logger.warning(
                "cache_refresh_failed",
                **self._context(entity, slot),
                error=str(e),
            )

    # ---- 判定与占位 ----

    async def _check_and_reserve(
        self, entity: str, slot: str, meta: JsonObject
    ) -> Decision | None:
        """返回 None = 放行（NOT_EXISTS）；否则为 CONFLICT/REJECT 决策。"""
        # ---- 阶段 1：缓存判定 ----
        try:
            existing = await self._cache.get_field_meta(entity, slot)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            return await self._direct_db_check(entity, slot, e)
        if existing is not None:
            return Decision.conflict(existing)

        try:
            key_exists = await self._cache.key_exists(entity)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            return await self._direct_db_check(entity, slot, e)
        if key_exists:
            # key 存在 + field 缺失 => 不存在，零 DB 访问
            return await self._reserve(entity, slot, meta)

        # ---- 阶段 2：冷实体回源（并发闸门）----
        try:
            entity_fields = await self._gated_load(entity)
        except ColdPathGateFullError:
            logger.warning(
                "cold_path_gate_rejected", **self._context(entity, slot)
            )
            return self._reject_gate_full()
        except Exception as e:
            logger.error("load_entity_failed", **self._context(entity, slot), error=str(e))
            return self._reject_dependency()

        meta_existing = entity_fields.get(slot)
        if meta_existing is not None:
            await self._backfill_best_effort(entity, entity_fields)
            return Decision.conflict(meta_existing)

        # DB 确认空：接收路径不写哨兵，直接原子占位
        return await self._reserve(entity, slot, meta)

    async def _gated_load(self, entity: str) -> dict[str, JsonObject]:
        """闸门满时不触碰 DB，立即抛 ColdPathGateFullError。

        单 event loop 内 locked() 检查与 acquire 之间无 await 点，无竞态窗口
        （asyncio.Semaphore 槽位空闲时 acquire 不挂起）。
        """
        if self._cold_gate is None:
            logger.info("cold_path_db_load", **self._context(entity, ""))
            return await self._backfill.load(entity)
        if self._cold_gate.locked():
            raise ColdPathGateFullError(entity)
        async with self._cold_gate:
            logger.info("cold_path_db_load", **self._context(entity, ""))
            return await self._backfill.load(entity)

    async def _reserve(
        self, entity: str, slot: str, meta: JsonObject
    ) -> Decision | None:
        try:
            reserved, existing = await self._cache.reserve_field(entity, slot, meta)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            # E3：占位失败 -> 无占位继续（ERROR 告警，删 key 修复）；仅配置关闭时可达
            logger.error(
                "reserve_failed_degraded",
                **self._context(entity, slot),
                error=str(e),
            )
            return None
        if reserved:
            return None
        return Decision.conflict(existing)  # 竞态输家

    async def _direct_db_check(
        self, entity: str, slot: str, error: Exception
    ) -> Decision | None:
        """Redis 故障降级：直查 DB，不回填、不占位。"""
        logger.warning(
            "redis_degraded_direct_db",
            **self._context(entity, slot),
            error=str(error),
        )
        try:
            entity_fields = await self._gated_load(entity)
        except ColdPathGateFullError:
            logger.warning(
                "cold_path_gate_rejected", **self._context(entity, slot)
            )
            return self._reject_gate_full()
        except Exception as db_err:
            logger.error(
                "load_entity_failed_degraded",
                **self._context(entity, slot),
                error=str(db_err),
            )
            return self._reject_dependency()
        meta = entity_fields.get(slot)
        if meta is not None:
            return Decision.conflict(meta)
        return None

    async def _backfill_best_effort(
        self, entity: str, entity_fields: dict[str, JsonObject]
    ) -> None:
        """冷实体全量回填（entity 全部 slot）；失败仅 WARN（判定结果已来自 DB，方向安全）。"""
        try:
            await self._cache.write_entity_fields(entity, entity_fields)
        except _REDIS_ERRORS as e:
            logger.warning(
                "cache_backfill_failed", **self._context(entity, ""), error=str(e)
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
                status_code=502,
                error_code=self._config.redis_unavailable_error_code,
                detail=self._config.reject_detail,
                retry_after=self._config.unavailable_retry_after_seconds,
                log_event=event,
                log_fields={"error": error} if error is not None else {},
            ),
        )

    def _reject_gate_full(self) -> Decision:
        return Decision(
            DecisionKind.REJECT,
            reject=RejectInfo(
                status_code=429,
                error_code=self._config.gate_full_error_code,
                detail=self._config.reject_detail,
                retry_after=self._config.gate_full_retry_after_seconds,
                log_event=_UNDETERMINED_LOG_EVENT,
                log_fields={
                    "error_code": self._config.gate_full_error_code,
                    "reason": UndeterminedReason.GATE_FULL.value,
                },
            ),
        )

    def _reject_dependency(self) -> Decision:
        return Decision(
            DecisionKind.REJECT,
            reject=RejectInfo(
                status_code=502,
                error_code=self._config.existence_unavailable_error_code,
                detail=self._config.reject_detail,
                retry_after=self._config.unavailable_retry_after_seconds,
                log_event=_UNDETERMINED_LOG_EVENT,
                log_fields={
                    "error_code": self._config.existence_unavailable_error_code,
                    "reason": UndeterminedReason.DEPENDENCY.value,
                },
            ),
        )

    # ---- 整实体 slot 查询（查询端点用）----

    async def entity_slots(self, entity: str) -> EntitySlots:
        """整实体 slot 列表：缓存优先，冷实体回源全量回填；空实体写哨兵预热。

        Redis+DB 均不可用时抛 ExistenceUnavailableError，
        路由按 reason 转 502 / 429。
        """
        try:
            fields = await self._cache.get_entity_fields(entity)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                raise ExistenceUnavailableError(
                    str(e), reason=UndeterminedReason.REDIS_UNAVAILABLE
                ) from e
            return await self._degraded_entity_slots(entity, e)
        if fields is not None:
            # key 存在：真实数据或空实体哨兵，均信任，零 DB
            return EntitySlots(sorted(fields), SlotSource.CACHE)

        # 冷实体：回源（并发闸门）
        try:
            entity_fields = await self._gated_load(entity)
        except ColdPathGateFullError as e:
            logger.warning("cold_path_gate_rejected", **self._context(entity, ""))
            raise ExistenceUnavailableError(
                "cold path gate full", reason=UndeterminedReason.GATE_FULL
            ) from e
        except Exception as e:
            logger.error(
                "load_entity_failed_query", **self._context(entity, ""), error=str(e)
            )
            raise ExistenceUnavailableError(
                str(e), reason=UndeterminedReason.DEPENDENCY
            ) from e

        if entity_fields:
            await self._backfill_best_effort(entity, entity_fields)
            return EntitySlots(sorted(entity_fields), SlotSource.DB)

        # 空实体：哨兵预热（仅此路径、确认空后才写；best-effort）
        try:
            await self._cache.write_empty_marker(entity)
        except _REDIS_ERRORS as e:
            logger.info(
                "empty_marker_write_skipped", **self._context(entity, ""), error=str(e)
            )
        return EntitySlots([], SlotSource.DB)

    async def _degraded_entity_slots(
        self, entity: str, error: Exception
    ) -> EntitySlots:
        """Redis 故障：直查 DB，不回填、不哨兵。DB 也挂 -> ExistenceUnavailableError。"""
        logger.warning(
            "redis_degraded_query", **self._context(entity, ""), error=str(error)
        )
        try:
            entity_fields = await self._gated_load(entity)
        except ColdPathGateFullError as e:
            logger.warning("cold_path_gate_rejected", **self._context(entity, ""))
            raise ExistenceUnavailableError(
                "cold path gate full", reason=UndeterminedReason.GATE_FULL
            ) from e
        except Exception as db_err:
            logger.error(
                "load_entity_failed_degraded_query",
                **self._context(entity, ""),
                error=str(db_err),
            )
            raise ExistenceUnavailableError(
                str(db_err), reason=UndeterminedReason.DEPENDENCY
            ) from db_err
        return EntitySlots(sorted(entity_fields), SlotSource.DB_DEGRADED)


__all__ = [
    "EntitySlots",
    "SlotSource",
    "ColdPathGateFullError",
    "ExistenceUnavailableError",
    "RedisExistenceAdmission",
    "RedisExistenceAdmissionConfig",
    "UndeterminedReason",
]
