"""RedisGroupDedupCarrier：分组分布式判重载体（guarantee="distributed"）。

与 RedisDedupCarrier 的差异：判重边界是"组"而非"身份"。
- 组键 = HASH（field = 组内身份），以组为单位缓存，整组一次冷回源预热
- admit 三阶段：组内缓存判定 → 组存在快路径（零 DB）→ 全新组冷回源
  （组级单飞 + 并发闸门）
- 组 key 生命周期不可变：仅"整组回填成功"、"空组确认后首身份占位"、
  "整组确认空标记（cache.confirm_empty）"三条路径创建 → 组 key 存在 ⇒
  组状态可信（可能为空），无需任何安全阀/开关
- load_group_refill(group) 公开"整组冷回源 + 回填建键"原语：复用组级单飞 +
  并发闸门 + 分片回填（与 admit 阶段3 同一机制），供枚举查询侧复用避免复制

框架保证调用时序：admit → (Kafka 发送) → on_send_success（每次成功，通知型）/
on_send_failed（每次失败，默认保留占位 TTL 自愈）→ on_force_accepted（仅
force 路径）；消费侧 write 成功 → on_persisted（判重与消费侧的唯一耦合点）。
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, TypeVar

from pydantic import BaseModel
from redis.exceptions import RedisError

from streamgate import logger
from streamgate.contrib.redis_dedup.carrier import RejectReason
from streamgate.contrib.redis_dedup.config import RedisConfig
from streamgate.contrib.redis_dedup.group_cache import RedisGroupDedupCache
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


class GroupLoadResult(str, Enum):
    """整组回源结果三态（等待方据此完成阶段3后续判定，无需重走阶段1/2）。"""

    FOUND = "found"       # 全组非空映射
    EMPTY = "empty"       # 确认组无记录
    FAILED = "failed"     # 回源失败（DB 故障 / 闸门满等，REJECT 方向）


@dataclass(frozen=True)
class GroupLoadOutcome:
    """组级单飞的回源结果载体。GATE_FULL 单独成态以区分拒绝原因。"""

    result: GroupLoadResult
    mapping: dict[str, JsonObject] = field(default_factory=dict)
    error: str | None = None
    gate_full: bool = False


class RedisGroupDedupCarrierConfig:
    """组载体自身参数：fail-closed vs 降级是策略参数，不是框架级开关。"""

    def __init__(
        self,
        *,
        fail_closed_on_unavailable: bool = True,
        group_cold_path_max_concurrency: int = 5,
        gate_full_retry_after_seconds: int = 1,
        unavailable_retry_after_seconds: int = 5,
        log_context: Callable[[str, str], JsonObject] | None = None,
    ) -> None:
        self.fail_closed_on_unavailable = fail_closed_on_unavailable
        self.group_cold_path_max_concurrency = group_cold_path_max_concurrency
        self.gate_full_retry_after_seconds = gate_full_retry_after_seconds
        self.unavailable_retry_after_seconds = unavailable_retry_after_seconds
        self.log_context = log_context


class RedisGroupDedupCarrier(Generic[RecordModelT]):
    """分组判重 + 整组原子占位编排（推入路径）。仅组内判重（组 = 判重完整边界）。

    作为 DedupCarrier 协议实例注入
    ``Producer(options=ProducerOptions(dedup=DedupOptions(key=..., carrier=...)))``。
    组号与组内身份分别从 payload 提取：``group_key(record) -> str``（组号）、
    ``key(record) -> str``（组内身份）。需要冷回源时传
    backfill=SqlBackfill(..., group_column=...)（streamgate.contrib.sql_upsert）。
    """

    def __init__(
        self,
        cache: RedisGroupDedupCache,
        group_key: Callable[[RecordModelT], str],
        key: Callable[[RecordModelT], str],
        summary: Callable[[RecordModelT], JsonObject] | None = None,
        backfill: BackfillSource | None = None,
        config: RedisGroupDedupCarrierConfig | None = None,
        redis_config: RedisConfig | None = None,
    ) -> None:
        self._cache = cache
        self._backfill: BackfillSource = backfill or NoBackfill()
        self._group_key = group_key
        self._key = key
        self._summary = summary
        self._config = config or RedisGroupDedupCarrierConfig()
        self._redis_config = redis_config
        self._context = self._config.log_context or (
            lambda group, identity: {"group": group, "identity": identity}
        )
        # 整组回源并发闸门。<=0 禁用（回滚手段）。
        self._cold_gate: asyncio.Semaphore | None = (
            asyncio.Semaphore(self._config.group_cold_path_max_concurrency)
            if self._config.group_cold_path_max_concurrency > 0
            else None
        )
        # 组级单飞：同组并发首触只回源一次，其余 await 同一 future
        self._in_flight: dict[str, asyncio.Future[GroupLoadOutcome]] = {}

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
            return self._redis_config.group_ttl_seconds
        return self._cache.group_ttl_seconds

    @property
    def cache(self) -> RedisGroupDedupCache:
        return self._cache

    def _summarize(self, record: RecordModelT) -> JsonObject:
        return self._summary(record) if self._summary is not None else {}

    async def admit(
        self, record: RecordModelT, *, force: bool = False
    ) -> Decision:
        group = str(self._group_key(record))
        identity = str(self._key(record))
        meta = self._summarize(record)
        if force:
            # force 路径：写 Kafka 前 Redis 健康预检（fail-closed）。
            # 预检失败即拒绝：不写 Kafka、不写摘要，消灭半完成态
            if self._config.fail_closed_on_unavailable:
                redis_ok, redis_error = await self._cache.check_health_detail()
                if not redis_ok:
                    logger.warning(
                        "force_rejected_redis_unavailable",
                        **self._context(group, identity),
                        error=redis_error,
                    )
                    return self._reject_redis_unavailable(
                        event="force_rejected_redis_unavailable",
                        error=redis_error,
                    )
            return Decision.allow()

        result = await self._check_and_reserve(group, identity, meta)
        if result is None:
            # NOT_EXISTS：占位已生效（或降级无占位，仅配置关闭时），继续
            return Decision.allow()
        return result

    async def on_send_success(self, record: RecordModelT) -> None:
        """通知型钩子：占位已在 admit 原子写入，无需动作。"""
        return None

    async def on_send_failed(self, record: RecordModelT) -> None:
        """默认保留占位（TTL 过后冷路径回源自愈）；需"失败立即可重推"可在此
        删除整组字段（自担模糊失败重复风险）。"""
        return None

    async def on_force_accepted(self, record: RecordModelT) -> bool:
        """force 摘要写：幂等 HSET + 续期；失败重试 1 次，仍失败 ERROR 并返回 False。"""
        group = str(self._group_key(record))
        identity = str(self._key(record))
        meta = self._summarize(record)
        for attempt in (1, 2):
            try:
                await self._cache.write_summary(group, identity, meta)
                return True
            except _REDIS_ERRORS as e:
                if attempt == 2:
                    logger.error(
                        "force_summary_write_failed",
                        **self._context(group, identity),
                        error=str(e),
                    )
        return False

    async def on_persisted(self, record: RecordModelT) -> None:
        """落库成功 → 提交 offset 前的权威缓存刷新（失败 WARN 不影响提交）。"""
        group = str(self._group_key(record))
        identity = str(self._key(record))
        if not group or not identity:
            return  # 毒行防御（写库侧会兜底报错）
        try:
            await self._cache.write_summary(group, identity, self._summarize(record))
        except Exception as e:
            logger.warning(
                "cache_refresh_failed",
                **self._context(group, identity),
                error=str(e),
            )

    # ---- 判定与占位 ----

    async def _check_and_reserve(
        self, group: str, identity: str, meta: JsonObject
    ) -> Decision | None:
        """返回 None = 放行（NOT_EXISTS）；否则为 DUPLICATE/REJECT 决策。"""
        # ---- 阶段 1：组内缓存判定 ----
        try:
            existing = await self._cache.get_identity_meta(group, identity)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            return await self._direct_backfill_check(group, identity, e)
        if existing is not None:
            return Decision.duplicate(existing)

        # ---- 阶段 2：组存在快路径（组已可靠完整，零 DB）----
        try:
            exists = await self._cache.group_exists(group)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            return await self._direct_backfill_check(group, identity, e)
        if exists:
            # field 缺失 = DB 内确认无此身份 → 原子占位
            logger.info("group_cache_shortcut", **self._context(group, identity))
            return await self._reserve(group, identity, meta)

        # ---- 阶段 3：全新组冷回源（组级单飞 + 并发闸门）----
        outcome = await self._load_group_single_flight(group)
        return await self._complete_stage3(group, identity, meta, outcome)

    async def _complete_stage3(
        self,
        group: str,
        identity: str,
        meta: JsonObject,
        outcome: GroupLoadOutcome,
    ) -> Decision | None:
        """阶段3终结：按单飞回源结果完成回填/占位/拒绝。"""
        if outcome.gate_full:
            logger.warning("cold_path_gate_rejected", **self._context(group, identity))
            return self._reject_gate_full()
        if outcome.result is GroupLoadResult.FAILED:
            logger.error(
                "load_group_failed",
                **self._context(group, identity),
                error=outcome.error,
            )
            return self._reject_dependency()

        if outcome.mapping:
            await self._backfill_best_effort(group, outcome.mapping)
            if identity in outcome.mapping:
                return Decision.duplicate(outcome.mapping[identity])
            return await self._reserve(group, identity, meta)

        # DB 确认组无记录：首身份占位建组（空组确认后才占位，安全）
        return await self._reserve(group, identity, meta)

    # ---- 组级单飞 + 整组回源 ----

    async def _load_group_single_flight(self, group: str) -> GroupLoadOutcome:
        """同组并发首触合并为一次回源；future 由后台任务解析，等待方共享。

        future 永远以 GroupLoadOutcome 解析（含 FAILED），等待方不悬挂；
        shield 隔离等待方取消，不打断已在飞的回源。
        """
        pending = self._in_flight.get(group)
        if pending is not None:
            logger.info("cold_group_load_hit", **self._context(group, ""))
            return await asyncio.shield(pending)

        loop = asyncio.get_running_loop()
        future: asyncio.Future[GroupLoadOutcome] = loop.create_future()
        self._in_flight[group] = future
        try:
            loop.create_task(self._run_group_load(group, future))
            return await asyncio.shield(future)
        finally:
            self._in_flight.pop(group, None)

    async def _run_group_load(
        self,
        group: str,
        future: asyncio.Future[GroupLoadOutcome],
    ) -> None:
        """回源任务：任何异常降级为 FAILED 结果，绝不悬挂等待方。"""
        try:
            outcome = await self._gated_load_group(group)
        except Exception as e:  # 兜底（正常路径不抛，只保 waiting 不悬挂）
            logger.error("load_group_failed", **self._context(group, ""), error=str(e))
            outcome = GroupLoadOutcome(GroupLoadResult.FAILED, error=str(e))
        if not future.done():
            future.set_result(outcome)

    async def _gated_load_group(self, group: str) -> GroupLoadOutcome:
        """整组回源：并发闸门。闸门满返回 GATE_FULL 态（REJECT 方向）。"""
        if self._cold_gate is None:
            logger.info("cold_group_load", **self._context(group, ""))
            return await self._load_group(group)
        if self._cold_gate.locked():
            return GroupLoadOutcome(GroupLoadResult.FAILED, gate_full=True)
        async with self._cold_gate:
            logger.info("cold_group_load", **self._context(group, ""))
            return await self._load_group(group)

    async def _load_group(self, group: str) -> GroupLoadOutcome:
        """执行统一回源（backfill.load(group)）；失败转为 FAILED 结果（在决策点统一告警）。
        """
        try:
            mapping = await self._backfill.load(group)
        except Exception as e:
            return GroupLoadOutcome(GroupLoadResult.FAILED, error=str(e))
        fields = mapping if mapping is not None else {}
        return GroupLoadOutcome(
            GroupLoadResult.FOUND if fields else GroupLoadResult.EMPTY,
            mapping=fields,
        )

    # ---- 公开原语：整组冷回源 + 回填（枚举查询侧复用，避免复制机制）----

    async def load_group_refill(self, group: str) -> GroupLoadOutcome:
        """整组冷回源 + 回填建键（公开原语，供枚举查询侧复用）。

        与 admit 阶段3 复用同一机制与不变式：
        - 组级单飞：同组并发首触只回源一次，Future 共享；等待方不悬挂；
        - 并发闸门：闸门满返回 GATE_FULL 态，不触碰 DB（REJECT 方向）；
        - 整组分片回填后统一续期；仅整组回填成功才建键（write_group_fields
          分片写出自动建键），半组/失败不成键、不对外判定。

        返回 GroupLoadOutcome（三态）：
        - FOUND：组内字段已回填，使用方 get_group_fields(group) 直接命中；
        - EMPTY：DB 确认组无记录，使用方可选 cache.confirm_empty(group)
          落"确认空"负缓存，消除后续冷回源；
        - FAILED / GATE_FULL：不建键，由使用方按策略处置。

        边界：本方法只暴露机制，不纳入枚举查询端点/响应体/HTTP 429-502/
        source 标签等策略内容（mechanism vs policy）。
        """
        outcome = await self._load_group_single_flight(group)
        if outcome.result is GroupLoadResult.FOUND:
            await self._backfill_best_effort(group, outcome.mapping)
        return outcome

    # ---- 占位与回填 ----

    async def _reserve(
        self, group: str, identity: str, meta: JsonObject
    ) -> Decision | None:
        try:
            reserved, existing = await self._cache.reserve(group, identity, meta)
        except _REDIS_ERRORS as e:
            if self._config.fail_closed_on_unavailable:
                return self._reject_redis_unavailable(error=str(e))
            # 占位失败 -> 无占位继续（ERROR 告警，删 group key 修复）；仅配置关闭时可达
            logger.error(
                "reserve_failed_degraded",
                **self._context(group, identity),
                error=str(e),
            )
            return None
        if reserved:
            return None
        return Decision.duplicate(existing)  # 竞态输家

    async def _backfill_best_effort(
        self, group: str, mapping: dict[str, JsonObject]
    ) -> None:
        """整组回填；失败仅 WARN（判定结果已来自确权后的 DB，方向安全）。
        不能在组键还不存在时回填后对外判定——本方法只在回源成功后调用。"""
        try:
            await self._cache.write_group_fields(group, mapping)
        except _REDIS_ERRORS as e:
            logger.warning(
                "group_backfill_fields_failed",
                **self._context(group, ""),
                error=str(e),
            )

    async def _direct_backfill_check(
        self, group: str, identity: str, error: Exception
    ) -> Decision | None:
        """Redis 故障降级：直查回源库，不回填、不占位。"""
        logger.warning(
            "redis_degraded_direct_backfill",
            **self._context(group, identity),
            error=str(error),
        )
        outcome = await self._gated_load_group(group)
        if outcome.gate_full:
            logger.warning("cold_path_gate_rejected", **self._context(group, identity))
            return self._reject_gate_full()
        if outcome.result is GroupLoadResult.FAILED:
            logger.error(
                "load_group_failed_degraded",
                **self._context(group, identity),
                error=outcome.error,
            )
            return self._reject_dependency()
        if identity in outcome.mapping:
            return Decision.duplicate(outcome.mapping[identity])
        return None

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
    "GroupLoadOutcome",
    "GroupLoadResult",
    "RedisGroupDedupCarrier",
    "RedisGroupDedupCarrierConfig",
]