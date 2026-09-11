"""Redis 分布式判重载体（extras：streamgate[redis]）。

RedisDedupCarrier（DedupCarrier 协议实现，guarantee="distributed"）+
RedisDedupCache（身份键 → summary 缓存）+ RedisGroupDedupCarrier/
RedisGroupDedupCache（组 → 组内身份 HASH 缓存）+ RedisConfig。
配合 streamgate.contrib.sql_upsert.SqlBackfill（可选 group_column 整组回源）
可实现冷身份/冷整组 DB 回源。
"""

from streamgate.contrib._deps import require_extra_import

try:
    from streamgate.contrib.redis_dedup.cache import (
        RedisDedupCache,
        dumps_summary,
        parse_summary,
    )
    from streamgate.contrib.redis_dedup.carrier import (
        ColdPathGateFullError,
        RedisDedupCarrier,
        RedisDedupCarrierConfig,
        RejectReason,
    )
    from streamgate.contrib.redis_dedup.config import RedisConfig
    from streamgate.contrib.redis_dedup.group_cache import RedisGroupDedupCache
    from streamgate.contrib.redis_dedup.group_carrier import (
        GroupLoadOutcome,
        GroupLoadResult,
        RedisGroupDedupCarrier,
        RedisGroupDedupCarrierConfig,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)

__all__ = [
    "ColdPathGateFullError",
    "GroupLoadOutcome",
    "GroupLoadResult",
    "RedisConfig",
    "RedisDedupCache",
    "RedisDedupCarrier",
    "RedisDedupCarrierConfig",
    "RedisGroupDedupCache",
    "RedisGroupDedupCarrier",
    "RedisGroupDedupCarrierConfig",
    "RejectReason",
    "dumps_summary",
    "parse_summary",
]
