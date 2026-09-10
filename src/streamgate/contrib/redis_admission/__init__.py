"""Redis 唯一性准入（extras：streamgate[redis]）。

RedisExistenceAdmission（AdmissionPolicy 协议实现）+ RedisExistenceCache
（entity→slot→summary 缓存）+ RedisConfig。配合
streamgate.contrib.sql_upsert.SqlBackfill 可实现冷实体 DB 回源。
"""

from streamgate.contrib._deps import require_extra_import

try:
    from streamgate.contrib.redis_admission.admission import (
        ColdPathGateFullError,
        EntitySlots,
        ExistenceUnavailableError,
        RedisExistenceAdmission,
        RedisExistenceAdmissionConfig,
        SlotSource,
        UndeterminedReason,
    )
    from streamgate.contrib.redis_admission.config import RedisConfig
    from streamgate.contrib.redis_admission.existence import (
        EMPTY_FIELD,
        RedisExistenceCache,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)

__all__ = [
    "EMPTY_FIELD",
    "ColdPathGateFullError",
    "EntitySlots",
    "ExistenceUnavailableError",
    "RedisConfig",
    "RedisExistenceAdmission",
    "RedisExistenceAdmissionConfig",
    "RedisExistenceCache",
    "SlotSource",
    "UndeterminedReason",
]
