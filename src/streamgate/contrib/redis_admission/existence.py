"""RedisExistenceCache：entity→slot→summary 的 Redis HASH 缓存（可插拔准入的参考存储）。

机制（摘要结构为 JSON dict）：
- 原子占位（单 key Lua，Cluster 兼容）：写入即无条件续期回满额（idle GC）
- 幂等摘要写：无条件 HSET + 续期（TTL 心跳）
- 空实体哨兵：仅 key 不存在时写入（防与占位竞态）
- fail-closed 与否是准入策略的参数，缓存层只如实报告错误
"""
import asyncio
import json
from collections.abc import Awaitable
from typing import TypeVar

import redis.asyncio as aioredis

from streamgate import logger
from streamgate.contrib.redis_admission.config import RedisConfig

_T = TypeVar("_T")

_HEALTH_TIMEOUT_S = 2.0

# 哨兵 field 保留字：空实体标记。所有 HGETALL/HKEYS 消费者必须过滤它。
EMPTY_FIELD: str = "__empty__"

# 原子占位（单 key，Cluster 兼容）：写入即无条件续期回满额（idle GC）
# KEYS[1]=existence key, ARGV=[slot, summary_json, ttl_seconds, empty_field]
# 返回 [1, ""] = 占位成功；[0, existing_json] = 已被占（竞态输家未写入，不续期）
_RESERVE_LUA = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    return {0, existing}
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return {1, ''}
"""

# 幂等摘要写（overwrite 路径 / 消费端权威刷新）：无条件 HSET + 无条件续期（idle GC，
# 消费端权威刷新即 TTL 心跳：key 过期时 DB 几乎必然已权威）
# KEYS[1]=existence key, ARGV=[slot, summary_json, ttl_seconds, empty_field]
_SET_SUMMARY_LUA = """
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
"""

# 空实体哨兵：仅 key 不存在时写入（哨兵闸门的原子保障，防与占位竞态）
# KEYS[1]=existence key, ARGV=[empty_ttl_seconds, empty_field]
_EMPTY_MARKER_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], ARGV[2], '{"empty":true}')
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    return 1
end
return 0
"""


def parse_summary(raw: str) -> dict[str, object]:
    """解析 field value；损坏数据不致命（返回空摘要，宁可多报 409）。"""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    logger.warning("cache_meta_corrupt", raw=raw[:100])
    return {}


def dumps_summary(summary: dict[str, object]) -> str:
    return json.dumps(summary, ensure_ascii=False, default=str)


class RedisExistenceCache:
    def __init__(
        self, config: RedisConfig, client: aioredis.Redis | None = None
    ) -> None:
        self._config = config
        self._injected = client
        self._client: aioredis.Redis | None = None

    @property
    def existence_ttl_seconds(self) -> int:
        return self._config.existence_ttl_seconds

    async def start(self) -> None:
        if self._client is not None:
            return
        if self._injected is not None:
            self._client = self._injected
            return
        self._client = aioredis.from_url(
            self._config.url,
            socket_timeout=self._config.socket_timeout_ms / 1000,
            socket_connect_timeout=self._config.socket_timeout_ms / 1000,
            decode_responses=True,
        )
        logger.info("redis_client_created", url=self._config.url)

    async def close(self) -> None:
        if self._client is None:
            return
        if self._injected is None:  # 注入的客户端归调用方关闭
            await self._client.aclose()
        self._client = None
        logger.info("redis_client_closed")

    async def check_health_detail(self) -> tuple[bool, str | None]:
        """健康预检。返回 (是否可用, 错误信息)；可用时 error 为 None。"""
        if self._client is None:
            return False, "redis client not started"
        try:
            await asyncio.wait_for(self._client.ping(), timeout=_HEALTH_TIMEOUT_S)
            return True, None
        except Exception as e:
            logger.debug("redis_health_check_failed", error=str(e))
            return False, str(e)

    async def check_health(self) -> bool:
        ok, _ = await self.check_health_detail()
        return ok

    def existence_key(self, entity: str) -> str:
        return f"{self._config.key_prefix}existence:{entity}"

    # ---------- 内部工具 ----------

    def _require_client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("RedisExistenceCache not started, call start() first")
        return self._client

    async def _read(self, coro: Awaitable[_T]) -> _T:
        """读操作统一超时（查询路径 socket_timeout_ms）。"""
        return await asyncio.wait_for(coro, timeout=self._config.socket_timeout_ms / 1000)

    async def _write(self, coro: Awaitable[_T]) -> _T:
        """写操作统一超时（接收路径 recv_timeout_ms）。"""
        return await asyncio.wait_for(coro, timeout=self._config.recv_timeout_ms / 1000)

    # ---------- 读 ----------

    async def get_field_meta(self, entity: str, slot: str) -> dict[str, object] | None:
        raw = await self._read(self._require_client().hget(self.existence_key(entity), slot))
        if raw is None:
            return None
        return parse_summary(str(raw))

    async def key_exists(self, entity: str) -> bool:
        return bool(await self._read(self._require_client().exists(self.existence_key(entity))))

    async def get_entity_fields(self, entity: str) -> dict[str, dict[str, object]] | None:
        raw: dict[str, str] = await self._read(
            self._require_client().hgetall(self.existence_key(entity))
        )  # type: ignore[assignment]
        if not raw:
            return None  # Redis 中不存在只有 0 个 field 的 HASH，空 dict 即 key 不存在
        return {
            k: parse_summary(v) for k, v in raw.items() if k != EMPTY_FIELD
        }

    async def get_ttl(self, entity: str) -> int:
        return int(await self._read(self._require_client().ttl(self.existence_key(entity))))

    # ---------- 写 ----------

    async def reserve_field(
        self, entity: str, slot: str, summary: dict[str, object]
    ) -> tuple[bool, dict[str, object] | None]:
        client = self._require_client()
        script = client.register_script(_RESERVE_LUA)
        result = await self._write(
            script(
                keys=[self.existence_key(entity)],
                args=[slot, dumps_summary(summary), self._config.existence_ttl_seconds, EMPTY_FIELD],
            )
        )
        reserved, existing = int(result[0]), result[1]  # type: ignore[index]
        if reserved == 1:
            return (True, None)
        return (False, parse_summary(str(existing)))

    async def write_summary(
        self, entity: str, slot: str, summary: dict[str, object]
    ) -> None:
        client = self._require_client()
        script = client.register_script(_SET_SUMMARY_LUA)
        await self._write(
            script(
                keys=[self.existence_key(entity)],
                args=[slot, dumps_summary(summary), self._config.existence_ttl_seconds, EMPTY_FIELD],
            )
        )

    async def write_entity_fields(
        self, entity: str, slots: dict[str, dict[str, object]]
    ) -> None:
        if not slots:
            return
        client = self._require_client()
        key = self.existence_key(entity)
        mapping = {slot: dumps_summary(meta) for slot, meta in slots.items()}
        async with client.pipeline(transaction=False) as pipe:
            # redis-py 桩的 FieldT 不变量约束无法匹配 Mapping[str, str]（运行时合法）
            pipe.hset(key, mapping=mapping)  # type: ignore[reportArgumentType]
            pipe.hdel(key, EMPTY_FIELD)
            pipe.expire(key, self._config.existence_ttl_seconds)  # 无条件续期
            await self._write(pipe.execute())

    async def write_empty_marker(self, entity: str) -> bool:
        client = self._require_client()
        script = client.register_script(_EMPTY_MARKER_LUA)
        created = await self._write(
            script(
                keys=[self.existence_key(entity)],
                args=[self._config.empty_existence_ttl_seconds, EMPTY_FIELD],
            )
        )
        return int(created) == 1  # type: ignore[arg-type]

    async def delete_entity(self, entity: str) -> None:
        await self._write(self._require_client().delete(self.existence_key(entity)))
