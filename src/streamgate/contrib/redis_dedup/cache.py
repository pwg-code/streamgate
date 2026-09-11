"""RedisDedupCache：身份键 → summary 的 Redis STRING 缓存（可插拔判重载体的参考存储）。

机制（摘要结构为 JSON dict，每身份键一条）：
- 原子占位（单 key Lua，Cluster 兼容）：GET 命中即返回既有摘要（竞态输家
  不写入、不续期）；未命中写入并按 TTL 落地（idle GC）
- 幂等摘要写：无条件 SET + 续期（TTL 心跳：key 过期时 DB 几乎必然已权威）
- fail-closed 与否是判重载体的参数，缓存层只如实报告错误
"""

import asyncio
import json
from collections.abc import Awaitable
from typing import TypeVar, cast

import redis.asyncio as aioredis

from streamgate import logger
from streamgate.contrib.redis_dedup.config import RedisConfig
from streamgate.protocols import JsonObject

_T = TypeVar("_T")

_HEALTH_TIMEOUT_S = 2.0

# 原子占位（单 key，Cluster 兼容）：命中即返回既有摘要，未命中写入 + TTL
# KEYS[1]=dedup key, ARGV=[summary_json, ttl_seconds]
# 返回 [1, ""] = 占位成功；[0, existing_json] = 已被占（竞态输家未写入，不续期）
_RESERVE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
    return {0, existing}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return {1, ''}
"""

# 幂等摘要写（force 路径 / 消费端权威刷新）：无条件 SET + 无条件续期
# （idle GC，消费端权威刷新即 TTL 心跳）
# KEYS[1]=dedup key, ARGV=[summary_json, ttl_seconds]
_WRITE_LUA = """
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""


def parse_summary(raw: str) -> JsonObject:
    """解析占位/摘要值；损坏数据不致命（返回空摘要，宁可多报 duplicate）。"""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    logger.warning("cache_meta_corrupt", raw=raw[:100])
    return {}


def dumps_summary(summary: JsonObject) -> str:
    return json.dumps(summary, ensure_ascii=False, default=str)


class RedisDedupCache:
    def __init__(
        self, config: RedisConfig, client: aioredis.Redis | None = None
    ) -> None:
        self._config = config
        self._injected = client
        self._client: aioredis.Redis | None = None

    @property
    def identity_ttl_seconds(self) -> int:
        return self._config.identity_ttl_seconds

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

    def dedup_key(self, identity: str) -> str:
        return f"{self._config.key_prefix}dedup:{identity}"

    # ---------- 内部工具 ----------

    def _require_client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("RedisDedupCache not started, call start() first")
        return self._client

    async def _read(self, coro: Awaitable[_T]) -> _T:
        """读操作统一超时（查询/校验路径 socket_timeout_ms）。"""
        return await asyncio.wait_for(
            coro, timeout=self._config.socket_timeout_ms / 1000
        )

    async def _write(self, coro: Awaitable[_T]) -> _T:
        """写操作统一超时（推入路径 recv_timeout_ms）。"""
        return await asyncio.wait_for(coro, timeout=self._config.recv_timeout_ms / 1000)

    # ---------- 读 ----------

    async def get_summary(self, identity: str) -> JsonObject | None:
        # redis-py 命令类型为 ResponseT（Awaitable|值 联合，同步模式兼容）；
        # 异步 client 恒返回 Awaitable，cast 仅为收窄静态类型
        raw = await self._read(
            cast(
                "Awaitable[str | None]",
                self._require_client().get(self.dedup_key(identity)),
            )
        )
        if raw is None:
            return None
        return parse_summary(str(raw))

    async def get_ttl(self, identity: str) -> int:
        return int(
            await self._read(self._require_client().ttl(self.dedup_key(identity)))
        )

    # ---------- 写 ----------

    async def reserve(
        self, identity: str, summary: JsonObject
    ) -> tuple[bool, JsonObject | None]:
        """原子检查 + 占位。返回 (True, None) = 占位成功；(False, 既有摘要) = 已被占。"""
        client = self._require_client()
        script = client.register_script(_RESERVE_LUA)
        result = await self._write(
            script(
                keys=[self.dedup_key(identity)],
                args=[dumps_summary(summary), self._config.identity_ttl_seconds],
            )
        )
        reserved, existing = int(result[0]), result[1]  # type: ignore[index]
        if reserved == 1:
            return (True, None)
        return (False, parse_summary(str(existing)))

    async def write_summary(self, identity: str, summary: JsonObject) -> None:
        """幂等摘要写（force 路径 / 消费端权威刷新），无条件续期。"""
        client = self._require_client()
        script = client.register_script(_WRITE_LUA)
        await self._write(
            script(
                keys=[self.dedup_key(identity)],
                args=[dumps_summary(summary), self._config.identity_ttl_seconds],
            )
        )

    async def delete(self, identity: str) -> None:
        await self._write(self._require_client().delete(self.dedup_key(identity)))
