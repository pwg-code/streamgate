"""RedisGroupDedupCache：组 → 组内身份 的 Redis HASH 缓存（分组判重载体的参考存储）。

机制（HASH，field = 组内身份，value = summary JSON）：
- 原子组内占位（单 key Lua，Cluster 兼容）：HGET 命中即返回既有摘要（竞态输家
  不写入、不续期）；未命中 HSET + EXPIRE（idle GC 续期）
- 幂等摘要写：HSET + EXPIRE 无条件续期（force / on_persisted / 回填共用）
- 整组回填 write_group_fields：pipeline 分片写入（分片为纯实现细节，无配置、无上界），
  写后统一 EXPIRE
- 组存在探测 group_exists：EXISTS。组 key 生命周期不可变：仅"整组回填成功"或
  "空组确认后首身份占位"两条路径创建 → 组 key 存在 ⇒ 组内判定可信，无需空组哨兵
- fail-closed 与否是判重载体的参数，缓存层只如实报告错误
"""

import asyncio
from collections.abc import Awaitable, Mapping
from typing import TypeVar, cast

import redis.asyncio as aioredis

from streamgate import logger
from streamgate.contrib.redis_dedup.cache import dumps_summary, parse_summary
from streamgate.contrib.redis_dedup.config import RedisConfig
from streamgate.protocols import JsonObject

_T = TypeVar("_T")

_HEALTH_TIMEOUT_S = 2.0

# 整组回填分片大小（纯实现细节：一次 HSET 的 field 数量上限，非业务配置）
_WRITE_CHUNK_SIZE = 200

# 原子组内占位（单 key，Cluster 兼容）：命中即返回既有摘要，未命中写入 + TTL
# KEYS[1]=group key, ARGV=[identity, summary_json, ttl_seconds]
# 返回 [1, ""] = 占位成功；[0, existing_json] = 已被占（竞态输家未写入，不续期）
_RESERVE_LUA = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    return {0, existing}
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return {1, ''}
"""

# 幂等摘要写（force 路径 / 消费端权威刷新 / 回填共用）：无条件 HSET + 无条件续期
# KEYS[1]=group key, ARGV=[identity, summary_json, ttl_seconds]
_WRITE_LUA = """
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"""


class RedisGroupDedupCache:
    def __init__(
        self, config: RedisConfig, client: aioredis.Redis | None = None
    ) -> None:
        self._config = config
        self._injected = client
        self._client: aioredis.Redis | None = None

    @property
    def group_ttl_seconds(self) -> int:
        return self._config.group_ttl_seconds

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

    def group_key(self, group: str) -> str:
        return f"{self._config.key_prefix}{self._config.group_key_prefix}{group}"

    # ---------- 内部工具 ----------

    def _require_client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("RedisGroupDedupCache not started, call start() first")
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

    async def get_identity_meta(self, group: str, identity: str) -> JsonObject | None:
        # redis-py 命令类型为 ResponseT（Awaitable|值 联合，同步模式兼容）；
        # 异步 client 恒返回 Awaitable，cast 仅为收窄静态类型
        raw = await self._read(
            cast(
                "Awaitable[str | None]",
                self._require_client().hget(self.group_key(group), identity),
            )
        )
        if raw is None:
            return None
        return parse_summary(str(raw))

    async def get_group_fields(self, group: str) -> dict[str, JsonObject]:
        raw = await self._read(
            cast(
                "Awaitable[dict[str, str]]",
                self._require_client().hgetall(self.group_key(group)),
            )
        )
        return {identity: parse_summary(str(payload)) for identity, payload in raw.items()}

    async def get_ttl(self, group: str) -> int:
        return int(
            await self._read(self._require_client().ttl(self.group_key(group)))
        )

    async def group_exists(self, group: str) -> bool:
        hits = int(
            await self._read(self._require_client().exists(self.group_key(group)))
        )
        return hits > 0

    # ---------- 写 ----------

    async def reserve(
        self, group: str, identity: str, summary: JsonObject
    ) -> tuple[bool, JsonObject | None]:
        """原子检查 + 组内占位。返回 (True, None) = 占位成功；(False, 既有摘要) = 已被占。"""
        client = self._require_client()
        script = client.register_script(_RESERVE_LUA)
        result = await self._write(
            script(
                keys=[self.group_key(group)],
                args=[identity, dumps_summary(summary), self._config.group_ttl_seconds],
            )
        )
        reserved, existing = int(result[0]), result[1]  # type: ignore[index]
        if reserved == 1:
            return (True, None)
        return (False, parse_summary(str(existing)))

    async def write_summary(self, group: str, identity: str, summary: JsonObject) -> None:
        """幂等摘要写（force 路径 / 消费端权威刷新），无条件续期。"""
        client = self._require_client()
        script = client.register_script(_WRITE_LUA)
        await self._write(
            script(
                keys=[self.group_key(group)],
                args=[identity, dumps_summary(summary), self._config.group_ttl_seconds],
            )
        )

    async def write_group_fields(self, group: str, mapping: dict[str, JsonObject]) -> None:
        """整组回填：pipeline 分片写入（纯实现细节），写后统一续期。

        best-effort 语义（失败抛异常，由调用方 WARN 告警）：
        - 冷路径：本方法之前组 key 不存在，首片写入即建组；分片中途失败时
          半组对上层不可判定（阶段2组存在快路径要求 group_exists=True 才走零 DB，
          判定路径在回填抛异常时统一 REJECT，不把半组当全集）
        - 幂等：对既有组重复回填全量映射无害（HASH 覆盖写 + 续期）
        """
        if not mapping:
            return
        client = self._require_client()
        key = self.group_key(group)
        items = [
            (identity, dumps_summary(summary)) for identity, summary in mapping.items()
        ]
        for i in range(0, len(items), _WRITE_CHUNK_SIZE):
            chunk = dict(items[i : i + _WRITE_CHUNK_SIZE])
            pipe = client.pipeline()
            pipe.hset(key, mapping=cast("Mapping[str, str]", chunk))  # type: ignore[reportArgumentType]
            pipe.expire(key, self._config.group_ttl_seconds)
            await self._write(pipe.execute())

    async def delete_group(self, group: str) -> None:
        """删除整组（供告警/运维删键修复）。"""
        await self._write(self._require_client().delete(self.group_key(group)))