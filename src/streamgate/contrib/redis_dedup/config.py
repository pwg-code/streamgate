"""Redis 连接与判重缓存配置（streamgate.contrib.redis_dedup）。

Redis 是使用方的基础设施：连接配置由使用方自带，不进框架核心。
"""

from pydantic import BaseModel


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379/0"
    key_prefix: str = "streamgate:"
    identity_ttl_seconds: int = 18000        # 占位/摘要 TTL 5h（idle GC：写即续期回满额）
    socket_timeout_ms: int = 1000            # 查询/校验路径超时
    recv_timeout_ms: int = 500               # 推入路径占位/摘要写超时
