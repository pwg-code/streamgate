"""示例本地 Redis 配置（原框架 RedisConfig 的示例化平移）。

Redis 是使用方的世界：连接配置由使用方自带，不进框架核心。
"""

from pydantic import BaseModel


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379/0"
    key_prefix: str = "streamgate:"
    existence_ttl_seconds: int = 18000       # existence TTL 5h
    empty_existence_ttl_seconds: int = 3600  # 空实体哨兵 TTL（短于 existence TTL）
    socket_timeout_ms: int = 1000      # 查询/校验路径超时
    recv_timeout_ms: int = 500         # 接收路径占位/摘要写超时
    # ingest 自身 Redis 不可用时 fail-closed：校验/overwrite/查询路径全部返回
    # 错误而非静默降级（杜绝无占位接受与不完整查询结果）；false=旧降级逃生门
    fail_closed_on_unavailable: bool = True
