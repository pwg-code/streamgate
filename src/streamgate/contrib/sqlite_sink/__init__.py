"""SQLite 落库入口（extras：streamgate[sql]，驱动 aiosqlite）。

dev/测试路径：connection_string 形如
`sqlite+aiosqlite:///./data/streamgate.db`。键上有唯一约束/主键时走
ON CONFLICT DO UPDATE，否则事务内 DELETE+INSERT 兜底；启动期自动
建表（WAL/busy_timeout/foreign_keys PRAGMA）。
"""

from streamgate.contrib.sql_sink import (
    DbConfig,
    SQLAlchemyErrorClassifier,
    SqlBackfill,
    Upsert,
    UpsertWriter,
    async_session_factory,
    create_write_engine,
    execute_upserts,
)

__all__ = [
    "DbConfig",
    "SqlBackfill",
    "SQLAlchemyErrorClassifier",
    "Upsert",
    "UpsertWriter",
    "async_session_factory",
    "create_write_engine",
    "execute_upserts",
]
