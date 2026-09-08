"""SQL 幂等 upsert 落地基座（extras：streamgate[sql]）。

共享实现层：Upsert/UpsertWriter 编排、引擎工厂、SQL 回源、DB 异常分类、
sqlite/mssql 双方言。面向用户的入口为
streamgate.contrib.sqlite_sink 与 streamgate.contrib.mssql_sink（两者均
从本包完整导出基座 API，方言由 DbConfig.connection_string 驱动）。
"""

from streamgate.contrib._deps import require_extra_import

try:
    from streamgate.contrib.sql_sink.backfill import SqlBackfill
    from streamgate.contrib.sql_sink.classifier import SQLAlchemyErrorClassifier
    from streamgate.contrib.sql_sink.config import DbConfig
    from streamgate.contrib.sql_sink.engines import (
        async_session_factory,
        create_write_engine,
    )
    from streamgate.contrib.sql_sink.upsert import (
        Upsert,
        UpsertWriter,
        execute_upserts,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)

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
