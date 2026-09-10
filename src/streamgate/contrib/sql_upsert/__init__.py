"""SQL 幂等 upsert 基座（extras：streamgate[sql]）。

共享实现层：Upsert/upsert_outlet 编排、引擎工厂、SQL 回源、DB 异常分类、
sqlite/mssql 双方言。面向用户的入口为
streamgate.contrib.sqlite_upsert（SqliteConsumer）与
streamgate.contrib.mssql_upsert（MssqlConsumer）预装配工厂；
本包供自组装（Consumer + upsert_outlet + 分类器自由搭配）使用。
"""

from streamgate.contrib._deps import require_extra_import

try:
    from streamgate.contrib.sql_upsert.backfill import SqlBackfill
    from streamgate.contrib.sql_upsert.classifier import SQLAlchemyErrorClassifier
    from streamgate.contrib.sql_upsert.config import DbConfig
    from streamgate.contrib.sql_upsert.engines import (
        async_session_factory,
        create_write_engine,
    )
    from streamgate.contrib.sql_upsert.upsert import (
        Upsert,
        execute_upserts,
        upsert_outlet,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)

__all__ = [
    "DbConfig",
    "SqlBackfill",
    "SQLAlchemyErrorClassifier",
    "Upsert",
    "async_session_factory",
    "create_write_engine",
    "execute_upserts",
    "upsert_outlet",
]
