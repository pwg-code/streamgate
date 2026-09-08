"""MSSQL 落库入口（extras：streamgate[sql]，驱动 aioodbc）。

生产路径：connection_string 走 aioodbc（如
`mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server`）。
幂等 upsert 用多行 VALUES + MERGE（HOLDLOCK，按参数预算分块）；写侧
异常按 SQL Server 错误号分类（RETRY/POISON）；驱动语句超时经 connect
钩子落到 pyodbc 系连接。
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
