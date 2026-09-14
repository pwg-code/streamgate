"""MSSQL 出口预装配（extras：streamgate[sql]，驱动 aioodbc）。

生产路径：db 连接串走 aioodbc（如
`mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server`）。
幂等 upsert 用多行 VALUES + MERGE（HOLDLOCK，按参数预算分块）；异常
按 SQL Server 错误号分类（RETRY/POISON）；驱动语句超时经 connect
钩子落到 pyodbc 系连接。建表交迁移工具（框架不做 schema 管理）。

MssqlConsumer 预接三件套（批量 upsert handler + 单条探针 + 方言异常
分类器）并返回核心 Consumer；batch_size / flush_timeout / options 与
核心 Consumer 完全一致。
"""

from streamgate import Consumer, ConsumerOptions
from streamgate.consumer.runner import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_FLUSH_TIMEOUT_SECONDS,
)
from streamgate.contrib._deps import require_extra_import
from streamgate.contrib.sql_upsert.factory import resolve_db, sql_upsert_consumer

try:
    from streamgate.contrib.sql_upsert import (
        DbConfig,
        SQLAlchemyErrorClassifier,
        SqlBackfill,
        Upsert,
        async_session_factory,
        create_write_engine,
        execute_upserts,
        upsert_outlet,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)


def MssqlConsumer(
    db: str | DbConfig,
    upserts: list[Upsert],
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    flush_timeout: float = DEFAULT_FLUSH_TIMEOUT_SECONDS,
    options: ConsumerOptions | None = None,
) -> Consumer:
    """MSSQL upsert 出口开箱糖：预接 handler/probe/classifier 三件套。

    - db：连接串（str 快捷方式）或 DbConfig；
    - upserts：upsert 目标声明（模型 + 幂等键）；
    - 其余参数与核心 Consumer 逐字一致（必填项真必填；batch_size /
      flush_timeout 不传 = 内置默认值）；
    - options 透传合并：传入字段覆盖工厂默认（如换 collapse_key、关 DLQ）。
    """
    return sql_upsert_consumer(
        db=resolve_db(db),
        upserts=upserts,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        batch_size=batch_size,
        flush_timeout=flush_timeout,
        options=options,
    )


__all__ = [
    "DbConfig",
    "MssqlConsumer",
    "SqlBackfill",
    "SQLAlchemyErrorClassifier",
    "Upsert",
    "async_session_factory",
    "create_write_engine",
    "execute_upserts",
    "upsert_outlet",
]
