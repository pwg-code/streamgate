"""SQL 出口预装配工厂（sqlite_upsert / mssql_upsert 共用）。

预接三件套：handler + probe（upsert_outlet 原语）与方言异常分类器；
使用方传入的 options 字段级覆盖工厂默认，其余透传。
返回值即核心 Consumer（无子类层级：全项目只有一种 Consumer）。
"""

import dataclasses

from streamgate import Consumer, ConsumerOptions
from streamgate.contrib.sql_upsert.classifier import SQLAlchemyErrorClassifier
from streamgate.contrib.sql_upsert.config import DbConfig
from streamgate.contrib.sql_upsert.upsert import Upsert, upsert_outlet


def _merge_options(
    base: ConsumerOptions, override: ConsumerOptions | None
) -> ConsumerOptions:
    """字段级合并：override 中取值 ≠ 默认值的字段覆盖 base，其余沿用 base。

    子对象（dlq/tuning）整体替换：传 DlqOptions(enabled=False) 即整体
    生效，无需逐字段对照工厂默认。
    """
    if override is None:
        return base
    defaults = ConsumerOptions()
    changed = {
        f.name: getattr(override, f.name)
        for f in dataclasses.fields(ConsumerOptions)
        if getattr(override, f.name) != getattr(defaults, f.name)
    }
    return dataclasses.replace(base, **changed)


def resolve_db(db: str | DbConfig) -> DbConfig:
    """连接串快捷方式：str → DbConfig(connection_string=str)。"""
    if isinstance(db, DbConfig):
        return db
    if isinstance(db, str):
        return DbConfig(connection_string=db)
    raise TypeError(
        f"db must be a connection string or DbConfig, got {type(db).__name__}"
    )


def sql_upsert_consumer(
    *,
    db: DbConfig,
    upserts: list[Upsert],
    bootstrap_servers: str | None,
    topic: str | None,
    group_id: str | None,
    batch_size: int | None,
    flush_timeout: float | None,
    options: ConsumerOptions | None,
) -> Consumer:
    """预装配 SQL 出口 Consumer：handler/probe/classifier 三件套生效。

    建表行为由出口载体 start() 决定（sqlite 自动建表；mssql 交迁移工具）。
    """
    handler, probe = upsert_outlet(db, upserts)
    merged = _merge_options(
        ConsumerOptions(probe=probe, classifier=SQLAlchemyErrorClassifier()),
        options,
    )
    return Consumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        handler=handler,
        batch_size=batch_size,
        flush_timeout=flush_timeout,
        options=merged,
    )


__all__ = ["resolve_db", "sql_upsert_consumer"]
