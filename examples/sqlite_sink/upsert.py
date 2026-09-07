"""Upsert 声明 + 幂等 upsert 编排（原 streamgate Upsert sugar 平移）。

原核心的 `Upsert`（specs.py）与 `UpsertWriter`（db/upsert.py）合并为本模块，
作为 RecordWriter 协议的参考实现演示注入用法。
"""

import asyncio
from collections.abc import Callable

from config import DbConfig
from dialects import mssql as mssql_dialect
from dialects import sqlite as sqlite_dialect
from dialects.mssql import mssql_cast_types
from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlmodel import SQLModel

from streamgate import logger
from streamgate.protocols import JsonObject, WriteResult

# 行级变换钩子类型（Upsert.prepare 参数）
RowTransform = Callable[[JsonObject], JsonObject]


class Upsert:
    """一个落库目标：模型 + 幂等键（+ 可选字段排除/行级变换）。

    name 用于写入计数与 batch_write_success 日志字段（f"{name}_count"）；
    prepare 为行级策略钩子（如时区归一、字段清洗），在字段投影前执行；
    exclude 为不参与 INSERT/UPDATE 的列（如 IDENTITY 自增主键）。
    """

    def __init__(
        self,
        model: type[SQLModel],
        keys: list[str],
        name: str = "",
        exclude: tuple[str, ...] = (),
        prepare: RowTransform | None = None,
    ) -> None:
        self.model = model
        self.keys = keys
        self.name = name
        self.exclude = exclude
        self.prepare = prepare
        if not self.keys:
            raise ValueError("Upsert.keys must not be empty")
        mapper = sa_inspect(self.model)
        self.fields: list[str] = [
            prop.key
            for prop in mapper.column_attrs
            if prop.key not in self.exclude
        ]
        missing = [k for k in self.keys if k not in self.fields]
        if missing:
            raise ValueError(f"Upsert keys not in model fields: {missing}")
        # 属性名 → DB 列名（保留模型元数据中的原始列名，含大小写/特殊字符）
        self.column_names: dict[str, str] = {
            prop.key: prop.columns[0].name
            for prop in mapper.column_attrs
        }
        self.cast_types: dict[str, str] = mssql_cast_types(self.model)
        if not self.name:
            self.name = str(self.model.__tablename__).lower()


def _create_tables(sync_conn: object) -> None:
    """SQLModel.metadata.create_all 的同步回调。"""
    SQLModel.metadata.create_all(sync_conn)  # type: ignore[reportAttributeAccessIssue]


def _project_rows(spec: Upsert, records: list[JsonObject]) -> list[JsonObject]:
    """单目标行投影：prepare → 字段投影 → 键完整性校验。"""
    rows: list[JsonObject] = []
    for record in records:
        data = spec.prepare(record) if spec.prepare is not None else record
        data = {k: data.get(k) for k in spec.fields}
        missing = [k for k in spec.keys if not data.get(k)]
        if missing:
            raise ValueError(
                f"Missing {missing[0]} in record: "
                f"{data.get(spec.keys[0])}_{data.get(spec.keys[-1])}"
            )
        rows.append(data)
    return rows


async def _write_single_target(
    session: AsyncSession,
    spec: Upsert,
    records: list[JsonObject],
    *,
    is_sqlite: bool,
) -> int:
    """把投影后的记录写入单个 upsert 目标，返回写入行数。"""
    rows = mssql_dialect.dedup_rows(_project_rows(spec, records), spec.keys)
    if not rows:
        return 0
    if is_sqlite:
        if sqlite_dialect.supports_on_conflict(spec.model, spec.keys):
            # 复合键主键/唯一约束在位：ON CONFLICT 按序后者覆盖
            await sqlite_dialect.upsert_on_conflict(
                session, spec.model, rows, spec.keys, spec.fields
            )
        else:
            # 键上无唯一约束（如 IDENTITY 主键宽表）：事务内 DELETE+INSERT 兜底
            await sqlite_dialect.delete_insert_by_keys(
                session, spec.model, rows, spec.keys
            )
    else:
        for chunk in mssql_dialect.chunk_by_param_budget(rows, len(spec.fields)):
            sql, params = mssql_dialect.build_multirow_merge(
                table_name=str(spec.model.__tablename__),
                fields=spec.fields,
                keys=spec.keys,
                column_names=spec.column_names,
                cast_types=spec.cast_types,
                rows=chunk,
            )
            await session.execute(sql, params)
    return len(rows)


async def execute_upserts(
    session: AsyncSession,
    upserts: list[Upsert],
    records: list[JsonObject],
    *,
    is_sqlite: bool,
) -> dict[str, int]:
    """把一批记录写入全部声明的 upsert 目标（单事务由调用方管理）。

    每个目标：prepare → 投影字段 → 键完整性校验 → 防御性去重 → 方言 upsert。
    """
    counts: dict[str, int] = {}
    for spec in upserts:
        counts[spec.name] = await _write_single_target(
            session, spec, records, is_sqlite=is_sqlite
        )
    return counts


class UpsertWriter:
    """RecordWriter 参考实现：幂等 upsert 编排（含启动建表/健康探测/关闭）。"""

    def __init__(
        self,
        config: DbConfig,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        upserts: list[Upsert],
    ) -> None:
        self._config = config
        self._engine: AsyncEngine | None = engine
        self._session_factory: async_sessionmaker[AsyncSession] | None = session_factory
        self._upserts = upserts

    async def start(self) -> None:
        """建表（dev sqlite）+ 启动期数据库连接探测（RecordWriter.start 实现）。

        engine 与 session factory 由装配点注入。连接成功 INFO
        `db_connected`，失败 ERROR `db_connection_failed`（不抛出；消费循环
        与健康检查以真实状态运行，DB 恢复后自动续写）。
        """
        engine = self._config.dialect
        url = self._config.redacted_connection_string
        try:
            if engine == "sqlite":
                assert self._engine is not None
                async with self._engine.begin() as conn:
                    await conn.execute(text("PRAGMA journal_mode=WAL"))
                    await conn.execute(text("PRAGMA busy_timeout=5000"))
                    await conn.execute(text("PRAGMA foreign_keys=ON"))
                    await conn.run_sync(_create_tables)
                logger.info("db_initialized", engine=engine, url=url)
            # 统一启动期探测（mssql 分支建连校验；sqlite 建表即已连，再探一次成本可忽略）
            ok, err = await self.check_health_detail()
            if ok:
                logger.info("db_connected", engine=engine, url=url)
            else:
                logger.error("db_connection_failed", engine=engine, url=url, error=err)
        except Exception as e:
            logger.error("db_connection_failed", engine=engine, url=url, error=str(e))

    async def close(self) -> None:
        if self._engine is None:
            return
        await self._engine.dispose()
        self._engine = None
        self._session_factory = None
        logger.info("db_disconnected")

    async def check_health(self) -> bool:
        ok, _ = await self.check_health_detail()
        return ok

    async def check_health_detail(self) -> tuple[bool, str | None]:
        """连接探测：返回 (是否可用, 错误信息)；失败时错误信息供调用方 ERROR 日志。"""
        if self._engine is None:
            return False, "engine not initialized"
        try:
            async def _ping() -> None:
                assert self._engine is not None
                async with self._engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))

            await asyncio.wait_for(_ping(), timeout=2.0)
            return True, None
        except Exception as e:
            logger.debug("db_health_check_failed", error=str(e))
            return False, str(e)

    async def write(self, batch: list[JsonObject]) -> WriteResult:
        """批量写入：prepare/投影 + 多目标 upsert，单事务。

        整个事务段（含 COMMIT）被 wait_for 包裹 -- MERGE 的 HOLDLOCK
        锁等待常发生在 COMMIT 阶段，只包 execute 不够。不变式：驱动超时
        （write_timeout_seconds）< wait_for（write_wait_seconds），驱动先报错、
        事务段以真实异常退出，wait_for 仅最后兜底；超时向上抛给消费循环
        既有重试→退避→paused 自愈分支。
        """
        if not batch:
            return WriteResult(counts={})
        if self._session_factory is None:
            raise RuntimeError("UpsertWriter not initialized, call start() first")

        try:
            counts = await asyncio.wait_for(
                self._write_tx(batch),
                timeout=self._config.write_wait_seconds,
            )
        except asyncio.TimeoutError:
            logger.error(
                "write_batch_timeout",
                batch_size=len(batch),
                wait_seconds=self._config.write_wait_seconds,
            )
            raise
        return WriteResult(counts=counts)

    async def _write_tx(self, batch: list[JsonObject]) -> dict[str, int]:
        """单事务批量写入（从 write 抽出，供 wait_for 包裹）。"""
        factory = self._session_factory
        if factory is None:
            raise RuntimeError("UpsertWriter not initialized, call start() first")
        async with factory() as session:
            async with session.begin():
                counts = await execute_upserts(
                    session, self._upserts, batch, is_sqlite=self._config.dialect == "sqlite"
                )
        return counts


__all__ = ["Upsert", "UpsertWriter", "execute_upserts"]
