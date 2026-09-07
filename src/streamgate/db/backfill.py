"""回源实现：NoBackfill / SqlBackfill（BackfillSource 协议的内置实现）。"""

import asyncio
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from streamgate.obs.logging import logger
from streamgate.protocols import JsonObject


def _jsonable(value: object) -> object:
    """datetime 实例/字符串统一为 ISO（与上游摘要模型序列化行为对齐：
    原实现经 pydantic 包装，DB 驱动返回的 "YYYY-MM-DD HH:MM:SS" 字符串
    会被解析后以 ISO 重新序列化；此处保持同一缓存格式）。"""
    if isinstance(value, datetime):
        return value.isoformat()
    if (
        isinstance(value, str)
        and len(value) >= 19
        and value[4] == "-"
        and value[7] == "-"
    ):
        try:
            return datetime.fromisoformat(value).isoformat()
        except ValueError:
            return value
    return value


class SqlBackfill:
    """SQL 回源：整实体加载 slot→summary，同 slot 多行取"最新"一行。

    order_columns 按优先级降序排列（如 ["tested_at", "seq_no"]），None 视为最小；
    summary_columns: summary 键 → 列名（值为 None 表示输出固定 null）。
    调用层超时（query_wait_seconds），超时抛 asyncio.TimeoutError。
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        table: str,
        entity_column: str,
        slot_column: str,
        summary_columns: dict[str, str | None],
        order_columns: list[str],
        query_wait_seconds: float = 8.0,
        component_label: str = "sql_backfill",
    ) -> None:
        self._engine: AsyncEngine | None = engine
        self._table = table
        self._entity_column = entity_column
        self._slot_column = slot_column
        self._summary_columns = summary_columns
        self._order_columns = list(order_columns)
        self._query_wait_seconds = query_wait_seconds
        self._label = component_label
        # 行布局固定：slot, *order_columns, *extra_summary_columns
        # （summary_columns 值为 None 的键不在 SELECT 中，恒输出 null）
        self._extra_summary: list[tuple[str, str]] = [
            (key, col) for key, col in summary_columns.items() if col is not None
        ]
        self._null_summary_keys = [
            key for key, col in summary_columns.items() if col is None
        ]
        select_cols = [slot_column, *order_columns, *(col for _, col in self._extra_summary)]
        # 列名/表名来自使用方受控配置，非用户输入，无注入面
        self._sql = text(
            f"SELECT {', '.join(select_cols)} "
            f"FROM {table} WHERE {entity_column} = :e"
        )
        logger.info(
            f"{component_label}_initialized",
            engine="sqlite" if "sqlite" in str(getattr(engine, "url", "")).lower() else "mssql",
        )

    async def close(self) -> None:
        if self._engine is None:
            return
        await self._engine.dispose()
        self._engine = None
        logger.info(f"{self._label}_closed")

    async def check_health(self) -> bool:
        ok, _ = await self.check_health_detail()
        return ok

    async def check_health_detail(self) -> tuple[bool, str | None]:
        """连接探测：返回 (是否可用, 错误信息)；失败时错误信息供调用方 ERROR 日志。"""
        if self._engine is None:
            return False, "engine not initialized"
        try:
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True, None
        except Exception as e:
            logger.debug(f"{self._label}_health_failed", error=str(e))
            return False, str(e)

    async def load(self, entity: str) -> dict[str, JsonObject]:
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        try:
            rows = await asyncio.wait_for(
                self._fetch_rows(entity), timeout=self._query_wait_seconds
            )
        except asyncio.TimeoutError:
            logger.error(
                f"{self._label}_load_timeout",
                entity=entity,
                wait_seconds=self._query_wait_seconds,
            )
            raise
        return self._merge_rows(rows)

    async def _fetch_rows(self, entity: str) -> list[tuple[object, ...]]:
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        async with self._engine.connect() as conn:
            result = await conn.execute(self._sql, {"e": entity})
            return [tuple(row) for row in result.fetchall()]

    def _merge_rows(self, rows: list[tuple[object, ...]]) -> dict[str, JsonObject]:
        """存量重复行：每组 slot 取排序列最大的一行（幂等摘要语义）。

        row 布局与 SELECT 列序一致：slot, *order_columns, *extra_summary_columns。
        """
        n_order = len(self._order_columns)
        best: dict[str, tuple[tuple[object, ...], JsonObject]] = {}
        for row in rows:
            slot = row[0]
            if not isinstance(slot, str):
                continue  # 历史脏行防御（None/非字符串主键），不应出现
            summary: JsonObject = {}
            for key in self._null_summary_keys:
                summary[key] = None
            for i, (key, _col) in enumerate(self._extra_summary):
                summary[key] = _jsonable(row[1 + n_order + i])
            sort_values = row[1 : 1 + n_order]
            sort_key = tuple((v is not None, v if v is not None else 0) for v in sort_values)
            if slot not in best or sort_key > best[slot][0]:
                best[slot] = (sort_key, summary)
        return {slot: meta for slot, (_, meta) in best.items()}
