"""SQL 回源实现（BackfillSource 协议的 SQL 实现）。

统一回源契约：scope 为身份键或组号，返回 {identity: summary} 映射。
- 未配 group_column：单键回源（WHERE key_column = :scope → 单条目映射），
  与 1.0.0 行为一一对应
- 配了 group_column：整组回源（WHERE group_column = :scope → 全组映射）
供 streamgate.contrib.redis_dedup 的单键/组载体注入。
"""

import asyncio
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from streamgate import logger
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
    """SQL 回源：按统一 scope 加载身份 → 摘要映射。

    行布局固定：key_column, *order_columns, *extra_summary_columns。
    同身份多行取 order_columns 排序"最新"一行（幂等摘要语义）；
    order_columns 按优先级降序排列（如 ["tested_at", "seq_no"]），None 视为最小；
    summary_columns: summary 键 → 列名（值为 None 表示输出固定 null）。
    group_column 可选：未设 = 单键回源（WHERE key_column = :scope）；
    已设 = 整组回源（WHERE group_column = :scope，返回该组全量身份）。
    调用层超时（query_wait_seconds），超时抛 asyncio.TimeoutError。
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        table: str,
        key_column: str,
        summary_columns: dict[str, str | None],
        order_columns: list[str],
        query_wait_seconds: float = 8.0,
        component_label: str = "sql_backfill",
        group_column: str | None = None,
    ) -> None:
        self._engine: AsyncEngine | None = engine
        self._table = table
        self._summary_columns = summary_columns
        self._order_columns = list(order_columns)
        self._query_wait_seconds = query_wait_seconds
        self._label = component_label
        self._group_column = group_column
        # 行布局固定：key_column, *order_columns, *extra_summary_columns
        # （summary_columns 值为 None 的键不在 SELECT 中，恒输出 null）
        self._extra_summary: list[tuple[str, str]] = [
            (key, col) for key, col in summary_columns.items() if col is not None
        ]
        self._null_summary_keys = [
            key for key, col in summary_columns.items() if col is None
        ]
        select_cols = [
            key_column, *order_columns, *(col for _, col in self._extra_summary)
        ]
        where_col = group_column or key_column
        # 列名/表名来自使用方受控配置，非用户输入，无注入面
        self._sql = text(
            f"SELECT {', '.join(select_cols)} "
            f"FROM {table} WHERE {where_col} = :k"
        )
        logger.info(
            f"{component_label}_initialized",
            engine="sqlite" if "sqlite" in str(getattr(engine, "url", "")).lower() else "mssql",
            scope="group" if group_column is not None else "identity",
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

    async def load(self, scope: str) -> dict[str, JsonObject] | None:
        """统一回源：返回 {identity: summary} 映射；scope 无记录返回 {}。

        scope = 身份键（单键载体）或组号（组载体，返回整组全量身份）。
        空结果返回 {}（与旧"确认不存在"语义一一对应）。
        """
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        try:
            rows = await asyncio.wait_for(
                self._fetch_rows(scope), timeout=self._query_wait_seconds
            )
        except asyncio.TimeoutError:
            logger.error(
                f"{self._label}_load_timeout",
                scope=scope,
                wait_seconds=self._query_wait_seconds,
            )
            raise
        if not rows:
            return {}
        return self._mapping(rows)

    async def _fetch_rows(self, scope: str) -> list[tuple[object, ...]]:
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        async with self._engine.connect() as conn:
            result = await conn.execute(self._sql, {"k": scope})
            return [tuple(row) for row in result.fetchall()]

    def _mapping(self, rows: list[tuple[object, ...]]) -> dict[str, JsonObject]:
        """行按 identity（SELECT 首列 key_column）归一，同身份多行取最新一行。"""
        grouped: dict[str, list[tuple[object, ...]]] = {}
        for row in rows:
            identity = str(row[0])
            grouped.setdefault(identity, []).append(row[1:])
        mapping: dict[str, JsonObject] = {}
        for identity, sub_rows in grouped.items():
            best = self._best_row(sub_rows)
            if best is not None:
                mapping[identity] = best
        return mapping

    def _best_row(self, rows: list[tuple[object, ...]]) -> JsonObject | None:
        """存量重复行：取排序列最大的一行（幂等摘要语义，方言无关）。

        row 布局与 SELECT 列序一致（key_column 已剥离）：*order_columns,
        *extra_summary_columns。
        """
        n_order = len(self._order_columns)
        best: tuple[tuple[object, ...], JsonObject] | None = None
        for row in rows:
            summary: JsonObject = {}
            for key in self._null_summary_keys:
                summary[key] = None
            for i, (key, _col) in enumerate(self._extra_summary):
                summary[key] = _jsonable(row[n_order + i])
            sort_values = row[0:n_order]
            sort_key = tuple(
                (v is not None, v if v is not None else 0) for v in sort_values
            )
            if best is None or sort_key > best[0]:
                best = (sort_key, summary)
        return best[1] if best is not None else None


__all__ = ["SqlBackfill"]
