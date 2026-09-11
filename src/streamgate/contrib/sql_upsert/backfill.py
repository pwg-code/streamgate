"""SQL 回源实现（BackfillSource 协议的 SQL 实现）。

冷身份单键加载 summary，供
streamgate.contrib.redis_dedup 的 RedisDedupCarrier 注入。
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
    """SQL 回源：按身份键加载单条摘要。

    同键多行取 order_columns 排序"最新"一行（幂等摘要语义）。
    order_columns 按优先级降序排列（如 ["tested_at", "seq_no"]），None 视为最小；
    summary_columns: summary 键 → 列名（值为 None 表示输出固定 null）。
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
    ) -> None:
        self._engine: AsyncEngine | None = engine
        self._table = table
        self._summary_columns = summary_columns
        self._order_columns = list(order_columns)
        self._query_wait_seconds = query_wait_seconds
        self._label = component_label
        # 行布局固定：*order_columns, *extra_summary_columns
        # （summary_columns 值为 None 的键不在 SELECT 中，恒输出 null）
        self._extra_summary: list[tuple[str, str]] = [
            (key, col) for key, col in summary_columns.items() if col is not None
        ]
        self._null_summary_keys = [
            key for key, col in summary_columns.items() if col is None
        ]
        select_cols = [
            *order_columns, *(col for _, col in self._extra_summary)
        ]
        # 列名/表名来自使用方受控配置，非用户输入，无注入面
        self._sql = text(
            f"SELECT {', '.join(select_cols)} "
            f"FROM {table} WHERE {key_column} = :k"
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

    async def load(self, identity: str) -> JsonObject | None:
        """冷身份回源：返回既有摘要；None = 确认不存在（框架直接原子占位）。"""
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        try:
            rows = await asyncio.wait_for(
                self._fetch_rows(identity), timeout=self._query_wait_seconds
            )
        except asyncio.TimeoutError:
            logger.error(
                f"{self._label}_load_timeout",
                identity=identity,
                wait_seconds=self._query_wait_seconds,
            )
            raise
        return self._best_row(rows)

    async def _fetch_rows(self, identity: str) -> list[tuple[object, ...]]:
        if self._engine is None:
            raise RuntimeError("SqlBackfill not started, engine not injected")
        async with self._engine.connect() as conn:
            result = await conn.execute(self._sql, {"k": identity})
            return [tuple(row) for row in result.fetchall()]

    def _best_row(self, rows: list[tuple[object, ...]]) -> JsonObject | None:
        """存量重复行：取排序列最大的一行（幂等摘要语义，方言无关）。

        row 布局与 SELECT 列序一致：*order_columns, *extra_summary_columns。
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
