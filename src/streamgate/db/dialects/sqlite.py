"""SQLite 方言（dev/测试路径）：ON CONFLICT 优先，无唯一约束时 DELETE+INSERT 兜底。"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from streamgate.protocols import JsonObject


def _constraint_covers_keys(model: type[SQLModel], keys: list[str]) -> bool:
    """表上是否存在恰好覆盖 upsert 键的主键/唯一约束（ON CONFLICT 前提）。"""
    table = model.__table__  # type: ignore[reportAttributeAccessIssue]  # SQLModel stub lacks SQLAlchemy Table
    key_set = set(keys)
    for constraint in (*table.constraints, *table.indexes):
        cols = {c.name for c in getattr(constraint, "columns", [])}
        if cols and cols == key_set and getattr(constraint, "unique", False):
            return True
    return False


async def upsert_on_conflict(
    session: AsyncSession,
    model: type[SQLModel],
    rows: list[JsonObject],
    keys: list[str],
    fields: list[str],
) -> None:
    """INSERT ... ON CONFLICT(keys) DO UPDATE（要求键上有唯一约束/主键）。"""
    stmt = sqlite_insert(model).values(rows)
    update_cols = {
        k: stmt.excluded[k] for k in fields if k not in keys
    }
    stmt = stmt.on_conflict_do_update(index_elements=keys, set_=update_cols)
    await session.execute(stmt)


async def delete_insert_by_keys(
    session: AsyncSession,
    model: type[SQLModel],
    rows: list[JsonObject],
    keys: list[str],
) -> None:
    """无唯一约束时的幂等兜底：事务内按键 DELETE 后 INSERT（仅测试环境用）。"""
    pairs = [tuple(d.get(k) for k in keys) for d in rows]
    pairs = [p for p in pairs if all(v is not None for v in p)]
    table_name = model.__tablename__
    where = " AND ".join(f"{k} = :k{i}" for i, k in enumerate(keys))
    stmt = text(f"DELETE FROM {table_name} WHERE {where}")
    for pair in pairs:
        params = {f"k{i}": v for i, v in enumerate(pair)}
        await session.execute(stmt, params)
    await session.execute(model.__table__.insert(), rows)  # type: ignore[reportAttributeAccessIssue]  # SQLModel stub lacks SQLAlchemy Table


def supports_on_conflict(model: type[SQLModel], keys: list[str]) -> bool:
    return _constraint_covers_keys(model, keys)
