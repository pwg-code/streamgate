"""MSSQL 方言：多行 VALUES + MERGE 幂等 upsert（按参数预算分块）。

SQLAlchemy 2.0 的 mssql 方言未提供 MERGE 构造，用 text() 写原生 SQL。

并发前提（生产实测教训，勿"简化"）：
- HOLDLOCK 是并发正确性的必需项（序列化同键 MERGE，防止双方同时
  判定 NOT MATCHED 而双插），不得移除；
- 外部写入方竞态可能在存量表中制造同键重复行，MERGE"target N 行匹配
  全部 UPDATE"语义是对该现实的对冲，不得收紧为单行更新；
- 阻塞/死锁（1205 被选为 victim）由整事务超时与上层通用重试兜底，
  事务回滚天然保原子性。
"""

from collections.abc import Iterator

from sqlalchemy import String, TextClause, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects import mssql
from sqlmodel import SQLModel

from streamgate.protocols import JsonObject

# SQL Server 单语句参数上限约 2100，预留 sp_executesql 内部参数开销后的
# 安全预算；每块行数 = 预算 // 字段数，随字段数动态计算（不写死行数）。
SAFE_PARAM_BUDGET = 2000


def mssql_cast_types(model: type[SQLModel]) -> dict[str, str]:
    """按表元数据生成各属性的 MSSQL CAST 目标类型（键为 Python 属性名）。

    应对 B4：VALUES 行构造器中某列全为 NULL 参数时，类型推导可能给出与
    目标列不匹配的类型，显式 CAST 固定每列类型。字符串列统一 NVARCHAR(max)：
    驱动（aioodbc/pyodbc）本就按 Unicode 声明字符串参数，与既有单行 MERGE
    的比较语义一致，且规避 CAST 到 VARCHAR 时按库默认排序规则代码页转换
    的字符损失风险；其余类型按 MSSQL 方言编译列元数据（FLOAT / DATETIME）。
    """
    dialect = mssql.dialect()
    types: dict[str, str] = {}
    for prop in sa_inspect(model).column_attrs:
        col_type = prop.columns[0].type
        # SQLModel 字符串列是 TypeDecorator(AutoString)，需下钻 impl 判定字符串族
        impl = getattr(col_type, "impl", col_type)
        if isinstance(impl, type):
            is_string = issubclass(impl, String)
        else:
            is_string = isinstance(impl, String)
        types[prop.key] = "NVARCHAR(max)" if is_string else col_type.compile(dialect)
    return types


def chunk_by_param_budget(
    rows: list[JsonObject],
    field_count: int,
) -> Iterator[list[JsonObject]]:
    """按参数预算分块：每块行数 = SAFE_PARAM_BUDGET // 字段数。"""
    max_rows = max(1, SAFE_PARAM_BUDGET // field_count)
    for i in range(0, len(rows), max_rows):
        yield rows[i : i + max_rows]


def dedup_rows(
    rows: list[JsonObject],
    keys: list[str],
) -> list[JsonObject]:
    """按 upsert 键去重，保留最后一条。

    防止多行 VALUES + MERGE 因 source 内部重复（M 行 source 匹配同一行
    target，MERGE 报 "attempted to UPDATE or DELETE the same row more
    than once"）导致整批失败成为毒批。业务上一键一条，正常不会重复；
    此为防御性去重。
    """
    seen: dict[tuple[object, ...], JsonObject] = {}
    for d in rows:
        key = tuple(d.get(k) for k in keys)
        seen[key] = d  # 后者覆盖前者
    return list(seen.values())


def build_multirow_merge(
    table_name: str,
    fields: list[str],
    keys: list[str],
    column_names: dict[str, str],
    cast_types: dict[str, str],
    rows: list[JsonObject],
) -> tuple[TextClause, JsonObject]:
    """构建多行 VALUES + MERGE 语句及参数（分块后的一块）。

    - 参数命名 r{行号}_{属性名}，每行每列唯一；字段名来自模型元数据，无注入面；
    - 列名经 column_names 映射（属性名 → DB 列名，保留原始列名的大小写与特殊字符）；
    - ON / UPDATE / INSERT 子句与生产验证过的单行 MERGE 模板逐字一致。
    """
    def _col(field: str) -> str:
        return column_names.get(field, field)

    values_rows: list[str] = []
    params: JsonObject = {}
    for i, row in enumerate(rows):
        casts: list[str] = []
        for field in fields:
            param = f"r{i}_{field}"
            params[param] = row.get(field)
            casts.append(f"CAST(:{param} AS {cast_types[field]})")
        values_rows.append(f"({', '.join(casts)})")

    source_rows = ",\n            ".join(values_rows)
    source_aliases = ", ".join(f"[{_col(f)}]" for f in fields)
    update_fields = [f for f in fields if f not in keys]
    set_clause = ", ".join(f"t.[{_col(f)}] = s.[{_col(f)}]" for f in update_fields)
    insert_col_list = ", ".join(f"[{_col(f)}]" for f in fields)
    insert_val_list = ", ".join(f"s.[{_col(f)}]" for f in fields)
    on_clause = " AND ".join(f"t.[{_col(k)}] = s.[{_col(k)}]" for k in keys)

    sql = text(
        f"""
        MERGE INTO {table_name} WITH (HOLDLOCK) AS t
        USING (VALUES
            {source_rows}
        ) AS s({source_aliases})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_col_list})
        VALUES ({insert_val_list});
        """
    )
    return sql, params
