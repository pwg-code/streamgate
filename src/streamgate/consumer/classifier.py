"""写库失败分类器：消费端写库异常的单点分类模块。

分类决定处置路径：
- INFRASTRUCTURE：连接/超时/死锁/池耗尽 → 保留既有 paused 无限自愈
- DATA：字符串截断/类型转换/约束违反 → 触发二分定位隔离
- UNKNOWN：其余 → 同 DATA（二分+探针对照会把结果安全归入上两类）

错误号事实来源：SQL Server 官方错误文档；常量表按生产实际持续扩充
（错误号缺漏导致的数据类误判为基础设施类会 paused 死循环，
由 backlog_age_warn / backpressure_tripped 告警兜底人工介入，不丢数据）。
使用方可通过 add_failure_rule 追加 (pattern → category) 规则，
追加规则先于内置规则评估（使用方覆盖语义）。
"""

import asyncio
import re
from collections.abc import Callable, Iterator
from enum import Enum

from sqlalchemy.exc import (
    DataError,
    IntegrityError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)


class FailureCategory(str, Enum):
    INFRASTRUCTURE = "infrastructure"
    DATA = "data"
    UNKNOWN = "unknown"


# 使用方追加规则（先于内置规则评估；返回 None 表示不适用，继续下一条）
ExtraRule = Callable[[Exception], "FailureCategory | None"]
_EXTRA_RULES: list[ExtraRule] = []


def add_failure_rule(rule: ExtraRule) -> None:
    """追加分类规则（使用方扩展点）。"""
    _EXTRA_RULES.append(rule)


# --- SQL Server 原生错误号 → 分类（常量表，持续扩充）---
# 数据类：重试必然无意义，写库被内容本身卡死
_MSSQL_DATA_ERROR_NUMBERS: frozenset[int] = frozenset({
    8152, 2628,          # 字符串或二进制数据将被截断（varchar(50) 超长）
    8114, 8169, 245,     # 类型转换失败（float/datetime 非法值）
    2627, 2714,          # 唯一约束/主键冲突、重复对象
    547, 233, 515,       # 外键/必填列/不允许 NULL 插入
})
# 基础设施类：DB 侧可自愈，重试有意义
_MSSQL_INFRA_ERROR_NUMBERS: frozenset[int] = frozenset({
    1205,                # 死锁 victim
    1204,                # 锁资源不足
    1222,                # 锁请求超时
    -2,                  # 查询超时（Timeout expired）
    4060, 18456,         # 登录失败/认证失败
    40197, 40613, 40501, 49918, 49919, 49920,  # 节流/资源暂不可用
})

# OperationalError 文本兜底关键字（小写化匹配；连接类错误的常见措辞）
_INFRA_KEYWORDS: tuple[str, ...] = (
    "connection", "timeout", "broken pipe", "reset by peer",
    "closed", "login failed", "pool",
)

# pyodbc 错误消息格式："('23000', '[23000] ... (2627) (SQLExecDirectW)')"
# 错误号在末尾圆括号中；SQLSTATE（'23000' / [23000]）带引号或方括号，不会被误匹配
_ERROR_NUMBER_RE = re.compile(r"\((-?\d{1,10})\)")


def _iter_exception_chain(exc: BaseException) -> Iterator[BaseException]:
    """遍历异常链：当前 → __cause__（显式）→ .orig（SQLAlchemy DBAPIError 包装的驱动原始异常）。

    刻意不走 __context__（隐式链）：except 块中可能夹带无关的历史异常，
    把它的错误号算进来会误分类。
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        cause = getattr(current, "__cause__", None)
        orig = getattr(current, "orig", None)
        nxt = cause if cause is not None else orig
        current = nxt if isinstance(nxt, BaseException) else None


def _extract_mssql_error_numbers(exc: BaseException) -> set[int]:
    """从异常链各节点字符串中提取 SQL Server 原生错误号（可能多个）。"""
    numbers: set[int] = set()
    for node in _iter_exception_chain(exc):
        for match in _ERROR_NUMBER_RE.finditer(str(node)):
            numbers.add(int(match.group(1)))
    return numbers


def classify_write_failure(exc: Exception) -> FailureCategory:
    """写库失败分类（单点）。优先级见 docstring；分类错误的后果是安全方向的：
    误判 infra→data 会走二分，探针失败自然回落 paused；误判 data→infra 会
    paused 死循环，由积压告警兜底。
    """
    # 0. 使用方追加规则（最具体，优先评估）
    for rule in _EXTRA_RULES:
        result = rule(exc)
        if result is not None:
            return result

    # 1. 事务/池超时：writer.write 的 wait_for 兜底会再抛 asyncio.TimeoutError
    if isinstance(exc, asyncio.TimeoutError):
        return FailureCategory.INFRASTRUCTURE
    # SQLAlchemy 连接池耗尽（sqlalchemy.exc.TimeoutError，与 asyncio 的同名不同类）
    if isinstance(exc, SQLAlchemyTimeoutError):
        return FailureCategory.INFRASTRUCTURE

    # 2. 消费端自身的内容防御：键缺失 / naive 时间戳抛 ValueError，
    #    本质是"消息内容入不了库"（旧消息残留场景），属数据类
    if isinstance(exc, ValueError):
        return FailureCategory.DATA

    # 3. SQL Server 错误号表（数据表优先判：更具体）
    numbers = _extract_mssql_error_numbers(exc)
    if numbers & _MSSQL_DATA_ERROR_NUMBERS:
        return FailureCategory.DATA
    if numbers & _MSSQL_INFRA_ERROR_NUMBERS:
        return FailureCategory.INFRASTRUCTURE

    # 4. SQLAlchemy 异常族兜底（SQLite 开发路径没有 MSSQL 错误号，走这里）
    if isinstance(exc, (IntegrityError, DataError)):
        return FailureCategory.DATA
    if isinstance(exc, OperationalError):
        text = str(exc).lower()
        if any(keyword in text for keyword in _INFRA_KEYWORDS):
            return FailureCategory.INFRASTRUCTURE
        return FailureCategory.UNKNOWN
    return FailureCategory.UNKNOWN
