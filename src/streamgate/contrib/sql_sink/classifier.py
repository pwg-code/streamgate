"""SQLAlchemyErrorClassifier：DB 专属异常分类（ErrorClassifier 实现）。

接 DB 的使用方应注入对应分类器：默认分类器不认识 DB 专有异常
（会被兜底为 POISON，误进 DLQ——探针对照会兜住，但浪费一轮二分）。
按所用数据库裁剪错误号表。

分类决定处置路径：
- RETRY：连接/超时/死锁/池耗尽 → 保留既有 paused 无限自愈
- POISON：字符串截断/类型转换/约束违反 → 触发二分定位隔离
- FATAL：本实现不产生（保守策略：DB 场景几乎都有自愈路径；
  需要时子类化覆盖 classify，如"重试 N 次后升级 FATAL"）

错误号事实来源：SQL Server 官方错误文档；常量表按生产实际持续扩充
（错误号缺漏导致的数据类误判为基础设施类会 paused 死循环，
由积压告警兜底人工介入，不丢数据）。
"""

import asyncio
import re
from collections.abc import Iterator

from aiokafka.errors import KafkaError
from sqlalchemy.exc import (
    DataError,
    IntegrityError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)

from streamgate import ErrorKind

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


class SQLAlchemyErrorClassifier:
    """SQLAlchemy 写侧异常分类。误判方向是安全方向的：
    误判 infra→poison 走二分，探针失败自然回落 paused；
    误判 poison→infra 会 paused 死循环，由积压告警兜底。
    """

    def classify(self, exc: Exception, attempt: int) -> ErrorKind:
        # 1. 事务/池超时：writer.write 的 wait_for 兜底会再抛 asyncio.TimeoutError
        if isinstance(exc, asyncio.TimeoutError):
            return ErrorKind.RETRY
        # SQLAlchemy 连接池耗尽（sqlalchemy.exc.TimeoutError，与 asyncio 的同名不同类）
        if isinstance(exc, SQLAlchemyTimeoutError):
            return ErrorKind.RETRY
        # Kafka 侧瞬态（理论不出现在写侧，防御性归 RETRY）
        if isinstance(exc, KafkaError):
            return ErrorKind.RETRY

        # 2. 消费端自身的内容防御：键缺失 / naive 时间戳抛 ValueError，
        #    本质是"消息内容入不了库"（旧消息残留场景），属数据类
        if isinstance(exc, ValueError):
            return ErrorKind.POISON

        # 3. SQL Server 错误号表（数据表优先判：更具体）
        numbers = _extract_mssql_error_numbers(exc)
        if numbers & _MSSQL_DATA_ERROR_NUMBERS:
            return ErrorKind.POISON
        if numbers & _MSSQL_INFRA_ERROR_NUMBERS:
            return ErrorKind.RETRY

        # 4. SQLAlchemy 异常族兜底（SQLite 开发路径没有 MSSQL 错误号，走这里）
        if isinstance(exc, (IntegrityError, DataError)):
            return ErrorKind.POISON
        if isinstance(exc, OperationalError):
            text = str(exc).lower()
            if any(keyword in text for keyword in _INFRA_KEYWORDS):
                return ErrorKind.RETRY
        return ErrorKind.POISON  # 未知异常：POISON（探针对照兜底，见模块 docstring）


# ---- SQL Server 错误号提取 ----

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


__all__ = ["SQLAlchemyErrorClassifier"]
