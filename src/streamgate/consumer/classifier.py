"""默认消费端异常分类器（ErrorClassifier 协议的框架内置实现）。

只认通用异常（asyncio 超时 / aiokafka KafkaError 族 / ValueError 兜底），
不认识任何 DB/中间件专有类型——机制归框架，策略归使用方。

默认兜底是 POISON（文档显著声明，勿静默改动）：
- 误把瞬态故障归为 POISON 的后果是安全方向的：定位隔离自带对照验证，
  对照探针失败即中止隔离转 paused，不会丢数据；
- 反向（把毒消息归为 RETRY）会陷入 paused→重试→paused 死循环，
  只能靠积压告警人工介入，代价更高。

接 DB 的使用方必须注入对应分类器（DB 专有异常 → RETRY/POISON 的精确映射），
生产级参考实现见 streamgate.contrib.sql_upsert.SQLAlchemyErrorClassifier。
"""

import asyncio

from aiokafka.errors import KafkaError

from streamgate.protocols import ErrorKind


class DefaultErrorClassifier:
    """通用异常分类：

    - asyncio.TimeoutError → RETRY（出口侧 wait_for 兜底超时等瞬态）
    - aiokafka KafkaError 族 → RETRY（broker 侧瞬态）
    - ValueError → POISON（消费端自身的内容防御：键缺失/naive 时间戳等，
      本质是"消息内容处理不了"）
    - 其余未知异常 → POISON（对照验证兜底，见模块 docstring）
    """

    def classify(self, exc: Exception, attempt: int) -> ErrorKind:
        if isinstance(exc, asyncio.TimeoutError):
            return ErrorKind.RETRY
        if isinstance(exc, KafkaError):
            return ErrorKind.RETRY
        if isinstance(exc, ValueError):
            return ErrorKind.POISON
        return ErrorKind.POISON


__all__ = ["DefaultErrorClassifier"]
