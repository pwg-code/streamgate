"""示例模型：ingress Schema（pydantic，策略归使用方）。

pure_pipeline 只依赖 streamgate 本体 + pydantic（streamgate 的传递依赖）：
零 DB 驱动、零 redis、零 httpx。
"""

from pydantic import BaseModel


class OrderIn(BaseModel):
    """接收端 Schema：使用方完成校验后交给 Producer.push()。"""

    order_id: str
    amount: float
