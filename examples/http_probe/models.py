"""示例模型：ingress Schema（pydantic，策略归使用方）。"""

from pydantic import BaseModel


class OrderIn(BaseModel):
    """接收端 Schema：使用方完成校验后交给 Producer.push()。"""

    order_id: str
    amount: float
