"""示例模型：ingress Schema（pydantic）与存储模型（SQLModel）。

ingress Schema 与存储模型归使用方（身份键/摘要/upsert 语义在此定义）；
框架核心对此零感知。
"""

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class OrderIn(BaseModel):
    """接收端 Schema：使用方完成校验后交给 Producer.push()。"""

    order_id: str
    amount: float


class Order(SQLModel, table=True):
    """存储模型：order_id 为幂等键（演示 sqlite ON CONFLICT upsert）。"""

    order_id: str = Field(primary_key=True)
    amount: float
