"""示例模型：ingress Schema（pydantic）与落库模型（SQLModel）。

ingress Schema 归使用方（策略归使用方：entity/slot/summary 语义在此定义）；
落库模型与 Upsert 声明同为使用方策略（框架核心对此零感知）。
"""

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class OrderIn(BaseModel):
    """接收侧 Schema：使用方完成校验后交给 IngestGateway。"""

    order_id: str
    amount: float


class Order(SQLModel, table=True):
    """落库模型：order_id 为幂等键（演示 sqlite ON CONFLICT upsert）。"""

    order_id: str = Field(primary_key=True)
    amount: float
