"""示例模型：ingress Schema（pydantic）与存储模型（SQLModel）。

ingress Schema 归使用方（策略归使用方：entity/slot/summary 语义在此定义）；
存储模型与 Upsert 声明同为使用方策略（框架核心对此零感知）。
"""

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class OrderIn(BaseModel):
    """接收侧 Schema：使用方完成校验后交给 IngestGateway。"""

    order_id: str
    amount: float


class Order(SQLModel, table=True):
    """存储模型：order_id 为幂等键（MSSQL MERGE / sqlite ON CONFLICT upsert）。

    表名显式取 orders：MSSQL 方言的 MERGE 语句模板不加方括号，
    保留字表名（如默认类名 order）会直接语法错误。
    """

    # SQLModel 桩把 __tablename__ 声明为 declared_attr，字符串赋值需定点抑制
    __tablename__ = "orders"  # type: ignore[assignment]

    order_id: str = Field(primary_key=True)
    amount: float
