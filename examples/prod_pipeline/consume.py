"""生产拓扑演示（消费侧）：MssqlConsumer 预装配 + 健康端点。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

本地冒烟默认落 sqlite（同一套 API，方言由连接串自动选择）；
生产传 MSSQL 连接串（aioodbc，建表交迁移工具）：
    DB_CONN='mssql+aioodbc://user:pass@host:1433/streamgate?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
"""

import asyncio
import os
from pathlib import Path

from health_server import serve_consumer_health
from models import Order
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

from streamgate import ConsumerOptions
from streamgate.contrib.mssql_upsert import MssqlConsumer, Upsert


def db_conn() -> str:
    """本地默认 sqlite（冒烟）；生产传 MSSQL 连接串，驱动差异由基座处理。"""
    return os.environ.get("DB_CONN", "sqlite+aiosqlite:///./data/streamgate.db")


async def init_db() -> None:
    """演示库建表（生产由迁移工具负责，框架不做 schema 管理）。"""
    Path("data").mkdir(exist_ok=True)
    engine = create_async_engine(db_conn())
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    await engine.dispose()


async def main() -> None:
    await init_db()
    consumer = MssqlConsumer(
        db=db_conn(),
        upserts=[Upsert(model=Order, keys=["order_id"])],
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        group_id="prod-pipeline",
        options=ConsumerOptions(expected_type="order"),
    )
    # 健康端点并行托管（数据来自 consumer.health_snapshot()），
    # produce 侧 HttpProbeSignal 轮询此端点做背压判定。
    health_task = asyncio.create_task(serve_consumer_health(consumer, 9109))
    try:
        await consumer.run()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
