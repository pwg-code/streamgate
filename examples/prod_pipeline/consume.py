"""生产拓扑演示（消费侧）：UpsertWriter 落 MSSQL + DB 分类器 + 健康端点。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

本地冒烟默认落 sqlite（同一套 API，方言由连接串自动选择）；
生产传 MSSQL 连接串（aioodbc）：
    DB_CONN='mssql+aioodbc://user:pass@host:1433/streamgate?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
"""

import asyncio
import os
from pathlib import Path

from health_server import serve_consumer_health
from models import Order
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

from streamgate import (
    ConsumerConfig,
    ConsumerWorker,
    ConsumeSpec,
    KafkaConfig,
)
from streamgate.contrib.mssql_sink import (
    DbConfig,
    SQLAlchemyErrorClassifier,
    Upsert,
    UpsertWriter,
    async_session_factory,
    create_write_engine,
)


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        dlq_topic=os.environ.get("KAFKA__DLQ_TOPIC", "orders-dlq"),
    )


def db_config() -> DbConfig:
    """本地默认 sqlite（冒烟）；生产传 MSSQL 连接串，驱动差异由基座处理。"""
    return DbConfig(
        connection_string=os.environ.get(
            "DB_CONN", "sqlite+aiosqlite:///./data/streamgate.db"
        )
    )


async def init_db() -> None:
    """演示库建表（生产由迁移工具负责，框架不做 schema 管理）。"""
    Path("data").mkdir(exist_ok=True)
    engine = create_async_engine(db_config().require_connection_string())
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    await engine.dispose()


async def main() -> None:
    await init_db()
    cfg = db_config()
    engine = create_write_engine(cfg)
    # RecordWriter 注入点：ConsumeSpec.sink 是唯一落库路径——
    # 这里是 MSSQL 幂等 upsert（MERGE + HOLDLOCK，方言由连接串选择）。
    writer = UpsertWriter(
        cfg,
        engine,
        async_session_factory(engine),
        [Upsert(model=Order, keys=["order_id"])],
    )
    spec = ConsumeSpec(
        sink=writer,
        expected_message_type="order",
    )
    worker = ConsumerWorker(
        spec,
        kafka_config=kafka_config(),
        consumer_config=ConsumerConfig(group_id="prod-pipeline"),
        # 接 DB 必须注入 DB 分类器：默认分类器不认识 DB 专有异常
        error_classifier=SQLAlchemyErrorClassifier(),
    )
    # 健康端点并行托管（数据来自 worker.health_snapshot()），
    # produce 侧 HttpProbeSignal 轮询此端点做背压判定。
    health_task = asyncio.create_task(serve_consumer_health(worker, 9109))
    try:
        await worker.run()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
