"""消费端演示：RecordWriter 注入（UpsertWriter 落 sqlite）+ DB 异常分类器。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

依赖（不随 streamgate 安装）：pip install sqlalchemy sqlmodel aiosqlite
"""

import asyncio
import os
from pathlib import Path

from classifier import SQLAlchemyErrorClassifier
from config import DbConfig
from engines import async_session_factory, create_write_engine
from models import Order
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel
from upsert import Upsert, UpsertWriter

from streamgate import (
    ConsumerConfig,
    ConsumerWorker,
    ConsumeSpec,
    KafkaConfig,
)


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        dlq_topic=os.environ.get("KAFKA__DLQ_TOPIC", "orders-dlq"),
    )


def db_config() -> DbConfig:
    """连接串必填（缺失即装配失败）；驱动按连接串选择：sqlite 装本示例的
    额外依赖 aiosqlite；mssql 用 aioodbc。"""
    return DbConfig(
        connection_string=os.environ.get(
            "DB_CONN", "sqlite+aiosqlite:///./data/streamgate.db"
        )
    )


async def init_db() -> None:
    """演示库建表（生产由迁移工具负责，框架不做 schema 管理）。"""
    Path("data").mkdir(exist_ok=True)
    cfg = db_config()
    engine = create_async_engine(cfg.require_connection_string())
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    await engine.dispose()


async def main() -> None:
    await init_db()
    cfg = db_config()
    engine = create_write_engine(cfg)
    # RecordWriter 注入点：ConsumeSpec.sink 是唯一落库路径——
    # 写库/写 ES/转发/告警都是同一个口，这里是 SQL upsert 参考。
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
        consumer_config=ConsumerConfig(group_id="sqlite-sink"),
        # 接 DB 必须注入 DB 分类器：默认分类器不认识 DB 专有异常
        error_classifier=SQLAlchemyErrorClassifier(),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
