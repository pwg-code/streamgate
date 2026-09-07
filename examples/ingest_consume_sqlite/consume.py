"""消费端演示：ConsumeSpec 声明式落库（sqlite upsert）→ consume_loop。

运行（另开终端，Kafka 已启动）：
    uv run python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。
"""

import asyncio
import os
from pathlib import Path

from models import Order
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel

from streamgate import (
    ConsumeSpec,
    ConsumerConfig,
    ConsumerWorker,
    DbConfig,
    KafkaConfig,
    Upsert,
)


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
    )


def db_config() -> DbConfig:
    """连接串必填（缺失即装配失败）；sqlite 需 extras：pip install "streamgate[sqlite]"。"""
    return DbConfig(
        connection_string=os.environ.get(
            "DB__CONNECTION_STRING", "sqlite+aiosqlite:///./data/streamgate.db"
        )
    )


async def init_db() -> None:
    """演示库建表（生产由迁移工具负责，框架不做 schema 管理）。"""
    Path("data").mkdir(exist_ok=True)
    engine = create_async_engine(db_config().connection_string)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    await engine.dispose()


async def main() -> None:
    await init_db()
    spec = ConsumeSpec(
        upserts=[Upsert(model=Order, keys=["order_id"])],
        expected_message_type="order",
    )
    worker = ConsumerWorker(
        spec,
        kafka_config=kafka_config(),
        consumer_config=ConsumerConfig(group_id="order-sink"),
        db_config=db_config(),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
