"""消费端演示：SqliteConsumer 预装配（批量 upsert + 单条探针 + DB 分类器）。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

依赖：pip install "streamgate[sql]"
"""

import asyncio
import os

from models import Order

from streamgate import ConsumerOptions
from streamgate.contrib.sqlite_upsert import SqliteConsumer, Upsert


async def main() -> None:
    consumer = SqliteConsumer(
        db=os.environ.get("DB_CONN", "sqlite+aiosqlite:///./data/streamgate.db"),
        upserts=[Upsert(model=Order, keys=["order_id"])],
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        group_id="sqlite-upsert",
        # 建表由出口载体启动期自动完成（WAL + create_all）
        options=ConsumerOptions(expected_type="order"),
    )
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
