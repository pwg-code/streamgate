"""数据入口演示：最小 Producer（纯推入，零判重概念；判重另见 redis_admission/）。

运行（先 docker compose up -d 启动 Kafka）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
"""

import asyncio
import os

from models import OrderIn

from streamgate import Producer, ProducerOptions


async def main() -> None:
    producer = Producer(
        os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        # 传 key 则同 order_id 保序（Kafka 按 key 哈希进同分区）；不传轮询不保序
        key=lambda r: r.order_id,
        options=ProducerOptions(message_type="order"),
    )
    await producer.start()
    try:
        for order_id in ("o-1", "o-2"):
            result = await producer.push(
                OrderIn(order_id=order_id, amount=19.9),
                source="sqlite-upsert-produce",
            )
            print(f"{order_id}: {result.kind.value}")
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
