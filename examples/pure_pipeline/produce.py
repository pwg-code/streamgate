"""数据入口演示：Producer 推入 + 进程内判重（一行开启）。

运行（先 docker compose up -d 启动 Kafka）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

o-1 发送两次：第二次得到 duplicate（进程内判重生效，guarantee="process-local"）。
注意内置判重载体仅单进程有效（重启后内存清空；多实例部署见
examples/redis_admission/ 的共享存储载体）。
"""

import asyncio
import os

from models import OrderIn

from streamgate import DedupOptions, Producer, ProducerOptions


def bootstrap_servers() -> str:
    """本地默认连 compose 暴露的 host 端口；容器网络内可覆盖为 kafka:9092。"""
    return os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092")


async def main() -> None:
    producer = Producer(
        bootstrap_servers(),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        options=ProducerOptions(
            message_type="order",
            # 一行开启进程内判重：身份键 = order_id，duplicate 时回给已有摘要
            dedup=DedupOptions(
                key=lambda r: r.order_id,
                summary=lambda r: {"amount": r.amount},
            ),
        ),
    )
    await producer.start()
    try:
        # o-1 发送两次：验证唯一性契约（第二次 duplicate，不产生重复消息）
        for order_id in ("o-1", "o-1", "o-2"):
            result = await producer.push(
                OrderIn(order_id=order_id, amount=9.9),
                source="pure-pipeline-produce",
            )
            print(f"{order_id}: {result.kind.value} (guarantee={result.guarantee})")
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
