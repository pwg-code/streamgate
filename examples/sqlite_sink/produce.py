"""接收端演示：IngestGateway + admission="none"（唯一性准入另见 redis_admission/）。

运行（先 docker compose up -d 启动 Kafka）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
"""

import asyncio
import os

from models import OrderIn

from streamgate import (
    BackpressureConfig,
    IngestBinding,
    IngestGateway,
    KafkaConfig,
)


def kafka_config() -> KafkaConfig:
    """本地默认连 compose 暴露的 host 端口；容器网络内可覆盖为 kafka:9092。"""
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        dlq_topic=os.environ.get("KAFKA__DLQ_TOPIC", "orders-dlq"),
    )


def build_binding() -> IngestBinding[OrderIn]:
    return IngestBinding(
        message_type="order",
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        admission="none",
    )


async def main() -> None:
    gateway = IngestGateway(
        binding=build_binding(),
        kafka_config=kafka_config(),
        backpressure_config=BackpressureConfig(enabled=False),
    )
    await gateway.start()
    try:
        for order_id in ("o-1", "o-2"):
            outcome = await gateway.process(
                OrderIn(order_id=order_id, amount=19.9),
                source="sqlite-sink-produce",
            )
            print(f"{order_id}: {outcome.kind.value}")
    finally:
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
