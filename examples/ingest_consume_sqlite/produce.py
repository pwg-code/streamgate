"""接收端演示：IngestGateway 单条接收链路（背压关闭 + admission="none"）。

运行（先 docker compose up -d 启动 Kafka）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 uv run python produce.py
"""

import asyncio
import os

from models import OrderIn

from streamgate import (
    AllowAllSignal,
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
    )


def build_binding() -> IngestBinding[OrderIn]:
    """声明接收什么消息、怎么判定唯一性（策略字段由使用方定义）。

    admission="none"：免依赖起步（零 DB / 零 redis）。
    换唯一性准入：admission="redis-existence"（需 pip install "streamgate[redis]"，
    并给 IngestGateway 传 redis_config=RedisConfig(...)）。
    """
    return IngestBinding(
        message_type="order",
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        admission="none",
    )


async def main() -> None:
    # 背压默认信号 HttpProbeSignal 需要可选依赖 httpx（streamgate[http-probe]）；
    # 演示注入核心内置的 AllowAllSignal（不探活、永远放行），保持最小依赖。
    gateway = IngestGateway(
        binding=build_binding(),
        kafka_config=kafka_config(),
        backpressure_config=BackpressureConfig(enabled=False),
        signal=AllowAllSignal(),
    )
    await gateway.start()
    try:
        for order_id in ("o-1", "o-2"):
            outcome = await gateway.process(
                OrderIn(order_id=order_id, amount=9.9),
                source="example-produce",
            )
            print(f"{order_id}: {outcome.kind.name}")
    finally:
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
