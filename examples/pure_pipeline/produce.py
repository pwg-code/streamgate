"""接收端演示：IngestGateway 单条接收链路 + InMemoryAdmission 唯一性准入。

运行（先 docker compose up -d 启动 Kafka）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

o-1 发送两次：第二次得到 CONFLICT（in-memory 准入生效）。
注意 InMemoryAdmission 仅单进程有效（重启后内存清空；多实例部署
见 examples/redis_admission/ 的共享存储载体）。
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
    )


def build_binding() -> IngestBinding[OrderIn]:
    """声明接收什么消息、怎么判定唯一性（策略字段由使用方定义）。

    admission="in-memory"：核心内置零 I/O 唯一性准入（单进程）。
    其他选择：admission="none"（完全不设防）或注入 AdmissionPolicy 实例。
    """
    return IngestBinding(
        message_type="order",
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        admission="in-memory",
    )


async def main() -> None:
    gateway = IngestGateway(
        binding=build_binding(),
        kafka_config=kafka_config(),
        # 未注入 signal 时默认 ManualBackpressureSignal（不背压）；
        # 此处显式写出以便阅读。动态背压信号见 examples/http_probe/。
        backpressure_config=BackpressureConfig(enabled=False),
    )
    await gateway.start()
    try:
        # o-1 发送两次：验证进程内唯一性契约（第二次 CONFLICT，不产生重复消息）
        for order_id in ("o-1", "o-1", "o-2"):
            outcome = await gateway.process(
                OrderIn(order_id=order_id, amount=9.9),
                source="pure-pipeline-produce",
            )
            print(f"{order_id}: {outcome.kind.value}")
    finally:
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
