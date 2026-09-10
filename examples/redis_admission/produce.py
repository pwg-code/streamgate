"""接收端演示：注入 RedisExistenceAdmission（共享存储唯一性准入）。

运行（先 docker compose up -d 启动 kafka + redis）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

依赖：pip install "streamgate[redis]"（策略实现已升级为
streamgate.contrib.redis_admission 正式功能）。
重复运行：第二次 o-1 得 CONFLICT（Redis 占位跨进程存活，与 pure_pipeline 的
in-memory 版本不同：多实例部署安全）。
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
from streamgate.contrib.redis_admission import (
    RedisConfig,
    RedisExistenceAdmission,
    RedisExistenceCache,
)


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
    )


def build_binding() -> IngestBinding[OrderIn]:
    return IngestBinding(
        message_type="order",
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        admission=build_admission(),  # 协议实例注入（字符串捷径只有 none/in-memory）
    )


def build_admission() -> RedisExistenceAdmission[OrderIn]:
    """存在性判定 + 原子占位：注入共享存储载体。

    需要冷实体回源时传 backfill=SqlBackfill(...)（streamgate.contrib.sql_upsert）。
    """
    redis_config = RedisConfig(
        url=os.environ.get("REDIS__URL", "redis://localhost:6379/0")
    )
    return RedisExistenceAdmission(
        cache=RedisExistenceCache(redis_config),
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        backfill=None,
        redis_config=redis_config,
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
                OrderIn(order_id=order_id, amount=29.9),
                source="redis-admission-produce",
            )
            print(f"{order_id}: {outcome.kind.value}")
    finally:
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
