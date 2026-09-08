"""生产拓扑演示：Redis 唯一性准入 + HTTP 探活背压（接收侧）。

三个 contrib 组件一次接全（多实例安全查重 / 动态背压 / MSSQL 落库）：
    pip install "streamgate[redis,sql,http]"

运行（先 docker compose up -d 启动 kafka + redis，再启动 consume.py）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python produce.py
重复运行：第二次 o-1 得 CONFLICT（Redis 占位跨进程存活）。
consume.py 停掉后再运行：探活不可达 → fail-closed 背压拒绝。
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
from streamgate.contrib.http_probe import HttpProbeSignal
from streamgate.contrib.redis_admission import (
    RedisConfig,
    RedisExistenceAdmission,
    RedisExistenceCache,
)


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
    )


def build_admission() -> RedisExistenceAdmission[OrderIn]:
    """共享存储唯一性准入：多实例部署安全。

    需要冷实体回源（Redis 数据丢失后向权威库核实而非拒绝）时传
    backfill=SqlBackfill(...)（streamgate.contrib.sql_sink）。
    """
    redis_cfg = RedisConfig(url=os.environ.get("REDIS__URL", "redis://localhost:6379/0"))
    return RedisExistenceAdmission(
        cache=RedisExistenceCache(redis_cfg),
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        backfill=None,
        redis_config=redis_cfg,
    )


def backpressure_config() -> BackpressureConfig:
    """探活 consumer 健康端点（consume.py 托管在 9109）。

    demo 用短周期便于观察；生产默认 trip/recover 为小时级磁滞带。
    """
    return BackpressureConfig(
        enabled=True,
        consumer_health_url=os.environ.get(
            "BACKPRESSURE__CONSUMER_HEALTH_URL", "http://localhost:9109/health"
        ),
        check_interval_seconds=5.0,
        unhealthy_check_interval_seconds=2.0,
        timeout_seconds=2.0,
        probe_retries=1,
        probe_retry_interval_seconds=2.0,
    )


def build_binding() -> IngestBinding[OrderIn]:
    return IngestBinding(
        message_type="order",
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
        admission=build_admission(),
    )


async def main() -> None:
    gateway = IngestGateway(
        binding=build_binding(),
        kafka_config=kafka_config(),
        backpressure_config=backpressure_config(),
        signal=HttpProbeSignal(backpressure_config()),  # 动态背压注入点
    )
    await gateway.start()
    try:
        for order_id in ("o-1", "o-2"):
            outcome = await gateway.process(
                OrderIn(order_id=order_id, amount=39.9),
                source="prod-pipeline-produce",
            )
            print(f"{order_id}: {outcome.kind.value}")
    finally:
        await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
