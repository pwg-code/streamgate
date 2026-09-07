"""消费端演示：ConsumerWorker + 健康端点（被 produce 侧的 HttpProbeSignal 探活）。

运行（另开终端，Kafka 已启动；on_record 直接打印，落地逻辑自便）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C。

依赖：仅 streamgate（健康端点用 stdlib asyncio；httpx 只在 produce 侧需要）。
"""

import asyncio
import os

from health_server import serve_consumer_health

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


async def print_records(records: list[dict], context: object) -> None:
    """on_record：演示直接打印（换成你的存储写入）。"""
    print(f"consumed {len(records)} records: {[r.get('order_id') for r in records]}")


async def main() -> None:
    spec = ConsumeSpec(on_record=print_records, expected_message_type="order")
    worker = ConsumerWorker(
        spec,
        kafka_config=kafka_config(),
        consumer_config=ConsumerConfig(group_id="http-probe-demo"),
    )
    # 健康端点并行托管（数据来自 worker.health_snapshot()），
    # ingest 侧 HttpProbeSignal 轮询此端点做背压判定。
    health_task = asyncio.create_task(serve_consumer_health(worker, 9109))
    try:
        await worker.run()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
