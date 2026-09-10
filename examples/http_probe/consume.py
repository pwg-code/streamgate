"""消费端演示：Consumer + 健康端点（被 produce 侧的 HttpProbeSignal 探活）。

运行（另开终端，Kafka 已启动；handler 直接打印，出口逻辑自便）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C。

依赖：仅 streamgate（健康端点用 stdlib asyncio；httpx 只在 produce 侧需要）。
"""

import asyncio
import os

from health_server import serve_consumer_health

from streamgate import ConsumeContext, Consumer, JsonObject


async def print_orders(
    batch: list[JsonObject], context: ConsumeContext
) -> None:
    """出口 handler：演示直接打印（换成你的实时分析/转发/告警/存储）。"""
    print(f"handled {len(batch)} records: {[r.get('order_id') for r in batch]}")


async def main() -> None:
    consumer = Consumer(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        group_id="http-probe-demo",
        handler=print_orders,
        batch_size=1,  # 单条实时
    )
    # 健康端点并行托管（数据来自 consumer.health_snapshot()），
    # ingest 侧 HttpProbeSignal 轮询此端点做背压判定。
    health_task = asyncio.create_task(serve_consumer_health(consumer, 9109))
    try:
        await consumer.run()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
