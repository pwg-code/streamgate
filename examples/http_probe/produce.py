"""数据入口演示：注入 HttpProbeSignal —— 消费端积压超阈值时自动拒绝推入。

运行（先启动 consume.py，再运行本脚本）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

依赖：pip install "streamgate[http]"（信号实现见
streamgate.contrib.http_probe 正式功能）。
观察：把 consume.py 停掉（探活不可达 → fail-closed 拒绝），或人为调低
BACKPRESSURE__TRIP_SECONDS 制造积压拒绝；恢复 consume.py 后磁滞放行。
"""

import asyncio
import os

from models import OrderIn

from streamgate import BackpressureConfig, Producer, ProducerOptions
from streamgate.contrib.http_probe import HttpProbeSignal


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


async def main() -> None:
    producer = Producer(
        os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        options=ProducerOptions(
            message_type="order",
            backpressure=backpressure_config(),
            signal=HttpProbeSignal(backpressure_config()),  # 背压信号注入点
        ),
    )
    await producer.start()
    try:
        for i in range(1, 6):
            result = await producer.push(
                OrderIn(order_id=f"o-{i}", amount=float(i)),
                source="http-probe-produce",
            )
            print(f"o-{i}: {result.kind.value}")
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
