"""数据入口演示：注入 RedisDedupCarrier（共享存储分布式判重）。

运行（先 docker compose up -d 启动 kafka + redis）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

依赖：pip install "streamgate[redis]"（判重载体实现见
streamgate.contrib.redis_dedup 正式功能）。
重复运行：第二次 o-1 得 duplicate（Redis 占位跨进程存活，与 pure_pipeline 的
进程内版本不同：多实例部署安全，guarantee="distributed"）。
"""

import asyncio
import os

from models import OrderIn

from streamgate import DedupOptions, Producer, ProducerOptions
from streamgate.contrib.redis_dedup import (
    RedisConfig,
    RedisDedupCache,
    RedisDedupCarrier,
)


def build_carrier() -> RedisDedupCarrier[OrderIn]:
    """存在性判定 + 原子占位：注入共享存储载体（guarantee="distributed"）。

    需要冷身份回源时传 backfill=SqlBackfill(...)（streamgate.contrib.sql_upsert）。
    """
    redis_config = RedisConfig(
        url=os.environ.get("REDIS__URL", "redis://localhost:6379/0")
    )
    return RedisDedupCarrier(
        cache=RedisDedupCache(redis_config),
        key=lambda r: r.order_id,
        summary=lambda r: {"amount": r.amount},
        backfill=None,
        redis_config=redis_config,
    )


async def main() -> None:
    carrier = build_carrier()
    producer = Producer(
        os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        options=ProducerOptions(
            message_type="order",
            dedup=DedupOptions(
                key=lambda r: r.order_id,  # 身份键声明（载体按同一键判定）
                carrier=carrier,
            ),
        ),
    )
    await producer.start()
    try:
        for order_id in ("o-1", "o-2"):
            result = await producer.push(
                OrderIn(order_id=order_id, amount=29.9),
                source="redis-admission-produce",
            )
            print(f"{order_id}: {result.kind.value} (guarantee={result.guarantee})")
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
