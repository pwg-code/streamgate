"""消费端演示：Consumer 自定义出口（标准库 sqlite3，零第三方依赖）。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

验证：sqlite3 data/pipeline.db "select * from orders;"
"""

import asyncio
import os
import sqlite3
from pathlib import Path

from streamgate import (
    ConsumeContext,
    Consumer,
    ConsumerOptions,
    DlqOptions,
    JsonObject,
)

DB_PATH = Path(os.environ.get("PIPELINE_DB_PATH", "data/pipeline.db"))


def _upsert_batch_sync(batch: list[JsonObject]) -> None:
    """同步 upsert sqlite（标准库）：幂等（重复消费安全）。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "order_id TEXT PRIMARY KEY, amount REAL NOT NULL)"
        )
        for r in batch:
            con.execute(
                "INSERT INTO orders(order_id, amount) VALUES(?, ?) "
                "ON CONFLICT(order_id) DO UPDATE SET amount=excluded.amount",
                (str(r["order_id"]), float(r["amount"])),  # type: ignore[arg-type]
            )
        con.commit()
    finally:
        con.close()


async def handle_orders(
    batch: list[JsonObject], context: ConsumeContext
) -> None:
    """出口契约 handler(batch, context)：正常返回 = 整批处理完成。

    数据到哪去完全是你的事——这里用标准库 sqlite3 演示；阻塞 I/O 用
    asyncio.to_thread 下放线程，不阻塞 event loop。抛异常 = 按分类处置
    （RETRY 退避重试 / POISON 隔离 / FATAL 停机）。
    """
    await asyncio.to_thread(_upsert_batch_sync, batch)
    print(f"handled {len(batch)} records -> {DB_PATH}")


async def main() -> None:
    consumer = Consumer(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
        group_id="pure-pipeline-outlet",
        handler=handle_orders,
        batch_size=500,
        flush_timeout=5.0,
        options=ConsumerOptions(
            expected_type="order",
            dlq=DlqOptions(enabled=False),  # 演示不接 DLQ topic
        ),
    )
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
