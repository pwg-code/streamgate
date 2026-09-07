"""消费端演示：on_record 逃生口 + 标准库 sqlite3 落地（零第三方依赖）。

运行（另开终端，Kafka 已启动）：
    KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py
停止：Ctrl+C（优雅停机：flush 缓冲、leave group、关资源）。

验证：sqlite3 data/pipeline.db "select * from orders;"
"""

import asyncio
import os
import sqlite3
from pathlib import Path
from typing import Any

from streamgate import (
    ConsumerConfig,
    ConsumerWorker,
    ConsumeSpec,
    KafkaConfig,
)

DB_PATH = Path(os.environ.get("PIPELINE_DB_PATH", "data/pipeline.db"))


def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.environ.get("KAFKA__BOOTSTRAP_SERVERS", "localhost:29092"),
        topic=os.environ.get("KAFKA__TOPIC", "orders"),
    )


def _write_batch_sync(records: list[dict[str, Any]]) -> None:
    """同步写 sqlite（标准库）：幂等 upsert（重复消费安全）。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "order_id TEXT PRIMARY KEY, amount REAL NOT NULL)"
        )
        for r in records:
            con.execute(
                "INSERT INTO orders(order_id, amount) VALUES(?, ?) "
                "ON CONFLICT(order_id) DO UPDATE SET amount=excluded.amount",
                (r["order_id"], r["amount"]),
            )
        con.commit()
    finally:
        con.close()


async def handle_records(
    records: list[dict[str, Any]], context: object
) -> None:
    """on_record 钩子：正常返回=整批可提交；抛异常=重试→paused 自愈。

    阻塞 I/O 用 asyncio.to_thread 下放线程，不阻塞 event loop。
    真实场景这里换成你的存储写入（HTTP/ES/文件/任意数据库——框架不管载体）。
    """
    await asyncio.to_thread(_write_batch_sync, records)
    print(f"wrote {len(records)} records -> {DB_PATH}")


async def main() -> None:
    spec = ConsumeSpec(
        on_record=handle_records,
        expected_message_type="order",
        dlq=False,  # 演示不接 DLQ topic（on_record 模式无写侧，不参与二分隔离）
    )
    worker = ConsumerWorker(
        spec,
        kafka_config=kafka_config(),
        consumer_config=ConsumerConfig(group_id="pure-pipeline-sink"),
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
