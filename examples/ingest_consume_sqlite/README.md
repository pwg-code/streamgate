# ingest → consume → sqlite 最小示例

单机跑通 streamgate 完整链路：`IngestGateway` 接收 → Kafka 投递 →
`ConsumerWorker` 消费 → sqlite 幂等落库（内置 Upsert sugar）。

## 前置依赖

- Python 3.10+、[uv](https://docs.astral.sh/uv/)（或 pip）
- Docker（Kafka 经 compose 提供；redis 非本示例必需，仅为后续换
  `redis-existence` 准入预留）

## 安装

```bash
pip install "streamgate[sqlite]"     # sqlite 落库需要 aiosqlite 驱动
```

## 步骤

```bash
# 1. 启动依赖（Kafka KRaft 单节点 + redis）
docker compose up -d        # 在 examples/ 目录执行

# 2. 启动消费端（终端 A）
cd examples/ingest_consume_sqlite
uv run python consume.py    # 或 python consume.py

# 3. 发送两条消息（终端 B）
uv run python produce.py

# 4. 验证落库
sqlite3 data/streamgate.db "SELECT * FROM order;"
# o-1|9.9
# o-2|9.9
```

## 说明

- **地址覆盖**：脚本默认连 `localhost:29092`（compose 已将该端口映射到宿主）。
  在容器网络内运行时指向服务名：
  `KAFKA__BOOTSTRAP_SERVERS=kafka:9092`；topic / 连接串分别可用
  `KAFKA__TOPIC`、`DB__CONNECTION_STRING` 覆盖。
- **admission="none"**：演示路径免依赖（零 DB / 零 redis）。换唯一性准入：
  在 `produce.py` 中改 `admission="redis-existence"` 并传入
  `redis_config=RedisConfig(url=...)`（需 `pip install "streamgate[redis]"`）。
- **背压信号**：演示注入核心内置 `AllowAllSignal`（免依赖）。生产可安装
  `streamgate[http-probe]` 使用默认 `HttpProbeSignal`，或自行实现
  `BackpressureSignal`。
- **连接串必填**：`DB__CONNECTION_STRING` 缺失时消费端装配即报错（含修复
  指引），与 `KAFKA__TOPIC` / `CONSUMER__GROUP_ID` 的启动失败语义一致。
