# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

[English](README.md) | 中文

**streamgate 是一条"带闸门的数据流水线"**：数据从门口进来（接收），穿过 Kafka 管道（投递），最后落到你想放的任何地方（存储）。

```
你的程序 ──► IngestGateway（进） ──► Kafka（管道） ──► ConsumerWorker（出） ──► 你的存储
              查重、限流、背压                        批量缓冲、重试、坏数据隔离
```

框架只装 3 个依赖（`aiokafka` / `loguru` / `pydantic`），**包里没有一行数据库、Redis、HTTP 客户端代码**——所有"数据落到哪、用什么存"的实现都以可复制的示例放在 [`examples/`](examples/)。

---

## 为什么选它

- **机制归框架，策略归你。** 查重流程、重试、限流、断线重连、优雅停机这些"脏活累活"框架全包；你只管业务语义（主键是什么、摘要放什么）和存储选型。
- **查重不绑定存储。** 框架提供查重的"流程编排"（存在性判定 → 原子占位 → 冷回源 → 消费后刷新），至于用 Redis、进程内字典还是别的什么来存，由你注入。内置零依赖实现开箱即用，生产级 Redis 参考实现在 examples 里抄。
- **一个写入钩子，接任何存储。** MySQL、ES、另一个服务、一个文件……实现 `RecordWriter` 接口注入即可。批量缓冲、失败重试、坏数据二分隔离这些框架替你兜底。
- **错误分类也是钩子。** "连接超时该重试"还是"数据脏了该隔离"，只有懂你存储的人知道。注入一个分类器即可；不注入也有安全的默认行为（不会丢数据）。
- **背压带磁滞防抖。** 消费端积压超标自动拒绝接收，回落后自动放行，来回抖动有磁滞带压着。
- **坏数据不堵管道。** 一条毒消息不会卡死整个分区：二分定位、隔离进死信 topic，好数据继续走。

---

## 快速入门（4 步）

### 第 1 步：安装

```bash
pip install streamgate
```

### 第 2 步：启动 Kafka

```bash
cd examples && docker compose up -d kafka
```

### 第 3 步：写"生产端"（数据入口）

```python
import asyncio
from pydantic import BaseModel
from streamgate import IngestBinding, IngestGateway, KafkaConfig, BackpressureConfig

class OrderIn(BaseModel):          # 你的数据格式，你来定义
    order_id: str
    amount: float

binding = IngestBinding(
    message_type="order",
    entity_key=lambda r: r.order_id,          # 业务主键
    slot_key=lambda r: "order",               # 槽位（同一实体下的类别）
    summary=lambda r: {"amount": r.amount},   # 冲突时返回给对方的摘要
    admission="in-memory",                    # 进程内查重：重复发同一条会得到 conflict
)

async def main() -> None:
    gateway = IngestGateway(
        binding=binding,
        kafka_config=KafkaConfig(bootstrap_servers="localhost:29092", topic="orders"),
        backpressure_config=BackpressureConfig(enabled=False),
    )
    await gateway.start()
    try:
        outcome = await gateway.process(OrderIn(order_id="o-1", amount=9.9))
        print(outcome.kind)   # accepted / conflict / backpressure
    finally:
        await gateway.close()

asyncio.run(main())
```

### 第 4 步：写"消费端"（数据出口）

```python
import asyncio
from streamgate import ConsumeSpec, ConsumerWorker, ConsumerConfig, KafkaConfig

async def handle(records, context):
    # records 是一批 dict —— 写数据库、写文件、发 HTTP，随你
    print(f"收到 {len(records)} 条: {records}")

async def main() -> None:
    spec = ConsumeSpec(on_record=handle, expected_message_type="order")
    worker = ConsumerWorker(
        spec,
        kafka_config=KafkaConfig(bootstrap_servers="localhost:29092", topic="orders"),
        consumer_config=ConsumerConfig(group_id="my-group"),
    )
    await worker.run()   # 阻塞运行；Ctrl+C 优雅停机（自动清空缓冲、提交位点）

asyncio.run(main())
```

跑通即为完整链路。可直接运行的完整版在 [`examples/pure_pipeline/`](examples/pure_pipeline/)（裸装即可跑，SQLite 用 Python 标准库写）。

---

## 核心概念，记住三句话

1. **门口（IngestGateway）管"收不收"**：查重、限流都是它的活；
2. **出口（ConsumerWorker）管"怎么落地"**：批量、重试、死信隔离都是它的活；
3. **两个"口"之间的一切存储载体都是你的**：实现协议 → 注入 → 完事。

三个使用层次：

| 层次 | 你用什么 | 适用场景 |
|------|----------|----------|
| **0 — 声明式** | `IngestBinding` + `ConsumeSpec`（`sink` 或 `on_record`） | 绝大多数场景：声明消息类型和查重键，注入你的写入器 |
| **1 — 换组件** | `streamgate.protocols` 里的协议 + 内置零依赖实现 | 替换准入、背压信号、编解码、错误分类器 |
| **2 — 逃生口** | `ConsumeSpec.on_record` / `ConsumeContext` | 自己处理整批记录，完全不碰 sink 机制 |

协议集中在 `streamgate.protocols`（冻结契约，只增不改）。

---

## 如何拓展

所有拓展都是同一个套路：**实现一个协议类，然后注入**，不需要继承任何框架基类。

### 5 个钩子，按需替换

| 钩子协议 | 管什么 | 什么时候需要自己写 |
|---|---|---|
| **RecordWriter** | 数据落到哪 | 最常用！写 MySQL/ES/调接口……都实现它 |
| **AdmissionPolicy** | 门口怎么查重 | 需要多实例共享去重时（如用 Redis） |
| **ErrorClassifier** | 写失败了算什么错 | 接了数据库，必须教它认数据库的错 |
| **BackpressureSignal** | 什么时候拒绝收货 | 需要根据消费端积压动态限流时 |
| **BackfillSource** | 缓存丢了怎么回源 | 查重缓存需要从数据库冷启动时 |

### 例 1：自定义落库（最常见）

```python
from streamgate.protocols import RecordWriter, WriteResult

class MyWriter:                      # 不用继承，duck typing
    async def start(self): ...       # 建连接（框架启动时调一次）
    async def write(self, batch):    # 幂等写一批（重复调安全）
        ...                          # 你的写入逻辑
        return WriteResult(counts={"my_table": len(batch)})
    async def close(self): ...

spec = ConsumeSpec(sink=MyWriter())  # 注入即可
```

### 例 2：教框架认数据库的错（接 DB 必做）

```python
from streamgate import ErrorKind

class MyClassifier:
    def classify(self, exc, attempt) -> ErrorKind:
        if "timeout" in str(exc).lower():
            return ErrorKind.RETRY    # 瞬态错误 → 退避重试
        return ErrorKind.POISON       # 内容问题 → 二分隔离进死信队列

worker = ConsumerWorker(spec, ..., error_classifier=MyClassifier())
```

不注入会怎样？默认分类器只认通用异常，不认识的都按 POISON 处理——不会丢数据（有探针保护），但会浪费一轮二分。**接数据库请务必注入对应分类器。**

### 例 3：注入准入策略

```python
class MyAdmission:
    async def admit(self, record, *, overwrite=False): ...  # 判定：允许/冲突/拒绝
    async def on_accepted(self, record): ...                # Kafka 发送成功后
    async def on_persisted(self, record): ...               # 消费落库成功后刷新
    # 另有 start / close / 健康探测方法，见 protocols.py

binding = IngestBinding(..., admission=MyAdmission())       # 实例注入
```

### 不想自己写？去 examples 抄

每个例子都是**生产级质量、复制即用**（纳入 CI lint/类型检查，腐化会被拦下）：

| 目录 | 给你什么 | 示例自身的额外依赖 |
|---|---|---|
| [`pure_pipeline/`](examples/pure_pipeline/) | 零依赖全链路（**主示例**，裸装跑通就是纯度承诺的活体证明） | 无 |
| [`sqlite_sink/`](examples/sqlite_sink/) | SQL 幂等落库全套（引擎/方言/回源）+ 数据库异常分类器 | sqlalchemy、sqlmodel、aiosqlite |
| [`redis_admission/`](examples/redis_admission/) | Redis 分布式查重（多实例安全，含 Lua 原子占位） | redis |
| [`http_probe/`](examples/http_probe/) | 积压探针 + 磁滞限流状态机 | httpx |

仓库根的 [`examples/docker-compose.yml`](examples/docker-compose.yml) 提供 Kafka + Redis 双服务。

---

## 安装

```bash
pip install streamgate    # 只装 aiokafka + loguru + pydantic，没有 extras
```

存储/探针载体：从 `examples/` 复制对应模块，第三方库（redis、sqlalchemy、httpx 等）装进**你自己的项目**。其他数据库（PostgreSQL、MySQL……）：自己实现 `RecordWriter`——所有内置路径用的也是同一个钩子。

## 配置

组件均用类型化配置对象，与 `KAFKA__*` / `CONSUMER__*` / `BACKPRESSURE__*` 环境变量一一对应；必填项（如 topic、group_id）缺失即启动失败并附修复指引，绝不带猜测默认值上路。数据库/Redis 连接配置归你的应用管（示例各自带本地配置类）。详见 [CONFIGURATION.md](CONFIGURATION.md)。

## 架构

```
你的 HTTP 应用（鉴权/路由/OpenAPI 归你：包内零 Web 框架代码）
        │
        ▼
  IngestGateway ──► BackpressureSignal（注入：manual / HTTP 探针）
        │           AdmissionPolicy（注入：none / in-memory /
        │            你的 Redis|DB 实现）
        ▼
      Kafka ◄──────────────────────────────────────────────┐
        │                                                  │ DLQ ◄─ 坏数据
        ▼                                                  │      （二分 + 探针对照）
  ConsumerWorker ──► RecordWriter（你的存储） ──────────────┘
        │             └─ ErrorClassifier（注入）
        └─ 健康快照（用你自己的 Web 框架暴露）
```

包内禁止任何 sqlalchemy / sqlmodel / redis / aiosqlite / aioodbc / httpx 导入——由 import-linter forbidden 契约在 CI 强制拦截。

## 路线图

- 测试套件（首发暂无测试；API 已在生产验证，项目视其为首要技术债）
- 文档站
- 更多 examples 参考实现

## 许可证

[MIT](LICENSE)
