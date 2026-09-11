# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

[English](README.md) | 中文

**streamgate 是一条"带闸门的数据流水线"**：数据从门口推进去（生产），穿过 Kafka 管道（投递），从另一头的出口出去（处理）——两头都是一行构造：`Producer(bootstrap_servers, topic, ...)` / `Consumer(bootstrap_servers, topic, group_id, handler)`。

```
你的程序 ──► Producer（进） ──► Kafka（管道） ──► Consumer（出） ──► 你的 handler
              判重、背压、自愈                    批量缓冲、重试、坏数据隔离
```

框架核心只装 3 个依赖（`aiokafka` / `loguru` / `pydantic`），核心层**没有一行数据库、Redis、HTTP 客户端代码**。官方 I/O 策略实现收录在 [`streamgate.contrib`](#不想自己写用-contrib)——按 extras 按需安装、装完即可 import；可运行的完整演示在 [`examples/`](examples/)。

---

## 为什么选它

aiokafka 给的是**传输**，streamgate 给的是**门的运营语义**。幂等只防会话内 broker 重复，防不了 HTTP 重试、双击、跨会话重推；出口侧（位点、退避重试、异常分类、毒消息精确隔离、积压告警）与裸 aiokafka 差距最大。唯一性契约是让这个项目叫 "gate" 而不叫 "client" 的东西。诚实边界：纯转发的场景裸 aiokafka 十几行就够，框架不追求替代它——它把每个服务都要手写且容易写错的运营语义标准化。

- **机制归框架，策略归你。** 判重编排（存在性判定 → 原子占位 → 发送 → 成功/失败钩子 → 权威刷新）、重试、背压、断线重连、优雅停机这些"脏活累活"框架全包；你只管身份语义（什么算"同一条"）、Schema 和数据去哪——入库、实时分析、转发、告警，都只是 handler 里那几行代码的区别。
- **查重默认关闭，开了也不绑定存储。** 不传 `dedup` 就是纯推入，零判重概念；一行 `DedupOptions(key=...)` 开启进程内判重，注入共享存储载体开启多实例判重。保证强度 = 所选载体的强度，由载体自报、随 `PushResult.guarantee` 透出（`"process-local"` / `"distributed"`），框架不做部署形态校验。生产级 Redis 载体已收录为 `streamgate.contrib.redis_dedup`（装 `[redis]` extra 即用）。
- **路由键和身份键是两个键。** `key=` 是**路由键**：同 key 进同分区、分区内有序；不传则轮询、不保序（仅文档承诺，不做运行时提醒）。`DedupOptions(key=...)` 是**身份键**：判定"两条记录是否同一条"，只在开启判重时有意义。框架绝不把两个键焊死在一起。
- **一个出口契约，接任何去处。** `Consumer(..., handler=handle_batch)` 就是全部故事：正常返回 = 整批处理完成（框架提交位点）；抛异常 = 按分类处置（重试退避 / 毒批隔离 / 致命停机）。单条还是批量，只是 `batch_size=1` 和 `batch_size=N` 的区别。SQL upsert 出口已预装配为 `streamgate.contrib.sqlite_upsert` / `mssql_upsert`。
- **错误分类也是钩子。** "连接超时该重试"还是"数据脏了该隔离"，只有懂你出口的人知道。注入一个分类器即可；不注入也有安全的默认行为（不会丢数据）。
- **背压带磁滞防抖，拒绝期零写入。** 消费端积压超标自动拒绝推入，回落后自动放行；拒绝期间不占位、不写存储、不发 Kafka。
- **坏数据不堵管道。** 一条毒消息不会卡死整个分区：单条探针逐条定位，坏数据精确隔离进死信 topic，好数据照常处理、位点照常推进。
- **运维观测长在快照里。** 健康快照自带滑动窗口算好的速率/延迟指标（推入速率、判重命中、生产成败、处理速率、重试速率、处理耗时 avg/max），现有健康端点直接当监控数据源用——不配 Prometheus 也能看趋势、设告警。

**责任边界。** 路由：传 `key` 则同 key 保序；不传不保序（仅文档说明，刻意不做运行时提醒）。判重：强度就是载体的强度，载体自报、随每次 `PushResult` 透出。呈现：`push()` 返回传输无关的 `PushResult`（`accepted` / `duplicate` / `rejected` / `backpressure` / `unavailable`）——HTTP 状态码、error_code、对外文案的映射**全部归使用方适配层**，核心一个不给。

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
from streamgate import DedupOptions, Producer, ProducerOptions

class OrderIn(BaseModel):          # 你的数据格式，你来定义
    order_id: str
    amount: float

async def main() -> None:
    producer = Producer(
        "localhost:29092",                    # 数据往哪个集群去
        topic="orders",                       # 写到哪个 topic
        key=lambda r: r.order_id,             # 路由键：同 key 保序；不传轮询不保序
        options=ProducerOptions(
            dedup=DedupOptions(               # 一行开启进程内判重；不传 = 纯推入
                key=lambda r: r.order_id,     # 身份键：什么算"同一条"
                summary=lambda r: {"amount": r.amount},
            ),
        ),
    )
    await producer.start()
    try:
        result = await producer.push(OrderIn(order_id="o-1", amount=9.9))
        print(result.kind, result.guarantee)  # accepted process-local
    finally:
        await producer.close()

asyncio.run(main())
```

### 第 4 步：写"消费端"（数据出口）

```python
import asyncio
from streamgate import Consumer

async def handle(batch, context):
    # batch 是一批 dict —— 入库、实时分析、转发、告警，随你
    print(f"收到 {len(batch)} 条: {batch}")

async def main() -> None:
    consumer = Consumer(
        bootstrap_servers="localhost:29092",  # 数据从哪个集群来
        topic="orders",                       # 从哪个 topic 读
        group_id="my-group",                  # 消费组身份（位点归属）
        handler=handle,                       # 数据到哪去——唯一的出口
        batch_size=500,                       # 改成 1 即单条实时
        flush_timeout=5.0,                    # 攒不满时超时也触发
    )
    await consumer.run()   # 阻塞运行；Ctrl+C 优雅停机（自动清空缓冲、提交位点）

asyncio.run(main())
```

跑通即为完整链路。可直接运行的完整版在 [`examples/pure_pipeline/`](examples/pure_pipeline/)（裸装即可跑，SQLite 用 Python 标准库写）。

---

## 核心概念，记住三句话

1. **门口（Producer）管"收不收"**：判重、背压、Kafka 自愈都是它的活；
2. **出口（Consumer）管"怎么处理"**：批量、重试、死信隔离都是它的活；
3. **两个"口"之外的一切存储载体和业务动作都是你的**：写个 handler/载体 → 注入 → 完事。

三个使用层次：

| 层次 | 你用什么 | 适用场景 |
|------|----------|----------|
| **0 — 扁平构造** | `Producer(bootstrap_servers, topic, key, options)` + `Consumer(...)` 四个必填项 | 绝大多数场景：两项必填，其余全默认；判重一行开启 |
| **1 — 换组件** | `streamgate.protocols` 里的协议 + 内置零依赖实现 | 替换判重载体、背压信号、编解码、错误分类器 |
| **2 — 高级项** | `ProducerOptions` / `DedupOptions` / `ConsumerOptions` / `DlqOptions` / `RuntimeTuning` | 判重、背压、type 校验、单条探针、分类器、联动钩子、DLQ、调优——全可选、全默认 |

协议集中在 `streamgate.protocols`（冻结契约，只增不改）。

---

## 如何拓展

所有拓展都是同一个套路：**实现一个协议（或一个函数），然后注入**，不需要继承任何框架基类。

### 5 个钩子，按需替换

| 钩子协议 | 管什么 | 什么时候需要自己写 |
|---|---|---|
| **BatchHandler** | 数据到哪去 | 最常用！一个 async 函数就是完整出口 |
| **DedupCarrier** | 门口怎么判重 | 需要多实例共享判重时（如用 Redis） |
| **ErrorClassifier** | 处理失败算什么错 | 接了数据库，必须教它认数据库的错 |
| **BackpressureSignal** | 什么时候拒绝收货 | 需要根据消费端积压动态限流时 |
| **BackfillSource** | 缓存丢了怎么回源 | 判重缓存需要从数据库冷启动时 |

### 例 1：自定义出口（最常见）

```python
from streamgate import ConsumeContext, Consumer, JsonObject

async def my_handler(batch: list[JsonObject], context: ConsumeContext) -> None:
    ...  # 你的处理逻辑：幂等，正常返回 = 整批完成

consumer = Consumer(..., handler=my_handler)   # 注入即可
```

### 例 2：教框架认数据库的错（接 DB 必做）

```python
from streamgate import ConsumerOptions, ErrorKind

class MyClassifier:
    def classify(self, exc, attempt) -> ErrorKind:
        if "timeout" in str(exc).lower():
            return ErrorKind.RETRY    # 瞬态错误 → 退避重试
        return ErrorKind.POISON       # 内容问题 → 定位隔离进死信队列

consumer = Consumer(..., options=ConsumerOptions(classifier=MyClassifier()))
```

不注入会怎样？默认分类器只认通用异常，不认识的都按 POISON 处理——不会丢数据（有对照保护），但会浪费一轮定位。**接数据库请务必注入对应分类器**（生产级参考：`streamgate.contrib.sql_upsert.SQLAlchemyErrorClassifier`）。

### 例 3：注入判重载体（含回源）

```python
class MyCarrier:
    guarantee = "process-local"                             # 自报保证强度，随 PushResult 透出
    async def admit(self, record, *, force=False): ...      # 判定：放行（含占位）/重复/拒绝
    async def on_send_success(self, record): ...            # 每次 Kafka 发送成功后（通知型）
    async def on_send_failed(self, record): ...             # 发送失败后；默认保留占位自愈，
                                                            # 也可在此释放占位换"立即可重推"
    async def on_force_accepted(self, record): ...          # 仅 force 推入：摘要写，返回 False
                                                            # → cache_updated=false
    async def on_persisted(self, record): ...               # 消费处理成功后刷新
    # 另有 start / close / 健康探测方法，见 protocols.py

producer = Producer(..., options=ProducerOptions(
    dedup=DedupOptions(key=lambda r: r.order_id, carrier=MyCarrier()),
))
```

单进程查重到这就够了（或者干脆用内置进程内载体，一行开启）。但如果判重数据放在"权威库的副本"里（如 Redis 分布式判重），副本可能与真相不一致：缓存重启清空后，来了一条"缓存里没有"的记录——是新数据，还是老数据丢了记录？不能瞎判。这时载体还需要一个核实渠道：**回源（BackfillSource）**——去权威数据库问一句"这个身份键的历史摘要是什么"，补回缓存再判定。

```python
from streamgate.protocols import BackfillSource

class MyBackfill:                              # 能力清单只有一个方法
    async def load(self, scope: str) -> dict[str, dict] | None:
        ...   # 返回 {identity: summary} 映射；scope = 身份键（单键载体）
              # 或组号（组载体，须返回该组全量身份）；无记录返回 {}
              # 核实不了就抛异常（按"不确定"处理：宁可拒，不重复）

carrier = MyCarrier(backfill=MyBackfill())      # 回源传给你的载体，不是 Producer！
```

不配回源会怎样？缓存里查不到的记录，载体不会猜"这是新数据"，而是去问回源；默认的 `NoBackfill` 问不出结果（`load` 直接抛错），于是按"核实不了"处理——**拒绝，绝不放行**。这不是故障，是故意的兜底：拒绝可恢复（稍后重试即可），重复不可逆（进库就洗不掉）。

| 回源配置 | 缓存查不到这条记录时 |
|---|---|
| 没配（默认 `NoBackfill`） | 核实不了 → 拒绝（fail-closed） |
| 配了，权威库说"有这条" | duplicate，返回既有摘要 |
| 配了，权威库说"确实没有" | 放行（确认是新数据） |
| 配了，但权威库也挂了 | 仍拒绝——核实不了绝不猜 |

内置进程内载体用不上回源：它的进程内字典本身就是权威（查不到 = 真的没有），代价是仅单进程有效、重启即清空——一旦多副本部署，就需要 Redis 判重 + 回源的两级结构（`streamgate.contrib.redis_dedup` + `streamgate.contrib.sql_upsert.SqlBackfill` 已内置这套组合）。

### 例 4：自定义背压信号（跟着消费端积压走）

推入端怎么知道该不该拒收？每次 `push` 前问一次注入的信号源："现在能收吗？"——拒收期间请求自动得到 backpressure，积压回落后自动放行（磁滞防抖见 contrib）。

```python
from streamgate.protocols import BackpressureSignal, BackpressureSnapshot

class MySignal:                                # 不用继承，duck typing
    async def snapshot(self) -> BackpressureSnapshot:
        # 判定来源随你：探消费端状态接口 / 读本地指标……
        return BackpressureSnapshot(rejecting=..., reason="backlog_high", lag=...)
    async def start(self): ...
    async def close(self): ...
    @property
    def rejecting(self) -> bool: ...           # 热路径只读这个布尔，保持便宜
    @property
    def reject_reason(self) -> str | None: ...

producer = Producer(..., options=ProducerOptions(signal=MySignal()))   # 注入即可
```

不注入会怎样？默认 `ManualBackpressureSignal`——纯手动开关，永远放行；维护窗口可把 `signal.rejecting` 翻成 `True` 手动挡闸。推入端与消费端不在同一进程时，框架"看不见"积压，用 `streamgate.contrib.http_probe`（积压探针 + 磁滞限流状态机，装 `[http]` extra）。

### 不想自己写？用 contrib

官方策略实现随 wheel 发布，按 extras 装依赖，**import 即用**（与核心同门禁：CI lint/类型检查全覆盖）：

| Extra | 安装 | 子包 | 给你什么 |
|---|---|---|---|
| `[redis]` | `pip install "streamgate[redis]"` | `streamgate.contrib.redis_dedup` | Redis 分布式判重载体（多实例安全，Lua 原子占位/冷回源/fail-closed，`guarantee="distributed"`）；单身份键版 `RedisDedupCarrier` + 组版 `RedisGroupDedupCarrier`（组内候选判重） |
| `[sql]` | `pip install "streamgate[sql]"` | `streamgate.contrib.sqlite_upsert` / `.mssql_upsert` / `.sql_upsert` | SQL 幂等出口全套：`SqliteConsumer` / `MssqlConsumer` 预装配工厂（批量 upsert handler + 单条探针 + 数据库异常分类器三件套）、`upsert_outlet` 原语、引擎工厂、`SqlBackfill` 回源 |
| `[http]` | `pip install "streamgate[http]"` | `streamgate.contrib.http_probe` | 积压探针 + 磁滞限流状态机（trip/recover 防抖、fail-closed） |

```python
from streamgate import DedupOptions, Producer, ProducerOptions
from streamgate.contrib.redis_dedup import (
    RedisConfig, RedisDedupCarrier, RedisDedupCache,
)

redis_config = RedisConfig(url="redis://localhost:6379/0")
producer = Producer(
    "localhost:29092",
    topic="orders",
    options=ProducerOptions(
        dedup=DedupOptions(
            key=lambda r: r.order_id,          # 身份键声明（载体按同一键判定）
            carrier=RedisDedupCarrier(
                cache=RedisDedupCache(redis_config),
                key=lambda r: r.order_id,
                summary=lambda r: {"amount": r.amount},
                redis_config=redis_config,
            ),
        ),
    ),
)
result = await producer.push(order)
if result.kind == "duplicate":
    ...   # result.summary = 已有记录的摘要
```

同一场景，如果判重边界是"组"（批次/任务/导入会话：一个组号 + 组内身份不重复），
换 **`RedisGroupDedupCarrier`**——组是一整个 HASH，冷路径**整组一次回源预热**，
组内后续记录纯 Redis 命中，零 DB 查询；只有"全新组"才触发回源（组级单飞合并并发）：

```python
from streamgate import DedupOptions, Producer, ProducerOptions
from streamgate.contrib.redis_dedup import (
    RedisConfig, RedisGroupDedupCache, RedisGroupDedupCarrier,
)

redis_config = RedisConfig(url="redis://localhost:6379/0")
producer = Producer(
    "localhost:29092",
    topic="orders",
    options=ProducerOptions(
        dedup=DedupOptions(
            key=lambda r: r.order_id,          # 组内身份键声明
            carrier=RedisGroupDedupCarrier(    # 仅组内判重（组 = 判重完整边界）
                cache=RedisGroupDedupCache(redis_config),
                group_key=lambda r: r.batch_no,   # payload 组号（组 → 组内身份 HASH）
                key=lambda r: r.order_id,
                summary=lambda r: {"amount": r.amount},
                redis_config=redis_config,
            ),
        ),
    ),
)
result = await producer.push(order)
if result.kind == "duplicate":
    ...   # result.summary = 组内既有记录的摘要
```

组载体搭配 `SqlBackfill` 做冷回源时，只需加一个列名，**整组**一次查回预热：

```python
from streamgate.contrib.sql_upsert import SqlBackfill

carrier = RedisGroupDedupCarrier(
    cache=RedisGroupDedupCache(redis_config),
    group_key=lambda r: r.batch_no,
    key=lambda r: r.order_id,
    backfill=SqlBackfill(            # group_column 已设 → 整组回源
        engine=...,
        table="orders",
        key_column="order_id",
        summary_columns={"amount": "amount"},
        order_columns=["seq_no"],
        group_column="batch_no",
    ),
)
```

> **大组注意（仅备注，不构成限制）**：整组回填按 pipeline 分片写入（纯实现细节）；
> 活跃大组的 Redis 内存水位随 `group_ttl_seconds`（idle-GC 续期）增长；单次回源
> 查询体积大时留意 `query_wait_seconds`（库内分页由回源实现自定）；写入间隔接近
> TTL 的低速率组可能在批未结束时过期，触发一次额外回源属正常自愈。

SQL 出口的开箱路径——工厂预接三件套，其余与核心 `Consumer` 完全一致：

```python
from models import Order                        # 你的 SQLModel 表
from streamgate.contrib.sqlite_upsert import SqliteConsumer, Upsert

consumer = SqliteConsumer(
    db="sqlite+aiosqlite:///./data/app.db",
    upserts=[Upsert(model=Order, keys=["order_id"])],
    bootstrap_servers="localhost:29092",
    topic="orders",
    group_id="order-sink",
    # batch_size / flush_timeout / options 与核心 Consumer 一致，可继续传
)
await consumer.run()
```

缺对应 extra 时 import 会报错并提示该装哪个 extras，不会出现裸的 `ModuleNotFoundError`。

**稳定性**：contrib 为 provisional（Beta 级）——可直接上生产（这些实现并入前已生产验证），但小版本内允许调整签名；核心 API 契约不受影响（核心层永不 import contrib）。

仓库根的 [`examples/docker-compose.yml`](examples/docker-compose.yml) 提供 Kafka + Redis 双服务；五个可运行演示见 [`examples/`](examples/)（策略演示直接 import contrib；[`examples/prod_pipeline/`](examples/prod_pipeline/) 把 Redis 判重 + HTTP 探活 + MSSQL 出口的生产拓扑一次接全）。

---

## 安装

```bash
pip install streamgate             # 只装 aiokafka + loguru + pydantic
pip install "streamgate[redis]"    # + Redis 分布式判重
pip install "streamgate[sql]"      # + SQL 幂等出口（SQLite + MSSQL）
pip install "streamgate[http]"     # + HTTP 探活背压
```

其他去处（PostgreSQL、MySQL、ES……）：自己写 `handler`——contrib 内置路径接的也是同一个契约。

## 配置

`Producer` / `Consumer` 必填项直接传参，未传时回退同名环境变量（`KAFKA__BOOTSTRAP_SERVERS` / `KAFKA__TOPIC` / `CONSUMER__GROUP_ID`）；高级项收口在 `ProducerOptions` / `ConsumerOptions`（判重、背压、DLQ、调优、指标窗口等），同样跟随 `KAFKA__*` / `CONSUMER__*` 环境变量回退。必填项缺失即启动失败并附修复指引，绝不带猜测默认值上路。数据库/Redis 连接配置归你的应用管（示例各自带本地配置）。详见 [CONFIGURATION.md](CONFIGURATION.md)。

## 架构

```
你的应用（鉴权/路由/OpenAPI 归你：包内零 Web 框架代码）
        │
        ▼
    Producer ──► BackpressureSignal（注入：manual / HTTP 探针）
        │       DedupCarrier（注入：内置进程内 / 你的 Redis|DB 实现；
        │        默认关闭零概念）— 自报 guarantee，随 PushResult 透出
        ▼
      Kafka ◄──────────────────────────────────────────────┐
        │                                                  │ DLQ ◄─ 坏数据
        ▼                                                  │      （单条探针定位）
     Consumer ──► handler（你的出口：SQL / 分析 / 转发 /     │
        │             告警……）─────────────────────────────┘
        │             └─ ErrorClassifier（经 options 注入）
        │             └─ persist_hook（载体的 on_persisted 权威刷新）
        └─ 健康快照（状态 + 速率/延迟指标，用你自己的 Web 框架暴露）
```

`Producer` / `Consumer` 是可嵌入任何宿主的循环——脚本、FastAPI 服务、独立 worker 皆可；进程边界归使用方，收发机制、判重编排、重试、自愈、优雅停机归框架。

核心层禁止 import `streamgate.contrib`（contrib 反向依赖核心是合法的）——由 import-linter 分层契约在 CI 强制拦截；contrib 的第三方依赖全部走 extras 可选声明，裸装 `pip install streamgate` 不携带。

## 从 0.x 迁移

1.0.0 对**两端**都做了破坏性重设计：消费侧 `ConsumerWorker` + `ConsumeSpec` + `RecordWriter` 由扁平 `Consumer` 构造器替代；生产侧 `IngestBinding` + `IngestGateway` 由扁平 `Producer` 构造器替代（`process()` → `push()`、`IngestOutcome` → `PushResult`、`overwrite` → `force`、`admission` → `DedupOptions`、`entity_key`/`slot_key` 拆分为路由键 `key` + 身份键 `DedupOptions.key`）。完整的新旧对照（API、结果 kind、指标名、日志事件、环境变量、import 路径）见 [CHANGELOG](CHANGELOG.md)。

## 路线图

- 测试套件（首发暂无测试；API 已在生产验证，项目视其为首要技术债）
- 文档站
- 更多 `streamgate.contrib` 判重载体/出口实现

## 许可证

[MIT](LICENSE)
