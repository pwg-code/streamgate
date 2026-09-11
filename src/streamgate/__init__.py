"""streamgate 公共 API 出口：数据管道机制内核（produce + consume + dedup）。

定位：条件推入 → 可靠投递 → 自由出口的数据管道框架。
- 机制归包：判重编排（唯一性契约：存在性判定 + 原子占位 + 冷回源，
  存储载体由你注入，默认关闭零概念）、背压/测活/自愈（重连、paused、
  DLQ 定位隔离、位点）、Kafka 收发、批量缓冲、优雅停机
- 策略归使用方：路由键/身份键/摘要、Schema 校验、数据出口
  （Consumer 的 handler / probe / ConsumerOptions 注入）；DB/Redis/HTTP 等
  I/O 策略实现收敛于 streamgate.contrib（随 wheel 发布、按 extras 携带
  依赖，可运行演示见仓库 examples/）
- 呈现归使用方：HTTP 路由/鉴权/OpenAPI、健康端点暴露（包内零 fastapi/uvicorn）

核心依赖仅 aiokafka / loguru / pydantic 三项；核心层不得 import
streamgate.contrib（import-linter 分层契约强制）。

生产侧扁平构造：Producer(bootstrap_servers, topic, key, options)
出口侧扁平构造：Consumer(bootstrap_servers, topic, group_id, handler)
    —— 数据入口/出口即推入/消费循环，嵌入宿主即可，进程边界归使用方
组件替换：protocols.py 中的协议 + 下方内置零 I/O 实现
高级项收口：ProducerOptions / ConsumerOptions / DlqOptions / RuntimeTuning
（不传即全默认）

使用方只允许 import 本模块与 streamgate.contrib.* 子包出口，
不得深入其余内部子模块（import-linter 门禁强制）。
"""

from streamgate.config import (
    BackpressureConfig,
    KafkaConfig,
    MetricsConfig,
)
from streamgate.consumer.classifier import DefaultErrorClassifier
from streamgate.consumer.dlq import (
    BisectOutcome,
    BufferedMessage,
    DlqProducer,
    DlqSendError,
    QuarantineRequest,
    locate_and_quarantine,
)
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.consumer.options import (
    ConsumerOptions,
    DlqOptions,
    RuntimeTuning,
)
from streamgate.consumer.runner import Consumer
from streamgate.ingest.dedup.in_memory import InMemoryDedupCarrier
from streamgate.ingest.producer import (
    DedupOptions,
    Producer,
    ProducerOptions,
)
from streamgate.obs.logging import configure_logger, logger
from streamgate.obs.metrics import LoggingMetricsSink, MetricsSink
from streamgate.protocols import (
    AllowAllSignal,
    BackfillSource,
    BackpressureSignal,
    BackpressureSnapshot,
    BatchHandler,
    ConsumeContext,
    Decision,
    DecisionKind,
    DedupCarrier,
    Envelope,
    ErrorClassifier,
    ErrorKind,
    JsonObject,
    MessageCodec,
    NoBackfill,
    Probe,
    ProbeResult,
    PushKind,
    PushResult,
    RejectInfo,
)
from streamgate.resilience.backpressure import ManualBackpressureSignal
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    ProducerHealthResponse,
    collect_consumer_health,
    collect_producer_health,
)
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService

__version__ = "2.1.0"

__all__ = [
    "AllowAllSignal",
    "BackfillSource",
    "BackpressureConfig",
    "BackpressureSignal",
    "BackpressureSnapshot",
    "BatchHandler",
    "BisectOutcome",
    "BufferedMessage",
    "ConsumeContext",
    "ConsumeRuntime",
    "Consumer",
    "ConsumerHealthResponse",
    "ConsumerOptions",
    "Decision",
    "DecisionKind",
    "DefaultErrorClassifier",
    "DedupCarrier",
    "DedupOptions",
    "DlqOptions",
    "DlqProducer",
    "DlqSendError",
    "Envelope",
    "ErrorClassifier",
    "ErrorKind",
    "InMemoryDedupCarrier",
    "JsonObject",
    "JsonEnvelopeCodec",
    "KafkaConfig",
    "KafkaConsumerService",
    "LoggingMetricsSink",
    "ManualBackpressureSignal",
    "MessageCodec",
    "MetricsConfig",
    "MetricsSink",
    "NoBackfill",
    "Producer",
    "ProducerHealthResponse",
    "ProducerOptions",
    "Probe",
    "ProbeResult",
    "PushKind",
    "PushResult",
    "QuarantineRequest",
    "RejectInfo",
    "RuntimeTuning",
    "collect_consumer_health",
    "collect_producer_health",
    "configure_logger",
    "consume_loop",
    "locate_and_quarantine",
    "logger",
]
