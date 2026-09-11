"""Producer：数据生产入口（背压 → 判重 → Kafka 发送 → 发送结果钩子）。

与消费侧 Consumer 完全同构的构造形态：必填项平铺（未传回退环境变量），
高级项折叠进 options（不传 = 全默认零概念）。三层回退链：
显式传值 > 环境变量 > 内置默认。

路由键与身份键是两个概念：
- key（构造参数）：路由键 —— 存在理由是顺序。Kafka 同 key 哈希进同分区、
  分区内有序；不传则轮询、不保序（仅文档说明，不做运行时提醒）。
- DedupOptions.key：身份键 —— 存在理由是判重。判定"两条记录是否同一条"，
  仅开启判重时有意义。

责任边界：
- 判重默认关闭（opt-in）：不传 dedup 即纯推入，零判重概念。
- 判重强度 = 所选载体（carrier）的强度：载体自报、随 PushResult.guarantee
  透出；框架不做部署形态启动期校验。
- 呈现归使用方：push() 返回传输无关的 PushResult，不抛 HTTP 异常、
  不构造 Response；HTTP 状态码/error_code/文案映射归使用方适配层。
- 日志事件名与字段为稳定观测契约。

启动时无论初始连接是否成功，都会拉起后台监控任务：周期性探活，
不健康时销毁旧 Kafka 实例（其内部 sender/client 可能已进入 fatal 状态）
并新建实例重连，覆盖启动期断线与运行期断线两种场景（内部机制，
不暴露独立类）。
"""

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generic, TypeVar, cast

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError
from pydantic import BaseModel

from streamgate.config import BackpressureConfig, KafkaConfig
from streamgate.ingest.dedup.in_memory import InMemoryDedupCarrier
from streamgate.obs.logging import logger
from streamgate.obs.metrics import ProducerMetrics
from streamgate.protocols import (
    BackpressureSignal,
    BackpressureSnapshot,
    DecisionKind,
    DedupCarrier,
    JsonObject,
    MessageCodec,
    PushKind,
    PushResult,
)
from streamgate.resilience.backpressure import ManualBackpressureSignal
from streamgate.resilience.health import (
    ProducerHealthResponse,
    collect_producer_health,
)
from streamgate.transport.codec import JsonEnvelopeCodec

# schema 锚定推断：泛型参数锚定 Pydantic 模型，lambda 钩子获得精确字段补全。
RecordT = TypeVar("RecordT", bound=BaseModel)

DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092"
DEFAULT_MESSAGE_TYPE = "streamgate_record"
DEFAULT_METRICS_WINDOW_SECONDS = 60
METRICS_WINDOW_RANGE = (1, 600)

# 旧参数 → 迁移指引（收到即报错，指向 CHANGELOG 迁移指南）
_MIGRATION_HINT = (
    "the producer API was redesigned in 1.0.0 — see the CHANGELOG.md "
    "1.0.0 migration guide: the declarative two-step ingest assembly is "
    "replaced by Producer(bootstrap_servers, topic, key, "
    "options=ProducerOptions(...)); process() is now push(), and the result "
    "object is now PushResult"
)


def _env_str(key: str) -> str | None:
    """读取环境变量（去空白；空串视为未设置）。"""
    value = os.environ.get(key)
    if value is None or not value.strip():
        return None
    return value.strip()


def _env_int(key: str, default: int) -> int:
    """整数环境变量回退（非法值报错并指明变量名）。"""
    raw = _env_str(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"env {key}={raw!r} is not a valid integer") from None


def _iso_z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _require_topic(topic: str | None) -> str:
    """必填项解析：显式传值 > 环境变量；仍缺失即报错（含修复指引）。"""
    resolved = topic or _env_str("KAFKA__TOPIC")
    if not resolved:
        raise ValueError("topic is required: pass topic= or set KAFKA__TOPIC")
    return resolved


def _optional_hook(
    carrier: DedupCarrier[RecordT], name: str
) -> Callable[[RecordT], Awaitable[object]] | None:
    """按名解析可选钩子：精简实现缺新钩子时返回 None（鸭子类型防御）。"""
    candidate: object | None = getattr(carrier, name, None)
    if not callable(candidate):
        return None
    return cast("Callable[[RecordT], Awaitable[object]]", candidate)


async def _lifecycle_start(component: object) -> None:
    """启动可选组件（鸭子类型防御：未实现 start() 的实现跳过）。"""
    start: Callable[[], Awaitable[None]] | None = getattr(component, "start", None)
    if start is not None:
        await start()


async def _lifecycle_close(component: object) -> None:
    """释放可选组件（鸭子类型防御：未实现 close() 的实现跳过）。"""
    close: Callable[[], Awaitable[None]] | None = getattr(component, "close", None)
    if close is not None:
        await close()


@dataclass
class DedupOptions(Generic[RecordT]):
    """判重选项（opt-in：ProducerOptions.dedup 不传即纯推入，零判重概念）。

    - key：身份键 —— "两条记录何时算同一条"（开启判重时必填）。
    - carrier：单参数位载体；None = 内置进程内实现（仅单进程有效，
      多实例拓扑注入共享存储载体，Redis 参考实现见 streamgate.contrib.redis_dedup）。
    - ttl_seconds：占位存活预算；None → 载体自定（内置进程内载体即不过期，
      随进程存续；外置载体在自己的构造参数里配置 TTL）。
    - summary：duplicate 时回给调用方的"已有记录摘要"；None = 空摘要。
    """

    key: Callable[[RecordT], str]
    carrier: DedupCarrier[RecordT] | None = None
    ttl_seconds: int | None = None
    summary: Callable[[RecordT], JsonObject] | None = None


@dataclass
class ProducerOptions(Generic[RecordT]):
    """Producer 高级项收口（不传 = 全默认零负担）。

    每一项的一行语义：
    - dedup：判重选项（DedupOptions）；None = 不判重（默认）
    - backpressure：背压闸门配置（BackpressureConfig）；None = 关闭（完全放行）
    - signal：背压信号注入点（BackpressureSignal，http_probe contrib 继续可用）；
      None = 手动静态开关（默认不背压）
    - codec：信封编解码（默认内置 JSON 实现）
    - message_type：信封 type 标记
    - metrics_window_seconds：健康速率窗口；None → METRICS__WINDOW_SECONDS → 60
    - log_context：日志扩展上下文钩子（进信封日志与 ingest_* 观测事件）
    - kafka：Kafka 连接/自愈调优（KafkaConfig 逃生门）；None = 全默认
    """

    dedup: DedupOptions[RecordT] | None = None
    backpressure: BackpressureConfig | None = None
    signal: BackpressureSignal | None = None
    codec: MessageCodec | None = None
    message_type: str = DEFAULT_MESSAGE_TYPE
    metrics_window_seconds: int | None = None
    log_context: Callable[[RecordT], JsonObject] | None = None
    kafka: KafkaConfig | None = None

    def resolved_metrics_window_seconds(self) -> int:
        """解析指标窗口（显式传值 > METRICS__WINDOW_SECONDS > 60；越界报错）。"""
        window = (
            self.metrics_window_seconds
            if self.metrics_window_seconds is not None
            else _env_int(
                "METRICS__WINDOW_SECONDS", DEFAULT_METRICS_WINDOW_SECONDS
            )
        )
        low, high = METRICS_WINDOW_RANGE
        if not low <= window <= high:
            raise ValueError(
                f"metrics_window_seconds={window} out of range; "
                f"set a value between {low} and {high} "
                "(or env METRICS__WINDOW_SECONDS)"
            )
        return window


class _KafkaClient:
    """Kafka 发送端（内部实现，不对外暴露）：带自愈监控。

    启动时无论初始连接是否成功，都会拉起后台监控任务：周期性探活，
    不健康时销毁旧 producer 实例（其内部 sender/client 可能已进入
    fatal 状态）并新建实例重连，覆盖启动期断线与运行期断线两种场景。
    健康观测面（check_health/last_failure_at/reconnect_count/
    down_duration_seconds）供 resilience.health 结构化匹配。
    """

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        topic = config.topic
        if not topic:
            raise ValueError("kafka topic is required: set KafkaConfig.topic")
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None
        self._started: bool = False
        self._closed: bool = False
        self._reconnect_attempt = 0
        self._lock = asyncio.Lock()
        self._monitor_task: asyncio.Task[None] | None = None
        # --- 自愈状态跟踪 ---
        self._reconnecting: bool = False          # 单飞行标志：True=后台重建进行中
        self._down_started_at: datetime | None = None  # 当前掉线周期起始（UTC）
        self._last_failure_at: datetime | None = None  # 最近一次 send 失败（UTC）
        self._episode_reconnect_count: int = 0    # 当前掉线周期内重建尝试次数
        self._rejected_requests: int = 0          # 当前掉线周期内拒绝计数
        self._consecutive_failures: int = 0       # 连续 send 失败计数（防抖阈值用）

    async def start(self) -> None:
        """启动服务并拉起自愈监控任务。

        初始连接失败抛异常（调用方决定是否降级启动），但监控任务仍会继续重连。
        """
        async with self._lock:
            if self._closed:
                raise RuntimeError("_KafkaClient is closed")
            if self._monitor_task is None:
                self._monitor_task = asyncio.create_task(self._monitor_loop())
            if self._started:
                return
        await self._connect()

    async def _connect(self) -> None:
        async with self._lock:
            if self._closed or self._started:
                return
            producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers,
                acks=self._config.acks,
                request_timeout_ms=self._config.request_timeout_ms,
                enable_idempotence=self._config.enable_idempotence,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            )
            try:
                await producer.start()
            except Exception as e:
                event = (
                    "kafka_connection_failed"
                    if isinstance(e, KafkaConnectionError)
                    else "kafka_start_failed"
                )
                logger.error(
                    event,
                    error=str(e),
                    bootstrap_servers=self._config.bootstrap_servers,
                    attempt=self._reconnect_attempt,
                )
                try:
                    await producer.stop()
                except Exception as close_e:
                    logger.debug("kafka_cleanup_error", error=str(close_e))
                raise
            self._producer = producer
            self._started = True
            self._reconnect_attempt = 0
            self._consecutive_failures = 0
            logger.info(
                "kafka_connected", bootstrap_servers=self._config.bootstrap_servers
            )

    async def _monitor_loop(self) -> None:
        while True:
            # R3：异常/重建中高频探活，健康态低频
            if self._reconnecting or self._down_started_at is not None:
                interval = self._config.unhealthy_check_interval_seconds
            else:
                interval = self._config.health_check_interval_seconds
            await asyncio.sleep(interval)
            if self._closed:
                return
            try:
                healthy = await self.check_health()
            except Exception as e:
                logger.debug("health_check_failed", error=str(e))
                healthy = False
            if healthy:
                continue
            await self._trigger_reconnect()

    async def _trigger_reconnect(self) -> None:
        """异步触发重建（单飞行防抖）。

        已在重建中或已关闭 -> 直接返回。
        首次触发 -> 设 _down_started_at、发射 kafka_down_started。
        每次触发 -> 发射 kafka_reconnect_started、创建后台任务。
        """
        async with self._lock:
            if self._closed or self._reconnecting:
                return
            self._reconnecting = True
            if self._down_started_at is None:
                now = datetime.now(timezone.utc)
                self._down_started_at = now
                logger.info("kafka_down_started", at=now.isoformat())
        logger.info("kafka_reconnect_started")
        asyncio.create_task(self._reconnect_and_recover())

    async def _reconnect(self) -> None:
        """连续重试直到成功（指数退避，对齐 consumer 重连策略）。

        失败按 backoff 指数退避后立即再试，不回落到探活间隔。
        """
        while not self._closed:
            async with self._lock:
                if self._closed:
                    return
                await self._close_safely()
            try:
                await self._connect()
                return
            except Exception as e:
                self._reconnect_attempt += 1
                self._episode_reconnect_count += 1
                backoff = min(
                    self._config.reconnect_max_backoff_seconds,
                    self._config.reconnect_base_backoff_seconds
                    * (2 ** min(self._reconnect_attempt - 1, 5)),
                )
                logger.error(
                    "kafka_reconnect_failed",
                    error=str(e),
                    attempt=self._reconnect_attempt,
                    backoff_seconds=backoff,
                )
                await asyncio.sleep(backoff)

    async def _reconnect_and_recover(self) -> None:
        """后台重建任务：调用 _reconnect（循环至成功）→ 发射恢复事件 → 清标志。"""
        started_monotonic = time.monotonic()
        try:
            await self._reconnect()
        except Exception as e:
            logger.error("kafka_reconnect_unexpected_error", error=str(e))
            return
        finally:
            async with self._lock:
                self._reconnecting = False

        duration = time.monotonic() - started_monotonic
        async with self._lock:
            self._consecutive_failures = 0
            logger.info(
                "kafka_reconnected",
                reconnect_duration_seconds=round(duration, 2),
                reconnect_count=self._episode_reconnect_count,
            )
            if self._down_started_at is not None:
                now = datetime.now(timezone.utc)
                down_duration = (now - self._down_started_at).total_seconds()
                logger.info(
                    "kafka_recovered",
                    down_duration_seconds=round(down_duration, 2),
                    reconnect_count=self._episode_reconnect_count,
                    rejected_requests=self._rejected_requests,
                )
                self._down_started_at = None
                self._episode_reconnect_count = 0
                self._rejected_requests = 0
                self._consecutive_failures = 0

    async def _close_safely(self) -> None:
        if self._producer is None:
            self._started = False
            return
        try:
            await self._producer.stop()
        except Exception as e:
            logger.debug("kafka_cleanup_error", error=str(e))
        finally:
            self._producer = None
            self._started = False

    async def stop(self) -> None:
        async with self._lock:
            self._closed = True
            task = self._monitor_task
            self._monitor_task = None
            await self._close_safely()
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("kafka_disconnected")

    async def send(self, key: str | None, message: JsonObject) -> None:
        """发送单条消息。key=None 时不带路由键（轮询分区、不保序）。"""
        async with self._lock:
            if self._closed or not self._started or self._producer is None:
                self._rejected_requests += 1
                raise RuntimeError("_KafkaClient is not started")
            producer = self._producer
        payload = json.dumps(message, ensure_ascii=False, default=str)
        try:
            await producer.send_and_wait(
                topic=self._topic,
                key=key,
                value=payload,
            )
        except KafkaError as e:
            now = datetime.now(timezone.utc)
            self._last_failure_at = now
            self._rejected_requests += 1
            self._consecutive_failures += 1
            logger.error(
                "kafka_send_failed",
                key=key,
                topic=self._topic,
                error=str(e),
                consecutive_failures=self._consecutive_failures,
            )
            # R1：连续失败达阈值即触发异步重建（单飞行防抖）
            if self._consecutive_failures >= self._config.reconnect_failure_threshold:
                await self._trigger_reconnect()
            raise

    async def check_health(self) -> bool:
        async with self._lock:
            if self._closed or not self._started or self._producer is None:
                return False
            producer = self._producer
            last_failure = self._last_failure_at

        # 近窗口内有 send 失败 -> 不健康（元数据可达不等于能发送）
        if last_failure is not None:
            age = (datetime.now(timezone.utc) - last_failure).total_seconds()
            if age < self._config.send_failure_window_seconds:
                return False

        # 元数据可达性
        try:
            ok = await asyncio.wait_for(
                producer.client.force_metadata_update(),
                timeout=2.0,
            )
        except Exception as e:
            logger.debug("health_check_failed", error=str(e))
            return False
        return bool(ok)

    @property
    def last_failure_at(self) -> datetime | None:
        return self._last_failure_at

    @property
    def reconnect_count(self) -> int:
        return self._episode_reconnect_count

    @property
    def down_duration_seconds(self) -> float | None:
        if self._down_started_at is None:
            return None
        return (datetime.now(timezone.utc) - self._down_started_at).total_seconds()

    @property
    def is_reconnecting(self) -> bool:
        return self._reconnecting

    @property
    def rejected_requests(self) -> int:
        return self._rejected_requests


class Producer(Generic[RecordT]):
    """数据生产入口：一个可嵌入宿主的推入门面。

    两项必填各回答一个问题：
    - bootstrap_servers：数据往哪个 Kafka 集群去（未传 → KAFKA__BOOTSTRAP_SERVERS）
    - topic：写到哪个 topic（未传 → KAFKA__TOPIC；缺失即报错，含修复指引）
    - key：可选路由键 —— 传则同 key 保序；不传轮询不保序（仅文档承诺）
    - options：高级项折叠（ProducerOptions）；不传 = 全默认零概念

    push(record, force=False, source=...)：背压 → 判重 → Kafka 发送 →
    发送结果钩子，返回 PushResult（不抛运行时异常；未 start() 即调用
    抛 RuntimeError，属装配错误）。判重默认关闭；一行开启进程内判重：
    ``options=ProducerOptions(dedup=DedupOptions(key=lambda r: r.order_id))``。
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        key: Callable[[RecordT], str] | None = None,
        options: ProducerOptions[RecordT] | None = None,
        **legacy: object,
    ) -> None:
        if legacy:
            raise TypeError(
                f"Producer received unknown legacy parameter(s): "
                f"{sorted(legacy)}; {_MIGRATION_HINT}"
            )
        self._bootstrap_servers: str = (
            bootstrap_servers
            or _env_str("KAFKA__BOOTSTRAP_SERVERS")
            or DEFAULT_BOOTSTRAP_SERVERS
        )
        self._topic: str = _require_topic(topic)
        self._key = key
        self._options = options or ProducerOptions()
        self._carrier: DedupCarrier[RecordT] | None = self._resolve_carrier()
        self._codec = self._options.codec or JsonEnvelopeCodec()
        self._backpressure = self._options.backpressure or BackpressureConfig(
            enabled=False
        )
        self._signal = self._options.signal or ManualBackpressureSignal()
        self._client: _KafkaClient | None = None
        self._started = False
        # 速率指标窗（构造时刻即进程启动锚点；不配置走默认 60s 窗口）
        self._metrics = ProducerMetrics(
            window_seconds=self._options.resolved_metrics_window_seconds()
        )

    # ---- 组件解析（声明期）----

    def _resolve_carrier(self) -> DedupCarrier[RecordT] | None:
        """判重载体解析：dedup 未配置 = None（纯推入）；未注入载体 = 内置进程内。"""
        dedup = self._options.dedup
        if dedup is None:
            return None
        if dedup.carrier is not None:
            return dedup.carrier
        return InMemoryDedupCarrier(
            dedup.key, summary=dedup.summary, ttl_seconds=dedup.ttl_seconds
        )

    @property
    def carrier(self) -> DedupCarrier[RecordT] | None:
        """判重载体实例（未开启判重时 None；使用方查询/复用时直接取用）。"""
        return self._carrier

    def _kafka_config(self) -> KafkaConfig:
        """Kafka 配置：构造器必填值恒覆盖 options.kafka 同名字段。"""
        base = self._options.kafka or KafkaConfig()
        return base.model_copy(
            update={
                "bootstrap_servers": self._bootstrap_servers,
                "topic": self._topic,
            }
        )

    # ---- 生命周期（宿主在 startup/shutdown 调用）----

    async def start(self) -> None:
        """启动 Kafka/判重载体/背压信号（懒连接，不抛；故障在调用时降级）。"""
        if self._started:
            return
        client = _KafkaClient(self._kafka_config())
        try:
            await client.start()
        except Exception as e:
            logger.error("kafka_startup_failed", error=str(e))
            logger.warning("startup_with_degraded_kafka")
        self._client = client
        if self._carrier is not None:
            await _lifecycle_start(self._carrier)
        self._warn_if_trip_exceeds_ttl()
        await _lifecycle_start(self._signal)
        self._started = True

    async def close(self) -> None:
        """释放资源（幂等）。"""
        if not self._started:
            return
        await _lifecycle_close(self._signal)
        client = self._client
        if client is not None:
            await client.stop()
        if self._carrier is not None:
            await _lifecycle_close(self._carrier)
        self._started = False

    def _warn_if_trip_exceeds_ttl(self) -> None:
        """背压拒绝（consumption-backpressure）：启动期 TTL 预算校验。"""
        carrier = self._carrier
        existence_ttl: int | None = (
            getattr(carrier, "existence_ttl_seconds", None) if carrier else None
        )
        if (
            self._backpressure.enabled
            and existence_ttl is not None
            and self._backpressure.trip_seconds >= existence_ttl
        ):
            logger.warning(
                "backpressure_config_trip_not_less_than_existence_ttl",
                trip_seconds=self._backpressure.trip_seconds,
                existence_ttl_seconds=existence_ttl,
            )

    # ---- 推入主链路 ----

    async def push(
        self,
        record: RecordT,
        *,
        force: bool = False,
        source: str = "unknown",
    ) -> PushResult:
        """推入单条记录：背压 → 判重 → Kafka 发送 → 发送结果钩子。

        force=True 跳过判重判定（防 duplicate 死循环；确认覆盖后的完整重推），
        成功后走载体的覆盖确认钩子。未 start() 直接抛 RuntimeError
        （装配错误，非运行时降级）。
        """
        if not self._started:
            raise RuntimeError("Producer is not started, call start() first")
        self._metrics.received.record()  # 入口总流量（含后续被拒的调用）
        log_ctx = (
            self._options.log_context(record)
            if self._options.log_context is not None
            else {}
        )
        received_at = datetime.now(timezone.utc)

        # ---- 背压拒绝：必须在判重占位/发送之前（拒绝期零写入）----
        snapshot = await self._signal.snapshot()
        if snapshot.rejecting:
            return self._backpressure_result(snapshot, log_ctx, source, force)

        # ---- 判重：检查 + 原子占位（force=true 跳过唯一性判定，防死循环）----
        verdict: PushResult | None = None
        if self._carrier is not None:
            verdict = await self._admit_verdict(
                record, log_ctx, source, force=force
            )
        if verdict is not None:
            return verdict

        return await self._send_and_accept(
            record, force=force, received_at=received_at,
            source=source, log_ctx=log_ctx,
        )

    async def health(self) -> ProducerHealthResponse:
        """健康快照（数据归框架，暴露方式归使用方）。"""
        return await collect_producer_health(
            self._client, self._carrier, metrics=self._metrics
        )

    # ---- 内部分支 ----

    def _guarantee(self) -> str | None:
        """判重保证强度（载体自报）；未开启判重时 None。"""
        if self._carrier is None:
            return None
        return str(getattr(self._carrier, "guarantee", "unknown"))

    def _backpressure_result(
        self,
        snapshot: BackpressureSnapshot,
        log_ctx: JsonObject,
        source: str,
        force: bool,
    ) -> PushResult:
        """背压拒绝（拒绝期零写入：不占位、不写共享存储、不写 Kafka）。"""
        self._metrics.backpressure_rejected.record()
        logger.warning(
            "ingest_rejected_backpressure",
            **log_ctx,
            source=source,
            force=force,
        )
        return PushResult.backpressure(
            snapshot.reason,
            retry_after=self._backpressure.retry_after_seconds,
            guarantee=self._guarantee(),
        )

    async def _admit_verdict(
        self,
        record: RecordT,
        log_ctx: JsonObject,
        source: str,
        *,
        force: bool,
    ) -> PushResult | None:
        """存在性校验 + 原子占位。返回 PushResult = duplicate/rejected 短路；
        None = 继续。force=True 时载体只做依赖预检、跳过唯一性判定。"""
        carrier = self._carrier
        if carrier is None:  # 调用点保证非 None
            return None
        decision = await carrier.admit(record, force=force)
        if decision.kind is DecisionKind.DUPLICATE:
            self._metrics.duplicate.record()
            summary = decision.summary or {}
            logger.info(
                "ingest_conflict",
                **log_ctx,
                source=source,
                existing_summary_keys=sorted(summary.keys()),
            )
            return PushResult.duplicate(summary, guarantee=self._guarantee())
        if decision.kind is DecisionKind.REJECT:
            info = decision.reject
            assert info is not None
            if info.log_event:
                logger.warning(
                    info.log_event, **log_ctx, source=source, **info.log_fields
                )
            return PushResult.rejected(info, guarantee=self._guarantee())
        return None

    async def _send_and_accept(
        self,
        record: RecordT,
        *,
        force: bool,
        received_at: datetime,
        source: str,
        log_ctx: JsonObject,
    ) -> PushResult:
        """Kafka 发送 → 失败 unavailable（on_send_failed，默认占位保留自愈）
        → force 摘要写 → 成功日志。"""
        routing_key = str(self._key(record)) if self._key is not None else None
        message = self._codec.encode(
            self._options.message_type,
            record.model_dump(mode="json"),
            _iso_z(received_at),
            source,
        )
        if not await self._produce(
            record, key=routing_key, message=message, log_ctx=log_ctx, source=source
        ):
            return PushResult.unavailable(
                "kafka_unavailable", guarantee=self._guarantee()
            )
        cache_updated = True
        if force and self._carrier is not None:
            cache_updated = await self._force_summary_write(record)

        logger.info(
            "ingest_request",
            **log_ctx,
            received_at=_iso_z(received_at),
            source=source,
            force=force,
        )
        return PushResult(
            kind=PushKind.ACCEPTED,
            guarantee=self._guarantee(),
            received_at=received_at,
            cache_updated=cache_updated,
        )

    async def _produce(
        self,
        record: RecordT,
        *,
        key: str | None,
        message: JsonObject,
        log_ctx: JsonObject,
        source: str,
    ) -> bool:
        """Kafka 发送 + 指标 + 发送结果钩子。False = 失败（已记录日志与钩子）。"""
        client = self._require_client()
        t0 = time.monotonic()
        try:
            await client.send(key=key, message=message)
        except Exception as e:
            self._metrics.produce_failure.record()
            logger.error(
                "kafka_send_failed",
                **log_ctx,
                error=str(e),
                source=source,
            )
            await self._notify_send_failed(record)
            return False
        self._metrics.produce_success.record()
        self._metrics.produce_latency.record_latency(
            (time.monotonic() - t0) * 1000.0  # send 调用到 broker 确认耗时（毫秒）
        )
        await self._notify_send_success(record)
        return True

    async def _notify_send_success(self, record: RecordT) -> None:
        """每次发送成功的通知钩子（best-effort：钩子异常不影响已成功的请求）。"""
        carrier = self._carrier
        if carrier is None:
            return
        hook = _optional_hook(carrier, "on_send_success")
        if hook is None:
            return
        try:
            await hook(record)
        except Exception as e:
            logger.warning("dedup_send_success_hook_failed", error=str(e))

    async def _notify_send_failed(self, record: RecordT) -> None:
        """发送失败钩子（best-effort：默认保留占位，实现可释放换取立即重推）。"""
        carrier = self._carrier
        if carrier is None:
            return
        hook = _optional_hook(carrier, "on_send_failed")
        if hook is None:
            return
        try:
            await hook(record)
        except Exception as e:
            logger.warning("dedup_send_failed_hook_failed", error=str(e))

    async def _force_summary_write(self, record: RecordT) -> bool:
        """force 路径摘要写（on_force_accepted）；False = 存储未反映本次记录。"""
        carrier = self._carrier
        if carrier is None:
            return True
        hook = _optional_hook(carrier, "on_force_accepted")
        if hook is None:
            return True
        return bool(await hook(record))

    def _require_client(self) -> _KafkaClient:
        """start() 完成前不可达；与 _KafkaClient 未启动语义一致。"""
        if self._client is None:
            raise RuntimeError("_KafkaClient is not started")
        return self._client


__all__ = [
    "DEFAULT_MESSAGE_TYPE",
    "DedupOptions",
    "Producer",
    "ProducerOptions",
]
