"""Kafka 生产服务，带自愈监控。

启动时无论初始连接是否成功，都会拉起后台监控任务：周期性探活，
不健康时销毁旧 producer 实例（其内部 sender/client 可能已进入 fatal 状态）
并新建实例重连，覆盖启动期断线与运行期断线两种场景。
"""

import asyncio
import json
import time
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError

from streamgate.config import KafkaConfig
from streamgate.obs.logging import logger
from streamgate.protocols import JsonObject


class KafkaProducerService:
    def __init__(self, config: KafkaConfig, topic: str | None = None) -> None:
        self._config = config
        self._topic = topic or config.topic
        if not self._topic:
            raise ValueError(
                "kafka topic is required: set KafkaConfig.topic or pass topic explicitly"
            )
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
        self._rejected_requests: int = 0         # 当前掉线周期内 502 拒绝计数
        self._consecutive_failures: int = 0      # 连续 send 失败计数（防抖阈值用）

    async def start(self) -> None:
        """启动服务并拉起自愈监控任务。

        初始连接失败抛异常（调用方决定是否降级启动），但监控任务仍会继续重连。
        """
        async with self._lock:
            if self._closed:
                raise RuntimeError("KafkaProducerService is closed")
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

    async def send(self, key: str, message: JsonObject) -> None:
        async with self._lock:
            if self._closed or not self._started or self._producer is None:
                self._rejected_requests += 1
                raise RuntimeError("KafkaProducerService is not started")
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
        if not ok:
            return False

        return True

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
