"""HttpProbeSignal：周期探活远端 consumer 健康接口的背压信号（streamgate.contrib 正式功能）。

判定语义（consumption-backpressure）：
- 触发：backlog_age_seconds > trip_seconds
- 恢复：backlog_age_seconds < recover_seconds（磁滞防抖）
- fail-closed：探活不可达（重试耗尽后仍失败）按"积压超额"处理
- kafka 组件断开时指标失效（consumer 不再排空，Kafka 侧积压不可见），
  立即拒绝；database/redis 降级不单独触发拒绝，交由 age 磁滞判定
  （异步缓冲架构主场：存储堵塞时 age 如实增长，容忍窗内持续接收）
- reject_on_any_degraded=true 恢复全量拒绝语义（任何 degraded 即拒绝，逃生门）

拒绝原因细分（消除磁滞带锁死）：非 age 触发的拒绝（kafka_down/unreachable）
在探活恢复后按当次 age 重新分类，磁滞带内直接 OPEN。

热路径零开销：请求路径只读 rejecting 布尔属性，探活仅发生在后台协程。
单实例、单 event loop，布尔读写无竞态。
"""

import asyncio
import time

import httpx

from streamgate import BackpressureConfig, logger
from streamgate.protocols import BackpressureSnapshot, ProbeResult


class HysteresisController:
    """磁滞状态机（OPEN <-> REJECTING）+ 周期评估循环，探测源由子类提供。"""

    def __init__(self, config: BackpressureConfig) -> None:
        self._config = config
        self._task: asyncio.Task[None] | None = None
        self._state = "OPEN"
        self._reject_reason: str | None = None
        self._rejecting_since: float | None = None  # REJECTING 起始 monotonic 时间戳
        self._last_probe: ProbeResult | None = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        """启动后台轮询协程。幂等；disabled 时仅做配置校验。"""
        self._validate_config()
        if self._task is not None:
            return
        await self._probe_start()
        if not self._config.enabled:
            logger.info(
                "backpressure_disabled",
                consumer_health_url=self._config.consumer_health_url,
            )
            return
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "backpressure_started",
            consumer_health_url=self._config.consumer_health_url,
            check_interval_seconds=self._config.check_interval_seconds,
            trip_seconds=self._config.trip_seconds,
            recover_seconds=self._config.recover_seconds,
            probe_retries=self._config.probe_retries,
        )

    async def close(self) -> None:
        """停协程 + 释放探测资源。幂等。"""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._probe_close()

    # ---- 探测源（子类实现）----

    async def _probe_start(self) -> None:
        return None

    async def _probe_close(self) -> None:
        return None

    async def _probe(self) -> ProbeResult | None:
        """单周期探测 + 重试；全部失败 -> None（不可达）。"""
        raise NotImplementedError

    # ---- 快照 ----

    async def snapshot(self) -> BackpressureSnapshot:
        probe = self._last_probe
        return BackpressureSnapshot(
            rejecting=self.rejecting,
            reason=self._reject_reason,
            backlog_age_seconds=probe.backlog_age_seconds if probe else None,
            pending_count=probe.pending_count if probe else 0,
            lag=probe.lag if probe else 0,
            status=probe.status if probe else "unknown",
            kafka=probe.kafka if probe else "unknown",
        )

    @property
    def rejecting(self) -> bool:
        return self._state == "REJECTING"

    @property
    def state(self) -> str:
        return self._state

    @property
    def reject_reason(self) -> str | None:
        return self._reject_reason

    # ---- 状态机 ----

    def _reset_to_open(self) -> None:
        """退出 REJECTING 态（状态与计时归位；日志由调用方发射）。"""
        self._state = "OPEN"
        self._reject_reason = None
        self._rejecting_since = None

    def _duration_seconds(self) -> float | None:
        """REJECTING 持续时长（未进入过 REJECTING 时为 None）。"""
        duration = self._rejecting_since
        if duration is None:
            return None
        return round(time.monotonic() - duration, 1)

    def _classify_age(self, age: float | None) -> str:
        """按积压时长给出目标状态分类（无磁滞语义，供重分类与常规判定共用）。

        返回 "open"（无积压/未超 trip）或 "backlog"（超 trip）。
        """
        if age is None:
            return "open"
        if age > self._config.trip_seconds:
            return "backlog"
        return "open"

    def _evaluate_unreachable(self) -> None:
        """fail-closed：不可达（重试耗尽）-> 拒绝（可配置放行逃生门）。"""
        if self._config.fail_closed_on_unreachable and self._state != "REJECTING":
            self._enter_rejecting("unreachable")

    def _evaluate_kafka_down(self, probe: ProbeResult) -> bool:
        """kafka 断开 -> 指标失效，立即拒绝（不依赖 age）。

        reject_on_any_degraded 逃生门：恢复全量拒绝语义。返回是否按本类处置。
        """
        kafka_down = probe.kafka != "connected" or (
            self._config.reject_on_any_degraded and probe.status != "healthy"
        )
        if kafka_down and (
            self._state != "REJECTING" or self._reject_reason != "kafka_down"
        ):
            self._enter_rejecting("kafka_down")
        return kafka_down

    def _evaluate_rejecting_recovery(self, age: float | None) -> bool:
        """非 age 触发的拒绝恢复：按当次 age 重新分类，磁滞带内直接 OPEN。

        （从未真实 trip 过，不受磁滞带卡死）返回是否已处置。
        """
        if not (self._state == "REJECTING" and self._reject_reason != "backlog"):
            return False
        if self._classify_age(age) == "backlog":
            self._enter_rejecting("backlog", age)
            return True
        recovered_from = self._reject_reason
        self._reset_to_open()
        logger.info(
            "backpressure_recovered",
            recovered_from=recovered_from,
            backlog_age_seconds=age,
            duration_seconds=self._duration_seconds(),
        )
        return True

    def _evaluate_no_backlog(self) -> None:
        """无积压 -> 放行（磁滞下也直接放开：无积压是明确的安全信号）。"""
        if self._state != "OPEN":
            self._reset_to_open()
            logger.info(
                "backpressure_recovered",
                backlog_age_seconds=None,
                duration_seconds=self._duration_seconds(),
            )

    def _evaluate_age(self, age: float, probe: ProbeResult) -> None:
        """有积压时的磁滞判定 + 探活观测日志。"""
        if self._state == "REJECTING":
            if age < self._config.recover_seconds:
                self._reset_to_open()
                logger.info(
                    "backpressure_recovered",
                    backlog_age_seconds=round(age, 1),
                    recover_seconds=self._config.recover_seconds,
                    duration_seconds=self._duration_seconds(),
                )
        elif age > self._config.trip_seconds:
            self._enter_rejecting("backlog", age)

        logger.debug(
            "backpressure_probe_ok",
            backlog_age_seconds=age,
            pending_count=probe.pending_count,
            lag=probe.lag,
            kafka=probe.kafka,
        )

    async def _evaluate(self) -> None:
        """单步判定。"""
        probe = await self._probe()
        self._last_probe = probe

        if probe is None:
            self._evaluate_unreachable()
            return

        if self._evaluate_kafka_down(probe):
            return

        age = probe.backlog_age_seconds
        if self._evaluate_rejecting_recovery(age):
            return
        if age is None:
            self._evaluate_no_backlog()
            return
        self._evaluate_age(age, probe)

    def _enter_rejecting(self, reason: str, age: float | None = None) -> None:
        self._state = "REJECTING"
        self._reject_reason = reason
        self._rejecting_since = time.monotonic()
        if reason == "backlog":
            logger.error(
                "backpressure_tripped",
                backlog_age_seconds=round(age, 1) if age is not None else None,
                trip_seconds=self._config.trip_seconds,
                consumer_health_url=self._config.consumer_health_url,
            )
        elif reason == "kafka_down":
            logger.warning(
                "backpressure_consumer_not_healthy",
                kafka="disconnected",
                consumer_health_url=self._config.consumer_health_url,
            )
        else:  # unreachable
            logger.error(
                "backpressure_probe_unreachable_rejecting",
                consumer_health_url=self._config.consumer_health_url,
            )

    async def _run_loop(self) -> None:
        """周期调用 _evaluate；异常不逃逸（循环永不死）。

        REJECTING 态高频探活（unhealthy_check_interval_seconds），
        OPEN 态低频（check_interval_seconds），减少 Kafka 恢复后感知延迟。
        """
        while True:
            try:
                await self._evaluate()
            except Exception as e:  # 理论不可达（_probe 已兜底），双保险
                logger.error("backpressure_evaluate_failed", error=str(e))
            if self._state == "REJECTING":
                await asyncio.sleep(self._config.unhealthy_check_interval_seconds)
            else:
                await asyncio.sleep(self._config.check_interval_seconds)

    # ---- 启动期配置不变式校验----

    def _validate_config(self) -> None:
        """WARN 级校验（不阻断启动）：trip < 存在性 TTL；recover < trip；retries >= 0。"""
        if self._config.recover_seconds >= self._config.trip_seconds:
            logger.warning(
                "backpressure_config_recover_not_less_than_trip",
                recover_seconds=self._config.recover_seconds,
                trip_seconds=self._config.trip_seconds,
            )
        if self._config.probe_retries < 0:
            logger.warning(
                "backpressure_config_negative_probe_retries",
                probe_retries=self._config.probe_retries,
            )


class HttpProbeSignal(HysteresisController):
    """周期探活远端 consumer 健康接口的背压信号（双进程拓扑）。

    用法：注入 IngestGateway(signal=HttpProbeSignal(config))。
    """

    def __init__(
        self,
        config: BackpressureConfig,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(config)
        self._injected_client = http_client is not None
        self._client = http_client  # start() 时若为 None 则自建

    async def _probe_start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._config.timeout_seconds)

    async def _probe_close(self) -> None:
        if self._client is not None and not self._injected_client:
            await self._client.aclose()
        if not self._injected_client:
            self._client = None

    async def _probe_once(self) -> ProbeResult | None:
        """单次 GET {consumer_health_url}；任何异常（超时/非200/解析失败）-> None。"""
        if self._client is None:
            return None
        try:
            resp = await self._client.get(self._config.consumer_health_url)
            resp.raise_for_status()
            payload = resp.json()
            age = payload.get("backlog_age_seconds")
            return ProbeResult(
                status=str(payload.get("status", "")),
                backlog_age_seconds=float(age) if age is not None else None,
                pending_count=int(payload.get("pending_count", 0) or 0),
                lag=int(payload.get("lag", 0) or 0),
                kafka=str(payload.get("kafka", "connected")),
            )
        except Exception as e:
            logger.warning(
                "backpressure_probe_attempt_failed",
                error=str(e),
                consumer_health_url=self._config.consumer_health_url,
            )
            return None

    async def _probe(self) -> ProbeResult | None:
        """探活 + 周期内重试（防抖）。

        总尝试次数 = 1 + probe_retries；全部失败 -> None（不可达）。
        """
        attempts = 1 + max(0, self._config.probe_retries)
        probe = await self._probe_once()
        attempt = 1
        while probe is None and attempt < attempts:
            await asyncio.sleep(self._config.probe_retry_interval_seconds)
            attempt += 1
            probe = await self._probe_once()
        if probe is not None and attempt > 1:
            logger.info(
                "backpressure_probe_recovered_after_retry",
                attempt=attempt,
                consumer_health_url=self._config.consumer_health_url,
            )
        elif probe is None:
            logger.error(
                "backpressure_probe_unreachable",
                attempts=attempts,
                consumer_health_url=self._config.consumer_health_url,
            )
        return probe


__all__ = ["HysteresisController", "HttpProbeSignal"]
