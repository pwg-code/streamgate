"""MetricsSink 协议 + 日志指标默认实现。

指标默认出口即结构化日志事件（与业务日志同一 json/text sink），
可注入任意监控系统适配器（StatsD/Prometheus push 等）。
事件名是公共契约：版本化演进，勿在补丁版本文案内改语义。
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from streamgate.obs.logging import logger


class MetricsSink(Protocol):
    def event(self, name: str, level: str = "info", /, **fields: object) -> None: ...


class LoggingMetricsSink:
    """默认实现：把指标事件按级别写入结构化日志。"""

    def event(self, name: str, level: str = "info", /, **fields: object) -> None:
        fn = {
            "debug": logger.debug,
            "info": logger.info,
            "warning": logger.warning,
            "error": logger.error,
        }.get(level, logger.info)
        fn(name, **fields)


DEFAULT_METRICS = LoggingMetricsSink()


@dataclass
class _WindowBucket:
    """单秒聚合桶：计数 + 延迟 sum/max（计数窗只用 count，延迟窗全用）。"""

    second: int
    count: int = 0
    latency_sum_ms: float = 0.0
    latency_max_ms: float = 0.0


class RateWindow:
    """分桶滑动窗口（1 秒 1 桶）：一种指标一个实例（计数型或延迟型）。

    纯内存、无锁、无 I/O（asyncio 单事件循环语义）；桶惰性老化
    （读/写时弹出窗口外旧桶），内存上界 O(window_seconds) 恒定。
    时间源 time.monotonic（防时钟回拨）。读取只读不重置。
    """

    def __init__(self, window_seconds: int = 60) -> None:
        self._window_seconds = window_seconds
        self._buckets: deque[_WindowBucket] = deque()
        self._created_monotonic = time.monotonic()  # 速率分母锚点

    def record(self, count: int = 1) -> None:
        """计数事件写入当前秒桶。"""
        self._append(count, None)

    def record_latency(self, duration_ms: float) -> None:
        """延迟事件：等价 record(1) + 累加 sum、刷新 max。"""
        self._append(1, duration_ms)

    def rate_per_second(self) -> float:
        """窗口内 count 总和 ÷ min(窗口秒数, 存活秒数)；无事件返回 0.0。

        存活秒数以实例创建时刻为锚点：启动初期不被空桶稀释。
        """
        total = sum(bucket.count for bucket in self._live_buckets())
        if total == 0:
            return 0.0
        # 存活秒数下限 1.0：与 1 秒桶粒度对齐，防创建瞬间除零
        elapsed = max(time.monotonic() - self._created_monotonic, 1.0)
        return total / min(float(self._window_seconds), elapsed)

    def latency_avg_ms(self) -> float:
        """窗口内延迟均值 Σsum ÷ Σcount；无样本返回 0.0。"""
        buckets = self._live_buckets()
        total_count = sum(bucket.count for bucket in buckets)
        if total_count == 0:
            return 0.0
        return sum(bucket.latency_sum_ms for bucket in buckets) / total_count

    def latency_max_ms(self) -> float:
        """窗口内各存活桶 max 的最大值；无样本返回 0.0。"""
        buckets = self._live_buckets()
        if not buckets:
            return 0.0
        return max(bucket.latency_max_ms for bucket in buckets)

    def _append(self, count: int, latency_ms: float | None) -> None:
        """写入当前秒桶；跨秒追加新桶并弹出窗口外旧桶（惰性老化）。"""
        now = int(time.monotonic())
        buckets = self._buckets
        if not buckets or buckets[-1].second != now:
            buckets.append(_WindowBucket(now))
            self._evict_expired(now)
        tail = buckets[-1]
        tail.count += count
        if latency_ms is None:
            return
        tail.latency_sum_ms += latency_ms
        if latency_ms > tail.latency_max_ms:
            tail.latency_max_ms = latency_ms

    def _evict_expired(self, now: int) -> None:
        """弹出窗口外旧桶（保留当前秒在内共 window_seconds 个）。"""
        floor = now - self._window_seconds + 1
        buckets = self._buckets
        while buckets and buckets[0].second < floor:
            buckets.popleft()

    def _live_buckets(self) -> list[_WindowBucket]:
        """读取侧同步老化：长时间无写入后残留的窗口外旧桶不参与聚合。"""
        floor = int(time.monotonic()) - self._window_seconds + 1
        buckets = self._buckets
        while buckets and buckets[0].second < floor:
            buckets.popleft()
        return list(buckets)


class IngestMetrics:
    """ingest 侧滑动窗口指标容器（构造时统一窗长）。"""

    def __init__(self, window_seconds: int = 60) -> None:
        self.received = RateWindow(window_seconds)
        self.admission_conflict = RateWindow(window_seconds)
        self.backpressure_rejected = RateWindow(window_seconds)
        self.produce_success = RateWindow(window_seconds)
        self.produce_failure = RateWindow(window_seconds)
        self.produce_latency = RateWindow(window_seconds)


class ConsumeMetrics:
    """consume 侧滑动窗口指标容器（构造时统一窗长）。"""

    def __init__(self, window_seconds: int = 60) -> None:
        self.consumed = RateWindow(window_seconds)
        self.handled = RateWindow(window_seconds)
        self.handle_failed = RateWindow(window_seconds)
        self.retries = RateWindow(window_seconds)
        self.handle_latency = RateWindow(window_seconds)


__all__ = ["ConsumeMetrics", "IngestMetrics", "RateWindow"]
