"""MetricsSink 协议 + 日志指标默认实现。

指标默认出口即结构化日志事件（与业务日志同一 json/text sink），
可注入任意监控系统适配器（StatsD/Prometheus push 等）。
事件名是公共契约：版本化演进，勿在补丁版本文案内改语义。
"""

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
