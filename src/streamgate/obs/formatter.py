"""内置 JSON Formatter：宿主开箱即用的结构化日志渲染。

3.0.0 移除 loguru 后，``emit()`` 经 ``extra=fields`` 发出的结构化字段仍在
LogRecord 上，但 stdlib 默认 Formatter 不渲染 extra——字段"看不见"。
本模块补齐渲染侧：每条日志渲染为一行 JSON（基础字段 + extra 平铺 +
exc_info 兜底）。定位是通用组件：任何 logger（含宿主自身的）经它渲染
都正确；本模块同样不做任何 logging 全局配置（挂载方式归宿主）。
"""

import json
import logging
from datetime import datetime, timezone
from typing import Final

from streamgate.obs.logging import RESERVED_RECORD_ATTRS

_BASE_FIELDS: Final[frozenset[str]] = frozenset(
    {"timestamp", "level", "logger", "event"}
)


def _iso_utc(epoch_seconds: float) -> str:
    """LogRecord 时间戳 → UTC ISO8601（毫秒精度，Z 后缀）。

    必须取自 LogRecord 时钟（record.created）而非 format 时刻，避免漂移。
    """
    moment = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonFormatter(logging.Formatter):
    """stdlib ``logging.Formatter`` 子类：每条日志渲染为一行 JSON。

    字段布局：``timestamp``（UTC ISO8601，Z 后缀）/ ``level`` /
    ``logger``（层级命名空间）/ ``event``（``record.getMessage()``，
    兼容宿主 ``%s`` 风格日志），其余键从 ``record.__dict__`` 剔除保留
    属性后平铺（即 ``emit(extra=...)`` 注入的全部结构化字段）；
    ``record.exc_info`` 存在时附 ``error``（formatException 输出）。
    序列化：``ensure_ascii=False``（中文原样）、``default=str`` 兜底。
    与发射侧 emit 的 fail-fast 不同层：遇保留属性撞键直接跳过、不抛错
    （渲染侧必须容错——宿主自己的 extra 不受 emit 保护）。
    """

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "timestamp": _iso_utc(record.created),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in RESERVED_RECORD_ATTRS or key in _BASE_FIELDS:
                continue
            entry[key] = value
        if record.exc_info is not None:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)
