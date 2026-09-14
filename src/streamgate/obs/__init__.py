"""可观测性出口：结构化日志发射层（logging）与宿主侧 JSON 渲染（formatter）。"""

from streamgate.obs.formatter import JsonFormatter
from streamgate.obs.logging import RESERVED_RECORD_ATTRS

__all__ = [
    "JsonFormatter",
    "RESERVED_RECORD_ATTRS",
]
