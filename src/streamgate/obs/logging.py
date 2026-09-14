"""stdlib logging 发射层：库只发日志记录、永不配置（配置权归宿主）。

各模块经 ``logging.getLogger(__name__)`` 取得层级命名空间 logger
（如 ``streamgate.ingest.producer``），宿主用
``logging.getLogger("streamgate")`` 即可整树控制级别与 handler。
``emit`` 是内部发射辅助：保持调用点单行形态，并统一防御
（结构化字段名与 LogRecord 保留属性冲突时 fail-fast）。
本模块不触碰任何 logging 全局状态（无 handler 增删、无级别修改）。
``RESERVED_RECORD_ATTRS`` 是保留属性集合的唯一来源：发射侧据此 fail-fast，
渲染侧（formatter.JsonFormatter）据此做"跳过"容错，两处禁止各自定义。
"""

import logging
from typing import Final

_LEVELS: Final[dict[str, int]] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

# LogRecord 实例属性 + 格式化期追加属性（asctime/message/exc_text）：
# extra 携带这些键会在 stdlib 内部炸 KeyError，这里提前 fail-fast 并给出可读错误
RESERVED_RECORD_ATTRS: Final[frozenset[str]] = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
) | {"asctime", "exc_text", "message", "taskName"}


def emit(logger: logging.Logger, level: str, event: str, /, **fields: object) -> None:
    """发射一条结构化日志记录：event 为事件名（对外契约，勿改语义），fields 平铺进 extra。"""
    conflicts = fields.keys() & RESERVED_RECORD_ATTRS
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise ValueError(
            f"log field conflicts with LogRecord reserved attribute(s): {names}; "
            "rename the field, e.g. add a 'sg_' prefix"
        )
    logger.log(_LEVELS[level], event, extra=fields)
