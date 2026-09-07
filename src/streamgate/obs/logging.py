import json
import sys
from datetime import timezone

from loguru import logger as _logger
from loguru._handler import Message


def _json_sink(message: Message) -> None:
    record = message.record
    log_entry: dict[str, object] = {
        "timestamp": record["time"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "level": record["level"].name,
        "event": record["message"],
    }
    extra = record["extra"]
    for key, value in extra.items():
        if key not in log_entry:
            log_entry[key] = value
    sys.stderr.write(json.dumps(log_entry, ensure_ascii=False, default=str) + "\n")


def _text_sink(message: Message) -> None:
    record = message.record
    extra_str = " ".join(f"{k}={v}" for k, v in record["extra"].items())
    sys.stderr.write(
        f"{record['time'].astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')} "
        f"{record['level'].name} "
        f"{record['message']} "
        f"{extra_str}\n"
    )


def configure_logger(level: str = "INFO", fmt: str = "json") -> None:
    _logger.remove()
    sink = _json_sink if fmt == "json" else _text_sink
    # loguru 0.7 类型桩未覆盖 callable sink（运行时支持），按桩缺陷最小抑制
    _logger.add(sink, level=level)  # type: ignore[reportCallIssue]


logger = _logger

configure_logger()
