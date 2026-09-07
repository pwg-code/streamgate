"""默认消息信封编解码：JSON + 可选版本号 v。

信封结构：{"type": ..., "v": "1"(可选), "received_at": ..., "source": ..., "data": {...}}
- v 缺省不写入（保持既有线格式不变）；启用版本戳后，未知 type 的消息走 DLQ 不 crash；
- 消费端 decode 只提取 data 与元数据，未知字段向前兼容。
"""

import json

from streamgate.obs.logging import logger
from streamgate.protocols import Envelope, JsonObject

ENVELOPE_VERSION = "1"


class JsonEnvelopeCodec:
    def __init__(self, *, stamp_version: bool = False) -> None:
        self._stamp_version = stamp_version

    def encode(
        self,
        message_type: str,
        data: JsonObject,
        received_at: str,
        source: str,
    ) -> JsonObject:
        message: JsonObject = {
            "type": message_type,
            "received_at": received_at,
            "source": source,
            "data": data,
        }
        if self._stamp_version:
            message["v"] = ENVELOPE_VERSION
        return message

    def decode(self, raw: str | None) -> Envelope | None:
        """解析 Kafka 消息 JSON，提取 data 载荷。None = 毒丸。

        raw 为 None（tombstone）时 json.loads 抛 TypeError，同样按毒丸处理。
        """
        try:
            if raw is None:
                # tombstone（value=None）历史上经 json.loads 抛 TypeError 走毒丸
                logger.error("poison_message_skipped", error="value is None (tombstone)")
                return None
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                return None
            data = msg.get("data")
            if not isinstance(data, dict):
                return None
            version = msg.get("v")
            return Envelope(
                data=data,
                type=str(msg["type"]) if "type" in msg else None,
                version=str(version) if version is not None else None,
                raw=raw,
            )
        except (json.JSONDecodeError, TypeError) as e:
            logger.error("poison_message_skipped", error=str(e))
            return None
