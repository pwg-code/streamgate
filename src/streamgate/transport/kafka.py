"""aiokafka 消费封装：拉取/手动提交/健康/lag。"""

import asyncio

from aiokafka import AIOKafkaConsumer
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.errors import CommitFailedError, KafkaConnectionError, KafkaError
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata, TopicPartition

from streamgate.obs.logging import logger

# 反序列化后的消息形态：value_deserializer 产 str（tombstone 为 None），
# key_deserializer 产 str | None。
KafkaRecord = ConsumerRecord[str | None, str | None]


class KafkaConsumerService:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        *,
        auto_offset_reset: str = "earliest",
        max_poll_records: int = 500,
        session_timeout_ms: int = 30000,
        max_poll_interval_ms: int = 300000,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._topic = topic
        if not self._topic:
            raise ValueError(
                "kafka topic is required: pass topic= or set KAFKA__TOPIC"
            )
        if not group_id:
            raise ValueError(
                "consumer group_id is required: pass group_id= "
                "or set CONSUMER__GROUP_ID"
            )
        self._auto_offset_reset = auto_offset_reset
        self._max_poll_records = max_poll_records
        self._session_timeout_ms = session_timeout_ms
        self._max_poll_interval_ms = max_poll_interval_ms
        self._consumer: AIOKafkaConsumer | None = None
        self._started: bool = False
        self._lag: int = 0

    async def start(self) -> None:
        if self._started:
            return
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset=self._auto_offset_reset,
            max_poll_records=self._max_poll_records,
            session_timeout_ms=self._session_timeout_ms,
            max_poll_interval_ms=self._max_poll_interval_ms,
            value_deserializer=lambda v: v.decode("utf-8") if v else v,
            key_deserializer=lambda k: k.decode("utf-8") if k else k,
        )
        try:
            await self._consumer.start()
        except Exception as e:
            event = (
                "kafka_connection_failed"
                if isinstance(e, KafkaConnectionError)
                else "kafka_start_failed"
            )
            logger.error(
                event,
                error=str(e),
                bootstrap_servers=self._bootstrap_servers,
            )
            await self._close_safely()
            raise
        self._started = True
        logger.info(
            "kafka_connected",
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            topic=self._topic,
        )

    async def _close_safely(self) -> None:
        if self._consumer is None:
            return
        try:
            await self._consumer.stop()
        except Exception as e:
            logger.debug("kafka_cleanup_error", error=str(e))
        finally:
            self._consumer = None
            self._started = False

    async def stop(self) -> None:
        if not self._started or self._consumer is None:
            return
        await self._close_safely()
        logger.info("kafka_disconnected")

    async def poll(self) -> list[KafkaRecord]:
        """拉取一批消息。返回 ConsumerRecord 列表。

        使用 getmany(timeout_ms=1000) 非阻塞拉取。
        空列表表示当前无消息。
        """
        if not self._started or self._consumer is None:
            raise RuntimeError("KafkaConsumerService is not started")
        try:
            result = await self._consumer.getmany(
                timeout_ms=1000,
                max_records=self._max_poll_records,
            )
        except KafkaError as e:
            logger.error("kafka_poll_failed", error=str(e))
            raise

        records: list[KafkaRecord] = []
        for _tp, msgs in result.items():
            for msg in msgs:
                records.append(msg)
        if records:
            logger.debug(
                "consumer_message_received",
                count=len(records),
                partition=records[-1].partition,
                offset=records[-1].offset,
            )
        return records

    async def commit(
        self,
        offsets: dict[TopicPartition, OffsetAndMetadata] | None = None,
    ) -> None:
        """手动提交 offset。

        Args:
            offsets: {TopicPartition: OffsetAndMetadata} 格式。
                     传 None 或空 dict 表示提交所有已消费但未提交的 offset。
        """
        if not self._started or self._consumer is None:
            raise RuntimeError("KafkaConsumerService is not started")
        try:
            if offsets:
                await self._consumer.commit(offsets)
            else:
                await self._consumer.commit()
            logger.debug("offset_committed")
        except CommitFailedError as e:
            logger.error("offset_commit_failed", error=str(e))
            raise

    async def check_health(self) -> bool:
        if not self._started or self._consumer is None:
            return False
        try:
            return await asyncio.wait_for(
                self._consumer._client.force_metadata_update(),
                timeout=2.0,
            )
        except Exception as e:
            logger.debug("health_check_failed", error=str(e))
            return False

    async def refresh_lag(self) -> None:
        """更新 lag 缓存（end_offsets - 已提交位置，按分区求和）。

        仅由消费循环周期调用（backlog 检查点），健康接口只读缓存值
        （零请求路径开销）。失败保持上次值（DEBUG 记日志）。
        """
        if not self._started or self._consumer is None:
            return
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return
            tps = list(assignment)
            ends = await self._consumer.end_offsets(tps)
            coordinator = self._consumer._coordinator
            if not isinstance(coordinator, GroupCoordinator):
                return
            positions = await coordinator.fetch_committed_offsets(tps)
            total = 0
            for tp, end in ends.items():
                pos = positions.get(tp) if positions else None
                committed_offset = pos.offset if pos is not None else -1
                base = max(committed_offset, 0)
                if end is not None and end > base:
                    total += end - base
            self._lag = total
        except Exception as e:
            logger.debug("lag_refresh_failed", error=str(e))

    @property
    def started(self) -> bool:
        return self._started

    @property
    def lag(self) -> int:
        return self._lag
