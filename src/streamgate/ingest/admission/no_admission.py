"""NoAdmission：默认准入策略（纯 append-only 管道，无 Redis 依赖）。"""

from typing import Generic

from streamgate.protocols import Decision, RecordT


class NoAdmission(Generic[RecordT]):
    """默认策略：不做唯一性校验、不占位；on_persisted 为空操作。"""

    async def admit(self, record: RecordT, *, overwrite: bool = False) -> Decision:
        return Decision.allow()

    async def on_send_success(self, record: RecordT) -> None:
        return None

    async def on_send_failed(self, record: RecordT) -> None:
        return None

    async def on_overwrite_accepted(self, record: RecordT) -> bool:
        return True

    async def on_accepted(self, record: RecordT) -> bool:
        """向后兼容别名：等价 on_overwrite_accepted。"""
        return await self.on_overwrite_accepted(record)

    async def on_persisted(self, record: RecordT) -> None:
        return None

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def check_cache_health(self) -> bool:
        return False

    async def check_backfill_health(self) -> bool:
        return False

    async def check_backfill_health_detail(self) -> tuple[bool, str | None]:
        return False, "no backfill source (NoAdmission)"

    @property
    def existence_ttl_seconds(self) -> int | None:
        return None
