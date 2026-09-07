"""InMemoryAdmission：纯内存唯一性准入（单进程/测试/低吞吐场景）。

零外部 I/O 的内置准入实现：entity+slot → summary 的进程内字典，
admit 时原子检查+占位（单 event loop 内无 await 竞态窗口）。

适用边界（务必遵守）：
- 仅单进程有效。内存不共享，多实例部署下去重失效——生产多副本拓扑
  请注入共享存储载体（Redis 参考实现见 examples/redis_admission/）。
- 无 TTL：条目随进程生命周期存续（重启即清空），不做 idle GC。
"""

from collections.abc import Callable
from typing import Generic

from streamgate.protocols import Decision, JsonObject, RecordT


class InMemoryAdmission(Generic[RecordT]):
    """唯一性契约的进程内实现：检查 + 原子占位 + 摘要刷新。

    框架保证调用时序：admit → (Kafka 发送) → on_accepted（仅 overwrite 路径）；
    消费侧落库成功 → on_persisted（权威刷新）。
    """

    def __init__(
        self,
        entity_key: Callable[[RecordT], str],
        slot_key: Callable[[RecordT], str],
        summary: Callable[[RecordT], JsonObject],
    ) -> None:
        self._entity_key = entity_key
        self._slot_key = slot_key
        self._summary = summary
        self._store: dict[tuple[str, str], JsonObject] = {}

    # ---- AdmissionPolicy 协议 ----

    async def admit(self, record: RecordT, *, overwrite: bool = False) -> Decision:
        if overwrite:
            # 409 确认后的完整重发：跳过唯一性判定（摘要由 on_accepted 刷新）
            return Decision.allow()
        key = (str(self._entity_key(record)), str(self._slot_key(record)))
        existing = self._store.get(key)
        if existing is not None:
            return Decision.conflict(existing)
        self._store[key] = self._summary(record)
        return Decision.allow()

    async def on_accepted(self, record: RecordT) -> bool:
        """overwrite 路径摘要写（内存操作恒成功）。"""
        self._store[(str(self._entity_key(record)), str(self._slot_key(record)))] = (
            self._summary(record)
        )
        return True

    async def on_persisted(self, record: RecordT) -> None:
        """落库成功后的权威摘要刷新（幂等）。"""
        self._store[(str(self._entity_key(record)), str(self._slot_key(record)))] = (
            self._summary(record)
        )

    # ---- 生命周期与健康探测 ----

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def check_cache_health(self) -> bool:
        return True  # 进程内存储，随进程存活

    async def check_backfill_health(self) -> bool:
        return False

    async def check_backfill_health_detail(self) -> tuple[bool, str | None]:
        return False, "no backfill source (InMemoryAdmission)"

    @property
    def existence_ttl_seconds(self) -> int | None:
        return None


__all__ = ["InMemoryAdmission"]
