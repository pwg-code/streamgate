"""InMemoryDedupCarrier：内置进程内判重载体（单进程/测试/低吞吐场景）。

零外部 I/O 的内置实现：身份键 → summary 的进程内字典，admit 时原子
检查+占位（单 event loop 内无 await 竞态窗口）。

适用边界（务必遵守）：
- 仅单进程有效（guarantee="process-local"）。内存不共享，多实例部署下
  判重失效——生产多副本拓扑请注入共享存储载体（Redis 参考实现见
  streamgate.contrib.redis_dedup）。
- 无回源；ttl_seconds 未配置时条目随进程生命周期存续（重启即清空），
  配置后按存活预算惰性过期。
"""

import time
from collections.abc import Callable
from typing import Generic, TypeVar

from pydantic import BaseModel

from streamgate.protocols import Decision, JsonObject

RecordModelT = TypeVar("RecordModelT", bound=BaseModel)


class InMemoryDedupCarrier(Generic[RecordModelT]):
    """唯一性契约的进程内实现：检查 + 原子占位 + 摘要刷新。

    框架保证调用时序：admit → (Kafka 发送) → on_send_success（每次成功，通知型）/
    on_send_failed（每次失败，默认保留占位自愈）→ on_force_accepted（仅
    force 路径）；消费侧落库成功 → on_persisted（权威刷新）。
    """

    def __init__(
        self,
        key: Callable[[RecordModelT], str],
        summary: Callable[[RecordModelT], JsonObject] | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        self._key = key
        self._summary = summary
        self._ttl_seconds = ttl_seconds
        # 身份键 → (摘要, 过期时刻 monotonic)；None = 不过期（随进程存续）
        self._store: dict[str, tuple[JsonObject, float | None]] = {}

    # ---- DedupCarrier 协议 ----

    @property
    def guarantee(self) -> str:
        return "process-local"

    async def admit(self, record: RecordModelT, *, force: bool = False) -> Decision:
        if force:
            # 确认覆盖后的完整重推：跳过唯一性判定（摘要由 on_force_accepted 刷新）
            return Decision.allow()
        identity = str(self._key(record))
        entry = self._store.get(identity)
        if entry is not None and not self._expired(entry[1]):
            return Decision.duplicate(entry[0])
        if entry is not None:
            del self._store[identity]  # 过期条目惰性清除
        self._store[identity] = (self._summarize(record), self._expires_at())
        return Decision.allow()

    async def on_send_success(self, record: RecordModelT) -> None:
        """通知型钩子：占位已在 admit 写入，无需动作。"""
        return None

    async def on_send_failed(self, record: RecordModelT) -> None:
        """默认保留占位（自愈）；需"失败立即可重推"可在此删除 _store 键。"""
        return None

    async def on_force_accepted(self, record: RecordModelT) -> bool:
        """force 路径摘要写（内存操作恒成功）。"""
        self._store[str(self._key(record))] = (
            self._summarize(record),
            self._expires_at(),
        )
        return True

    async def on_persisted(self, record: RecordModelT) -> None:
        """落库成功后的权威摘要刷新（幂等）。"""
        self._store[str(self._key(record))] = (
            self._summarize(record),
            self._expires_at(),
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
        return False, "no backfill source (InMemoryDedupCarrier)"

    @property
    def existence_ttl_seconds(self) -> int | None:
        return self._ttl_seconds

    # ---- 内部工具 ----

    def _summarize(self, record: RecordModelT) -> JsonObject:
        return self._summary(record) if self._summary is not None else {}

    def _expires_at(self) -> float | None:
        if self._ttl_seconds is None:
            return None
        return time.monotonic() + self._ttl_seconds

    @staticmethod
    def _expired(expires_at: float | None) -> bool:
        return expires_at is not None and time.monotonic() >= expires_at


__all__ = ["InMemoryDedupCarrier"]
