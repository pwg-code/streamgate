"""背压信号内置实现（零 I/O）。

BackpressureSignal 协议（protocols.py）定义判定契约；核心只内置零 I/O 的
静态开关实现。动态拓扑信号（HTTP 探针 + 积压磁滞 trip/recover 状态机等）
由使用方注入，可复制参考实现见 examples/http_probe/。

热路径零开销：请求路径只读 rejecting 布尔属性，单实例、单 event loop。
"""

from streamgate.protocols import AllowAllSignal, BackpressureSnapshot


class ManualBackpressureSignal:
    """纯内存静态开关（未注入信号时的默认值）：默认不背压。

    直接赋值实例属性即可人工翻转（非线程安全，单 event loop 内使用）：
        signal = ManualBackpressureSignal()
        signal.rejecting = True
        signal.reject_reason = "manual_maintenance"

    测试与人工干预（发布窗口/维护期拒绝）场景用；生产动态信号
    请注入自定义 BackpressureSignal（参考实现见 examples/http_probe/）。
    """

    def __init__(
        self, *, rejecting: bool = False, reason: str | None = None
    ) -> None:
        self.rejecting: bool = rejecting
        self.reject_reason: str | None = reason

    async def snapshot(self) -> BackpressureSnapshot:
        return BackpressureSnapshot(
            rejecting=self.rejecting, reason=self.reject_reason
        )

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None


__all__ = ["AllowAllSignal", "ManualBackpressureSignal"]
