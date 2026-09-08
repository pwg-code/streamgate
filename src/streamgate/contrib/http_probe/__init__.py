"""HTTP 探活背压信号（extras：streamgate[http]）。

HttpProbeSignal（BackpressureSignal 协议实现）周期探活远端 consumer
健康接口，磁滞状态机（trip/recover 防抖）+ fail-closed；热路径零开销。
健康数据归框架 health_snapshot()，暴露方式归使用方（见
examples/http_probe/health_server.py 的零依赖演示端点）。
"""

from streamgate.contrib._deps import require_extra_import

try:
    from streamgate.contrib.http_probe.probe_signal import (
        HttpProbeSignal,
        HysteresisController,
    )
except ModuleNotFoundError as e:
    require_extra_import(e)

__all__ = ["HysteresisController", "HttpProbeSignal"]
