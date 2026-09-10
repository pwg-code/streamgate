"""streamgate 组件配置对象（每组件独立）。

配置键名与既有环境变量逐字对应（KAFKA__* / CONSUMER__* / BACKPRESSURE__*）。
1.0.0 起消费侧配置不再经配置对象装配：Consumer 直接以一等参数构造
（bootstrap_servers/topic/group_id 未传时回退同名环境变量；调优项收口在
ConsumerOptions.tuning / options.dlq，同样跟随 CONSUMER__* 环境变量回退）。
DB/Redis 是使用方的世界：连接配置由使用方/示例自带（不进框架核心）。
"""

from pydantic import BaseModel, field_validator


class KafkaConfig(BaseModel):
    bootstrap_servers: str = "kafka:9092"
    # 必填：producer/consumer 装配时校验（缺失即启动失败，不做项目专属默认）
    topic: str | None = None
    acks: str = "all"
    request_timeout_ms: int = 10000
    enable_idempotence: bool = True
    # 自愈监控：周期性探活，不健康时销毁旧实例重建连接（覆盖启动期/运行期断线）
    health_check_interval_seconds: float = 30.0   # 探活轮询周期
    reconnect_base_backoff_seconds: float = 1.0   # 重连指数退避基数
    reconnect_max_backoff_seconds: float = 30.0   # 重连退避上限
    # R1 防抖：连续 send 失败 N 次才触发重建（默认 1=首次失败即重建；
    # broker 秒级抖动时可调高减少误重建）
    reconnect_failure_threshold: int = 1
    # 探活口径：近窗口内有 send 失败即判定不健康（秒）
    send_failure_window_seconds: float = 30.0
    # R3 周期分级：异常/重建中高频探活间隔（秒）；健康态仍用 health_check_interval_seconds
    unhealthy_check_interval_seconds: float = 5.0

    @field_validator("bootstrap_servers", mode="before")
    @classmethod
    def strip_url_scheme(cls, v: object) -> object:
        if not isinstance(v, str):
            return v
        cleaned: list[str] = []
        for part in v.split(","):
            part = part.strip()
            for scheme in ("http://", "https://", "kafka://"):
                if part.lower().startswith(scheme):
                    part = part[len(scheme):]
                    break
            cleaned.append(part)
        return ",".join(cleaned)


class BackpressureConfig(BaseModel):
    """ingest 背压拒绝配置（consumption-backpressure）。

    默认值与 existence TTL 联动：trip = existence_ttl/2 = 9000s（漏报窗口打开前拦截，
    与积压告警线 backlog_age_warn 一致）；recover = trip*80% = 7200s（磁滞防抖）。
    """

    enabled: bool = True                                # 总开关；False=完全放行（紧急回滚）
    consumer_health_url: str = "http://localhost:9109/health"  # consumer 健康接口地址
    check_interval_seconds: float = 30.0                # 轮询周期（与积压检查周期一致）
    timeout_seconds: float = 2.0                        # 单次探活超时
    probe_retries: int = 3                              # 单周期内探活重试次数（不含首次）；0=禁用
    probe_retry_interval_seconds: float = 15.0          # 重试间隔（秒）
    trip_seconds: float = 9000.0                        # 触发阈值：积压超此值开始拒绝
    recover_seconds: float = 7200.0                     # 恢复阈值：积压回落至此值以下放行（磁滞）
    retry_after_seconds: int = 60                       # 拒绝时 Retry-After 头（秒）
    fail_closed_on_unreachable: bool = True             # 探活不可达时是否拒绝（fail-closed）
    reject_on_any_degraded: bool = False                # 旧行为逃生门：任何 degraded 即拒绝（恢复全量拒绝语义）
    # R3 背压周期分级：REJECTING 态高频探活间隔（秒）；OPEN 态仍用 check_interval_seconds
    unhealthy_check_interval_seconds: float = 5.0


class MetricsConfig(BaseModel):
    """健康快照速率指标配置（滑动窗口长度，ingest/consume 两侧共用）。"""

    window_seconds: int = 60

    @field_validator("window_seconds")
    @classmethod
    def check_window_seconds(cls, v: int) -> int:
        if not 1 <= v <= 600:
            raise ValueError(
                "window_seconds out of range; "
                "set METRICS__WINDOW_SECONDS to a value between 1 and 600"
            )
        return v
