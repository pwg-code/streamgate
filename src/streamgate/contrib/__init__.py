"""streamgate.contrib：官方增强策略层（Beta / provisional）。

核心机制归 streamgate，I/O 策略实现归本层：全部随 wheel 发布、按后端 extras
携带依赖，装完即可 import，无需复制源码。分层契约（import-linter 强制）：
核心层不得 import 本层；本层可使用核心公共 API 与所在 extra 的第三方库。

子包与 extras 对照（缺依赖时 import 会给出安装指引）：
- redis_dedup：Redis 分布式判重载体（身份键原子占位 / 冷回源，guarantee="distributed"）
    → pip install streamgate[redis]
- sql_upsert / sqlite_upsert / mssql_upsert：SQL 幂等 upsert 出口（基座 + 双方言预装配工厂）
    → pip install streamgate[sql]
- http_probe：HTTP 探活背压信号（磁滞状态机，双进程拓扑）
    → pip install streamgate[http]

定位为 provisional：公开可用，但小版本内允许调整签名（降级成本低于核心 API）。
"""
