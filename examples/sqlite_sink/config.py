"""示例本地 DB 配置（原框架 DbConfig 的示例化收敛）。

DB 是使用方的世界：连接配置由使用方自带，不进框架核心。
仅保留 sqlite_sink 示例用到的字段；生产按需扩充（凭据走环境变量）。
"""

from pydantic import BaseModel


class DbConfig(BaseModel):
    """示例落库配置：连接串必填（缺失即装配失败，含修复指引）。"""

    connection_string: str | None = None
    echo: bool = False
    write_timeout_seconds: int = 20   # 驱动语句超时（MSSQL；须 <= write_wait_seconds）
    write_wait_seconds: int = 25      # write_batch 调用层 wait_for
    pool_timeout_seconds: int = 3     # 连接池获取超时（池耗尽快速失败）
    write_pool_size: int = 10         # 写池固定连接数
    write_pool_max_overflow: int = 20  # 写池溢出连接数

    def require_connection_string(self) -> str:
        """连接串装配校验（引擎创建单点调用）：None 即配置错误，含修复指引。"""
        if self.connection_string is None:
            raise ValueError(
                "connection_string is required: set DbConfig.connection_string "
                "(e.g. env DB_CONN='sqlite+aiosqlite:///./data/streamgate.db')"
            )
        return self.connection_string

    @property
    def dialect(self) -> str:
        """驱动方言标签（日志/分支用）：sqlite | mssql。"""
        return (
            "sqlite"
            if "sqlite" in self.require_connection_string().lower()
            else "mssql"
        )

    @property
    def redacted_connection_string(self) -> str:
        """隐藏密码后的连接串（日志用，避免凭据入日志）。"""
        s = self.require_connection_string()
        if "://" not in s or "@" not in s:
            return s
        scheme, _, rest = s.partition("://")
        userinfo, _, host = rest.rpartition("@")
        if ":" in userinfo:
            user, _, _ = userinfo.partition(":")
            userinfo = f"{user}:***"
        return f"{scheme}://{userinfo}@{host}"
