"""引擎工厂（读写池、超时钩子，唯一建引擎点）。"""

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from streamgate.config import DbConfig
from streamgate.obs.logging import logger


def make_query_timeout_hook(timeout_seconds: int):
    """SQLAlchemy connect 事件钩子：建连后设置 pyodbc 语句级查询超时（秒）。

    生产驱动（pyodbc 系 / aioodbc）通过 dbapi_conn.timeout 控制，
    超时后连接真正释放；SQLite 无此概念（dev 不适用）。
    aioodbc 异步引擎的 connect 事件拿到的是 AsyncAdapt 包装连接，
    timeout 需落在底层 driver_connection（aioodbc/pyodbc 真实连接）上，
    故优先取 driver_connection 再 setattr。
    aioodbc 0.5.x 的 Connection.timeout 是只读 property（无 setter），
    setattr 会抛 AttributeError；真实 pyodbc 连接存于 Connection._conn，
    SQLAlchemy 的 AsyncAdapt autocommit setter 亦穿透 _conn 设置（模式一致），
    故失败时穿透 _conn 再设一次。
    驱动差异防御：属性不存在/设置失败仅 WARN，不阻断建连；
    调用层 wait_for 仍是硬保障。
    """

    def _on_connect(dbapi_conn: object, _record: object) -> None:
        try:
            driver = getattr(dbapi_conn, "driver_connection", dbapi_conn)
            _set_driver_timeout(driver, timeout_seconds)
        except Exception as e:
            logger.warning("db_query_timeout_set_failed", error=str(e))

    return _on_connect


def _set_driver_timeout(conn: object, timeout_seconds: int) -> None:
    try:
        setattr(conn, "timeout", timeout_seconds)
    except AttributeError:
        underlying = getattr(conn, "_conn", None)
        if underlying is None:
            raise
        setattr(underlying, "timeout", timeout_seconds)


def _is_sqlite(config: DbConfig) -> bool:
    return "sqlite" in config.require_connection_string().lower()


def _create_engine(config: DbConfig, pool_size: int, max_overflow: int, timeout_seconds: int) -> AsyncEngine:
    """引擎创建单点：内置 sqlite/mssql 分支、池参数、timeout 钩子挂载。

    C7 装配校验：connection_string 缺失即在此抛错（含修复指引），
    对齐 KAFKA__TOPIC / CONSUMER__GROUP_ID 的"必填即启动失败"语义。
    """
    connection_string = config.require_connection_string()
    if _is_sqlite(config):
        return create_async_engine(
            connection_string,
            echo=config.echo,
            connect_args={"check_same_thread": False},
        )
    engine = create_async_engine(
        connection_string,
        echo=config.echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=config.pool_timeout_seconds,  # 池耗尽快速失败
        pool_recycle=3600,
        pool_pre_ping=True,
    )
    # 建连即设驱动语句超时（pyodbc 系 dbapi_conn.timeout），
    # 钩子内含驱动差异防御，失败仅 WARN 不阻断建连。
    if timeout_seconds > 0:
        event.listen(
            engine.sync_engine,
            "connect",
            make_query_timeout_hook(timeout_seconds),
        )
    return engine


def create_write_engine(config: DbConfig) -> AsyncEngine:
    """consumer 写库引擎（池参数见 DB__WRITE_POOL_* 配置键）。"""
    return _create_engine(
        config,
        pool_size=config.write_pool_size,
        max_overflow=config.write_pool_max_overflow,
        timeout_seconds=config.write_timeout_seconds,
    )


def create_read_engine(config: DbConfig) -> AsyncEngine:
    """ingest 读库引擎（冷实体回源专用）。"""
    return _create_engine(
        config,
        pool_size=config.read_pool_size,
        max_overflow=config.read_pool_max_overflow,
        timeout_seconds=config.query_timeout_seconds,
    )


def async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """统一 session factory 参数（expire_on_commit=False）。"""
    return async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
