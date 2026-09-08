"""contrib 依赖守卫：缺 extra 时的 ModuleNotFoundError 转为带安装指引的报错。"""

from typing import NoReturn

_EXTRA_BY_MODULE: dict[str, str] = {
    "redis": "streamgate[redis]",
    "sqlalchemy": "streamgate[sql]",
    "sqlmodel": "streamgate[sql]",
    "aiosqlite": "streamgate[sql]",
    "aioodbc": "streamgate[sql]",
    "httpx": "streamgate[http]",
}


def require_extra_import(error: ModuleNotFoundError) -> NoReturn:
    """缺依赖时抛带安装指引的 ImportError；与本层无关的缺失原样抛出。"""
    name = error.name or ""
    extra = _EXTRA_BY_MODULE.get(name.split(".")[0])
    if extra is None:
        raise error
    raise ImportError(
        f"streamgate.contrib requires missing package '{name}'. "
        f"Install it with: pip install {extra}"
    ) from error
