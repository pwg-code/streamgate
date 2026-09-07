"""可选 extras 依赖装载点（惰性导入，C6 门禁基础设施）。

分层约定：redis / httpx 及各类数据库驱动是"可选策略实现"的载体，
一律不进入模块顶层 import；使用点经本模块校验后再局部导入，
裸装（无任何 extras）时 `import streamgate` 零第三方可选依赖触达。

对外契约：缺失 extras 时抛出带安装指引的 ImportError
（而非裸 ModuleNotFoundError），指引文案与 pyproject 的
[project.optional-dependencies] 保持一致。
"""

import importlib.util
from importlib import import_module

# 顶层依赖名 → 安装指引（extra 名与 pyproject 可选依赖组逐字一致）
_EXTRA_HINTS: dict[str, str] = {
    "redis": "streamgate[redis]",
    "httpx": "streamgate[http-probe]",
    "aiosqlite": "streamgate[sqlite]",
    "aioodbc": "streamgate[mssql]",
}


def require_optional(dotted: str) -> None:
    """校验可选依赖可用，缺失即抛 ImportError（含 pip 安装指引）。

    dotted 为目标模块点路径（如 "redis.asyncio"），指引按顶层包名匹配。
    通过校验后由调用方在局部作用域执行真实 import（保留完整类型推导）。
    """
    top = dotted.split(".", 1)[0]
    if importlib.util.find_spec(top) is None:
        hint = _EXTRA_HINTS.get(top)
        guidance = f"Install it with: pip install {hint}" if hint else ""
        raise ImportError(
            f"Optional dependency '{top}' is required for this feature but is not "
            f"installed. {guidance}".rstrip()
        )
    import_module(dotted)
