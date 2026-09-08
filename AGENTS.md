# AGENTS.md

## 架构与编码规范

**类型与 Pydantic v2**：
- 所有函数/方法必须标注参数与返回值类型，禁止省略。
- 禁用 `Any`、裸 `dict` / `list`（无泛型）等无法推断具体结构的类型；类型不确定时用具体类型或 `object` + `isinstance` 守卫。所有结构化数据必须定义对应的 Schema（Pydantic Model / dataclass / TypedDict）。
- 使用 Python 3.12+ 语法：`A | B`（代替 `Union`）、`list[int]`（代替 `List[int]`）；可能返回空值的函数必须标注 `| None`。
- 单函数体不超过 50 行，复杂逻辑拆分为带类型注解的子函数；生成代码时先输出数据模型定义，再输出业务逻辑。
- 禁止手动逐字段映射：用 `model_validate()`（ORM 对象加 `from_attributes=True`）代替 `_to_response()` 和 `Schema(**obj)`；用 `model_dump(exclude=.../exclude_unset=True)` 代替手写字典；字段定义只出现在 Schema 一处。响应体 Schema 有默认值，请求体 Schema 保持必填。
- 新增代码必须通过 pyright 静态检查。

## 发布流程（PyPI）

- PyPI Trusted Publisher 已配置（owner=pwg-code, repo=streamgate, workflow=release.yml, environment=pypi）。发布一律走 GitHub Actions 自动流程，禁止本地 `uv publish`/`twine upload`。
- 步骤：更新 `src/streamgate/__init__.py` 的 `__version__` 与 `CHANGELOG.md`（两者必须与 tag 一致，workflow 会校验）→ commit → `git tag vX.Y.Z` → `git push origin main vX.Y.Z` → tag 推送自动触发 `.github/workflows/release.yml` 完成构建与发布。
- 本地只做 `uv build` 验证，不承担发布职责。


