# Contributing

Thanks for your interest in contributing to streamgate!

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/{github_owner}/streamgate
cd streamgate
uv sync            # creates .venv and installs the project (editable) + dev tools
```

## Local checks

CI runs the same gates; please run them locally before opening a PR:

```bash
uv run ruff check .        # lint (correctness + import sorting)
uv run lint-imports        # architectural layering contract
uv run pyright             # static type check
uv build                   # wheel + sdist build
```

## Scope and conventions

- **Mechanisms vs. policies.** The package owns mechanisms (admission plumbing, delivery, backpressure, self-healing). Policy choices (entity/slot semantics, schemas, sink targets) belong to users. PRs should keep this boundary.
- **Architecture gate.** The layering contract in `pyproject.toml` (`[tool.importlinter]`) is enforced: higher layers may import lower layers, never the reverse. If your change legitimately needs a new edge, update the contract in the same PR and explain why.
- **Optional extras.** Database drivers, `redis` and `httpx` must never be imported at module top level; load them via `streamgate._optional.require_optional` at the point of use so a bare install keeps working.
- **Comments language.** Internal docstrings/comments are written in Chinese and kept that way. Docstrings on public API symbols (exported from `streamgate.__init__`) should be English-friendly.
- **No test suite yet.** The project currently ships without tests (top roadmap item). Until then, please include a minimal reproduction script (or a runnable snippet) with bug reports and behavioral PRs so reviewers can verify against real data.
- **Changelog.** User-visible changes must update `CHANGELOG.md`.

## Submitting

1. Fork / create a branch.
2. Make the change, run the four local checks above.
3. Open a pull request with a short description and (if applicable) the reproduction script.
