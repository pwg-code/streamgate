# Contributing

Thanks for your interest in contributing to streamgate!

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/pwg-code/streamgate
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

- **Mechanisms vs. policies.** The package owns mechanisms (admission orchestration, delivery, backpressure, self-healing). Policy choices (entity/slot semantics, schemas, storage carriers) belong to users. PRs should keep this boundary.
- **Core purity / layering gate.** The core layers must never import `streamgate.contrib`, and `import streamgate` must work with zero I/O third-party dependencies — enforced by the import-linter layered contract in `pyproject.toml` (`[tool.importlinter]`) plus the CI bare-install smoke. Official I/O strategy implementations belong in `streamgate.contrib.*` with their third-party dependencies declared as extras (`[redis]` / `[sql]` / `[http]`); a missing extra must raise an import error that names the extra (use `streamgate.contrib._deps.require_extra_import`). If you add a protocol extension point, add or update a contrib implementation and a runnable example for it in the same PR.
- **Contrib is provisional.** Contrib packages are public API at beta grade: signatures may adjust within minor releases, but behavior changes and deprecations must go through the CHANGELOG. Merged strategy code must keep behavior-preserving moves only (lint + pyright gates apply to it exactly like the core).
- **Examples are the storefront.** Code in `examples/` is held to the same quality bar as the package itself: CI runs lint and typecheck over it, and example READMEs must match reality. Demos import strategies from `streamgate.contrib` — never copy strategy source into `examples/`. Rotting examples rot the project.
- **Architecture gate.** The layering contract in `pyproject.toml` (`[tool.importlinter]`) is enforced: higher layers may import lower layers, never the reverse. If your change legitimately needs a new edge, update the contract in the same PR and explain why.
- **Comments language.** Internal docstrings/comments are written in Chinese and kept that way. Docstrings on public API symbols (exported from `streamgate.__init__`) should be English-friendly.
- **No test suite yet.** The project currently ships without tests (top roadmap item). Until then, please include a minimal reproduction script (or a runnable snippet) with bug reports and behavioral PRs so reviewers can verify against real data.
- **Changelog.** User-visible changes must update `CHANGELOG.md`.

## Submitting

1. Fork / create a branch.
2. Make the change, run the four local checks above.
3. Open a pull request with a short description and (if applicable) the reproduction script.
