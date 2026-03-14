# Contributing to recua

Thank you for your interest in contributing. This document covers setup,
conventions, and the pull request process.

## Development setup

**Prerequisites:** Python 3.11+, [uv](https://docs.astral.sh/uv/)

```bash
git clone https://github.com/youruser/recua
cd recua
uv sync                  # creates .venv and installs all dev dependencies
pre-commit install       # installs git hooks (ruff lint + format, file hygiene)
```

## Running tests

```bash
# Unit tests + mocked HTTP (fast, ~10s)
uv run pytest -m "not integration"

# Integration tests — real HTTP server via pytest-httpserver (~30s)
uv run pytest -m integration

# Full suite with coverage report
uv run pytest

# Coverage HTML report
open htmlcov/index.html
```

## Lint and type checking

```bash
uv run ruff check src tests        # lint
uv run ruff format src tests       # format
uv run mypy src                    # type check
```

Pre-commit runs ruff automatically on every `git commit`. mypy runs in CI
but not in pre-commit (it's too slow for a commit hook).

## Project structure

```
src/recua/          library source — see README for module descriptions
tests/              one test file per source module
.github/workflows/  CI: test.yml, lint.yml, publish.yml
docs/               MkDocs source
```

## Making changes

1. **Branch** from `main` using a descriptive name:
   - `feat/s3-adapter` — new feature
   - `fix/resume-offset-race` — bug fix
   - `chore/update-deps` — maintenance

2. **Write tests first** when adding features or fixing bugs. All modules
   have a corresponding `tests/test_<module>.py`. Integration tests go in
   `tests/test_integration.py` and are marked `@pytest.mark.integration`.

3. **Keep the public API small.** New public symbols require deliberate
   additions to `__init__.py` and `__all__`.

4. **Update `CHANGELOG.md`** for any user-facing change under `[Unreleased]`.

5. **Open a PR** against `main`. The PR template will guide you through
   the checklist.

## Adding a new transport adapter

Implement the `TransferAdapter` protocol from `recua.protocols`:

```python
from collections.abc import Iterator
from recua.job import TransferJob

class MyAdapter:
    def supports(self, source: str) -> bool:
        return source.startswith("myscheme://")

    def get_size(self, source: str) -> int | None:
        ...  # HEAD equivalent; return None if unknown

    def fetch(
        self,
        job: TransferJob,
        offset: int = 0,
        chunk_size: int = 1_048_576,
    ) -> Iterator[bytes]:
        ...  # yield chunks; raise RetriableError / FatalTransferError
```

Register it by passing it to `TransferEngine._adapters` or by subclassing
`TransferEngine` and overriding `__init__`. A proper plugin registration
API is planned for v0.2.

## Versioning

recua follows [Semantic Versioning](https://semver.org/):

- **Patch** (`0.1.x`): bug fixes, no API changes
- **Minor** (`0.x.0`): new features, backward-compatible
- **Major** (`x.0.0`): breaking API changes

Version is set in `pyproject.toml`. `recua.__version__` reads it at
runtime via `importlib.metadata`.

## Release process (maintainers)

1. Update `version` in `pyproject.toml`
2. Move `[Unreleased]` section in `CHANGELOG.md` to the new version with today's date
3. Commit: `git commit -m "chore: release v0.x.y"`
4. Tag: `git tag v0.x.y && git push origin v0.x.y`
5. Create a GitHub Release from the tag — this triggers `publish.yml`

## Code style

- Line length: 100 characters (ruff enforced)
- Type annotations: required on all public functions and methods
- Docstrings: NumPy/Google style, required on all public classes and methods
- No bare `except:` — always catch specific exception types
- No `print()` in library code — use `logging.getLogger(__name__)`

## Questions

Open a GitHub Discussion or file an issue with the `question` label.
