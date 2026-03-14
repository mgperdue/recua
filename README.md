<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.png">
    <img src="assets/logo-light.png" alt="recua" width="480">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/mgperdue/recua/actions/workflows/test.yml">
    <img src="https://github.com/mgperdue/recua/actions/workflows/test.yml/badge.svg" alt="Tests">
  </a>
  <a href="https://github.com/mgperdue/recua/actions/workflows/lint.yml">
    <img src="https://github.com/mgperdue/recua/actions/workflows/lint.yml/badge.svg" alt="Lint">
  </a>
  <a href="https://codecov.io/gh/mgperdue/recua">
    <img src="https://codecov.io/gh/mgperdue/recua/branch/main/graph/badge.svg" alt="Coverage">
  </a>
  <a href="https://pypi.org/project/recua/">
    <img src="https://img.shields.io/pypi/v/recua.svg" alt="PyPI version">
  </a>
  <a href="https://pypi.org/project/recua/">
    <img src="https://img.shields.io/pypi/pyversions/recua.svg" alt="Python 3.11+">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="MIT License">
  </a>
</p>

Embeddable, pure-Python, aria2-style file transfer engine.

*Recua* — a coordinated train of pack animals, each carrying part of the load.

Designed for hundreds or thousands of large files, unreliable networks,
and integration into other Python programs.

## Features

- **Concurrent downloads** — configurable worker pool
- **Resumable** — HTTP Range requests, SQLite state persistence across restarts
- **Retries** — exponential backoff, rate-limit awareness (HTTP 429)
- **Checksum verification** — SHA-256/512, MD5, SHA-1 post-download integrity checks
- **Bandwidth control** — token-bucket aggregate MB/s cap
- **Priority queue** — lower-value priority jobs run first; FIFO within same priority
- **Live progress** — rich terminal display (optional); falls back to plain logging
- **CLI** — `recua get` and `recua batch` commands
- **Embeddable** — pure Python, no daemon, no subprocess, no install-time extras required

## Installation

```bash
pip install recua

# with optional rich progress bars
pip install "recua[progress]"
```

## Quickstart

```python
from pathlib import Path
from recua import TransferEngine, TransferJob, TransferOptions

opts = TransferOptions(
    max_workers=4,
    state_path=Path("transfers.db"),   # enables resume across restarts
    checksum_algorithm="sha256",       # verify integrity post-download
)

with TransferEngine(opts) as engine:
    engine.submit_many(
        TransferJob(
            source=url,
            dest=Path("downloads") / Path(url).name,
        )
        for url in my_urls
    )
```

## CLI

```bash
# Download a single file
recua get https://example.com/archive.tar.gz --dest ./downloads

# Download with SHA-256 verification
recua get https://example.com/archive.tar.gz \
    --sha256 a3f5c2... \
    --dest ./downloads

# Download many files from a list (one URL per line, # comments ok)
recua batch urls.txt --dest ./downloads --workers 8

# Resume interrupted downloads
recua batch urls.txt --dest ./downloads --state transfers.db

# Cap bandwidth to 5 MB/s
recua batch urls.txt --dest ./downloads --limit 5
```

## Producer pattern (recommended for large datasets)

```python
def discover_urls():
    """Generator — yields URLs lazily."""
    for page in api.paginate():
        for record in page:
            yield record.download_url

with TransferEngine(opts) as engine:
    engine.submit_many(
        TransferJob(source=url, dest=out_dir / Path(url).name)
        for url in discover_urls()
    )
    # Queue backpressure keeps memory bounded — producer slows
    # automatically when workers can't keep up.
```

## Callbacks

```python
opts = TransferOptions(
    on_complete=lambda job: db.mark_downloaded(job.meta["id"]),
    on_error=lambda job, exc: logger.error("Failed %s: %s", job.source, exc),
    on_progress=lambda job, done, total: update_ui(job, done, total),
)
```

## Exceptions

```python
from recua import (
    TransferEngineError,   # base class for all recua exceptions
    EngineNotStartedError, # submit() before start()
    EngineShutdownError,   # submit() after close() / cancel()
    QueueFullError,        # non-blocking submit on full queue
    RetriableError,        # transient failure (worker retried)
    FatalTransferError,    # permanent failure (worker gave up)
    RateLimitError,        # HTTP 429; carries .retry_after
)
```

## Development setup

```bash
git clone https://github.com/youruser/recua
cd recua
uv sync
uv run pytest                           # all tests
uv run pytest -m "not integration"      # skip real-HTTP tests
uv run pytest -m integration            # only real-HTTP tests
uv run mypy src
uv run ruff check src tests
```

## Package layout

```
src/recua/
    __init__.py         public API — TransferEngine, TransferJob, TransferOptions, exceptions
    engine.py           TransferEngine — lifecycle, submission, context manager
    job.py              TransferJob — immutable job descriptor with resume_key
    options.py          TransferOptions — all configuration knobs
    exceptions.py       public exception hierarchy
    protocols.py        TransferAdapter + StateStore structural protocols
    workers.py          Worker threads — download loop, retry, checksum
    scheduler.py        PriorityQueue wrapper with backpressure
    rate_limit.py       Token-bucket bandwidth limiter
    metrics.py          Thread-safe counters + sliding-window speed meter
    state.py            SQLiteStateStore + NullStateStore
    checksum.py         Post-download hash verification
    progress.py         Rich terminal display (optional) + plain fallback
    cli.py              recua get / recua batch CLI
    adapters/
        http.py         HTTP/HTTPS adapter — Range resume, status classification
tests/
    test_checksum.py
    test_cli.py
    test_engine.py
    test_exceptions.py
    test_http_adapter.py
    test_integration.py     # requires pytest-httpserver; marked @pytest.mark.integration
    test_job.py
    test_metrics.py
    test_options.py
    test_progress.py
    test_protocols.py
    test_rate_limit.py
    test_scheduler.py
    test_state.py
    test_workers.py
```
