# recua
Pure-Python concurrent transfer engine with resumable downloads, bandwidth control, and persistent state.

*Recua* — a coordinated train of pack animals, each carrying part of the load.

Designed for hundreds or thousands of large files, unreliable networks,
and integration into other Python programs.

## Quickstart

```python
from pathlib import Path
from recua import TransferEngine, TransferJob, TransferOptions

opts = TransferOptions(
    max_workers=4,
    state_path=Path("transfers.db"),  # enables resume across restarts
)

with TransferEngine(opts) as engine:
    for url in my_urls:
        engine.submit(TransferJob(
            source=url,
            dest=Path("downloads") / Path(url).name,
        ))
```

## Installation

```bash
pip install recua

# or with uv
uv add recua

# with optional rich progress bars
uv add "recua[progress]"
```

## Development setup

```bash
git clone https://github.com/youruser/recua
cd recua
uv sync
uv run pytest
```

## Package layout

```
src/recua/
    __init__.py         public API surface
    engine.py           TransferEngine — lifecycle and submission
    job.py              TransferJob — immutable job descriptor
    options.py          TransferOptions — configuration dataclass
    exceptions.py       public exception hierarchy
    protocols.py        TransferAdapter + StateStore protocols
    scheduler.py        PriorityQueue wrapper
    workers.py          Worker threads
    rate_limit.py       Token-bucket bandwidth limiter
    metrics.py          Thread-safe counters + sliding-window speed meter
    state.py            SQLiteStateStore + NullStateStore
    adapters/
        http.py         HTTP/HTTPS adapter (Range-based resume)
tests/
    test_job.py
    test_exceptions.py
    test_state.py
```

## Status

Scaffold — stubs in place, implementations pending.
