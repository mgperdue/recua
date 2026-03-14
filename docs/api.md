# API Reference

## Core classes

### `TransferEngine`

```python
from recua import TransferEngine, TransferOptions

engine = TransferEngine(options=None)
```

| Method | Description |
|---|---|
| `start()` | Spin up worker threads. Must be called before `submit()`. |
| `submit(job, block=True, timeout=None)` | Add a job to the queue. |
| `submit_many(jobs)` | Submit an iterable of jobs. |
| `close()` | Signal no more submissions. Non-blocking. |
| `join()` | Block until all jobs finish and workers exit. |
| `cancel()` | Stop immediately. Partial downloads resumable. |
| `stats()` → `EngineStats` | Live metrics snapshot. |
| `__enter__` / `__exit__` | Context manager: calls `start()`, `close()`, `join()`. |

### `TransferJob`

```python
from recua import TransferJob

job = TransferJob(
    source="https://example.com/file.bin",  # required
    dest=Path("downloads/file.bin"),         # required
    name="file.bin",                         # display name (optional)
    expected_size=1_048_576,                 # bytes (optional)
    expected_checksum="a3f5...",             # hex digest (optional)
    priority=0,                              # lower = higher priority
    meta={},                                 # passed to callbacks
)
```

Properties: `resume_key`, `display_name`

### `TransferOptions`

```python
from recua import TransferOptions

opts = TransferOptions(
    # Concurrency
    max_workers=4,
    queue_size=1000,
    # Performance
    max_mb_per_sec=None,       # float | None
    chunk_size=1_048_576,      # bytes
    # Reliability
    retries=5,
    backoff_base=1.5,
    # Persistence
    state_path=None,           # Path | None
    # Integrity
    checksum_algorithm=None,   # "md5"|"sha1"|"sha256"|"sha512"|None
    # Callbacks
    on_complete=None,          # Callable[[TransferJob], None]
    on_error=None,             # Callable[[TransferJob, Exception], None]
    on_progress=None,          # Callable[[TransferJob, int, int|None], None]
    # UX
    progress=True,             # rich display; requires recua[progress]
)
```

### `EngineStats`

Immutable snapshot returned by `engine.stats()`:

```python
stats = engine.stats()
stats.completed      # int
stats.failed         # int
stats.queued         # int
stats.active         # int
stats.bytes_total    # int
stats.bytes_done     # int
stats.speed_mb_per_sec  # float
```

---

## Exceptions

All importable from `recua` or `recua.exceptions`:

```
TransferEngineError
├── EngineNotStartedError   submit() before start()
├── EngineShutdownError     submit() after close() / cancel()
├── QueueFullError          non-blocking submit on full queue
└── TransferError
    ├── RetriableError      transient — worker retried
    │   └── RateLimitError  HTTP 429; carries .retry_after (float|None)
    └── FatalTransferError  permanent — worker gave up
```

---

## Protocols

For building custom adapters or state stores:

```python
from recua.protocols import TransferAdapter, StateStore
```

See `src/recua/protocols.py` for full method signatures.

---

## CLI

```
recua get URL [URL ...] [--dest DIR] [--workers N] [--retries N]
             [--limit MB/S] [--state FILE]
             [--sha256 HEX] [--sha512 HEX] [--md5 HEX]

recua batch FILE [--dest DIR] [--workers N] [--retries N]
              [--limit MB/S] [--state FILE] [--checksum-algo ALGO]
```

---

## Version

```python
import recua
print(recua.__version__)
```
