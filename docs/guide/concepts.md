# Core concepts

## The producer → queue → workers model

recua separates *what to download* from *how downloads happen*:

```
Your code (producer)
    ↓ submit()
TransferEngine (queue)
    ↓ internal PriorityQueue
Worker pool (threads)
    ↓
HTTP adapter
    ↓
Filesystem
```

Your code discovers URLs and submits jobs. The engine handles
concurrency, retries, rate limiting, progress, and state.

## Jobs are immutable

`TransferJob` is a frozen dataclass. Once submitted, it cannot be
modified. The engine identifies a job for resumption by its
`resume_key = (source, str(dest.resolve()))`.

## Backpressure

`submit()` blocks by default when the queue is full
(`queue_size=1000`). This means a fast producer automatically slows
down to match worker throughput — memory stays bounded even with
millions of URLs.

## Resume semantics

When `state_path` is set, every job's progress is checkpointed to
SQLite every 16 MiB. On restart, the engine reads the offset and
sends a `Range: bytes=N-` request to resume from that byte.

Completed jobs are skipped entirely on re-runs.

## Retry policy

| Error type | Behaviour |
|---|---|
| 5xx (500, 502, 503, 504) | Retry with exponential backoff |
| 429 Too Many Requests | Retry after `Retry-After` header (or backoff) |
| Connection error / timeout | Retry with backoff |
| 4xx (400, 401, 403, 404…) | Fatal — no retry |
| Checksum mismatch | Retry from byte 0 (file truncated) |

Backoff delay after attempt *n*: `backoff_base ** n` seconds (default: 1.5ⁿ).
