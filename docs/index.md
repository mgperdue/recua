# recua

**Embeddable, pure-Python, aria2-style file transfer engine.**

*Recua* — a coordinated train of pack animals, each carrying part of the load.

---

## What is recua?

recua is a Python library for downloading many large files concurrently,
reliably, and efficiently — designed to be embedded directly in other programs
rather than run as a standalone tool.

```python
from recua import TransferEngine, TransferJob, TransferOptions

with TransferEngine(TransferOptions(max_workers=8)) as engine:
    engine.submit_many(
        TransferJob(source=url, dest=out / Path(url).name)
        for url in urls
    )
```

## Key features

| Feature | Details |
|---|---|
| Concurrent downloads | Configurable worker pool |
| Resumable | HTTP Range requests + SQLite state across restarts |
| Retries | Exponential backoff, HTTP 429 awareness |
| Checksums | SHA-256/512, MD5, SHA-1 post-download verification |
| Bandwidth cap | Token-bucket aggregate MB/s limit |
| Priority queue | Per-job priority with FIFO tie-breaking |
| Progress display | Live rich terminal bars; plain-log fallback |
| CLI | `recua get` and `recua batch` |
| Pure Python | No daemon, no subprocess, no compiled extensions |

## Why not aria2?

aria2 is a standalone external downloader. recua is a library:

| | aria2 | recua |
|---|---|---|
| Requires install | yes | no |
| Embeddable Python API | awkward | native |
| Dynamic producers | limited | first-class |
| Domain integration | hard | trivial |
| Pure Python | no | yes |

## Get started

- [Installation](installation.md)
- [Quickstart](quickstart.md)
- [API Reference](api.md)
