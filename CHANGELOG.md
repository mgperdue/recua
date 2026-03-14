# Changelog

All notable changes to recua will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] — unreleased

### Added

- `TransferEngine` — concurrent job engine with context manager lifecycle
- `TransferJob` — immutable job descriptor with `resume_key`, `expected_checksum`
- `TransferOptions` — full configuration dataclass including callbacks, checksum, bandwidth cap, progress toggle
- `SQLiteStateStore` — persistent resume state across process restarts (WAL mode, thread-safe)
- `NullStateStore` — zero-config in-memory fallback when `state_path` is None
- `HTTPAdapter` — HTTP/HTTPS downloads with Range-based resume, 429/5xx/4xx classification, per-thread sessions
- `RateLimiter` — token-bucket aggregate bandwidth cap (`max_mb_per_sec`)
- `MetricsCollector` — thread-safe counters and sliding-window speed meter
- `Scheduler` — priority queue with sequence-number FIFO tie-breaking and backpressure
- `Worker` — iterative retry loop, exponential backoff, checksum verification, progress callbacks
- `checksum.py` — post-download SHA-256/512/MD5/SHA-1 verification; mismatch raises `RetriableError`
- `progress.py` — rich terminal display (live per-job bars, speed, ETA) with plain-log fallback
- `cli.py` — `recua get` and `recua batch` commands via `argparse`
- Full public exception hierarchy: `TransferEngineError`, `QueueFullError`, `EngineNotStartedError`, `EngineShutdownError`, `RetriableError`, `FatalTransferError`, `RateLimitError`
- `TransferAdapter` and `StateStore` structural protocols (PEP 544, `runtime_checkable`)
- 265 tests across 15 test files; integration tests marked `@pytest.mark.integration`
- GitHub Actions CI: test, lint (ruff + mypy), publish (Trusted Publisher)
- Pre-commit hooks: ruff lint + format, file hygiene

[Unreleased]: https://github.com/mgperdue/recua/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mgperdue/recua/releases/tag/v0.1.0
