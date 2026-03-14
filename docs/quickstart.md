# Quickstart

## Basic download

```python
from pathlib import Path
from recua import TransferEngine, TransferJob, TransferOptions

urls = [
    "https://example.com/file1.bin",
    "https://example.com/file2.bin",
]

with TransferEngine(TransferOptions(max_workers=4)) as engine:
    engine.submit_many(
        TransferJob(source=url, dest=Path("downloads") / Path(url).name)
        for url in urls
    )
```

The context manager handles start, drain, and shutdown automatically.

## With persistence (resume across restarts)

```python
opts = TransferOptions(
    max_workers=4,
    state_path=Path("transfers.db"),
)

with TransferEngine(opts) as engine:
    engine.submit_many(jobs)
```

If the process is interrupted, re-running the same code picks up where
it left off — no re-downloading of completed files, partial files resume
from their last checkpoint.

## With checksum verification

```python
opts = TransferOptions(checksum_algorithm="sha256")

with TransferEngine(opts) as engine:
    engine.submit(TransferJob(
        source="https://example.com/archive.tar.gz",
        dest=Path("archive.tar.gz"),
        expected_checksum="a3f5c2...",  # hex digest
    ))
```

Jobs without `expected_checksum` are downloaded without verification even
when `checksum_algorithm` is set.

## Producer pattern

For large datasets, generate jobs lazily:

```python
def discover(api) -> Iterator[TransferJob]:
    for page in api.paginate():
        for record in page:
            yield TransferJob(
                source=record.url,
                dest=out_dir / record.filename,
                meta={"id": record.id},
            )

with TransferEngine(opts) as engine:
    engine.submit_many(discover(api))
    # Queue backpressure slows the producer automatically
    # when workers can't keep up — memory stays bounded
```

## Callbacks

```python
opts = TransferOptions(
    on_complete=lambda job: db.mark_done(job.meta["id"]),
    on_error=lambda job, exc: alerts.send(f"Failed: {job.source}"),
    on_progress=lambda job, done, total: update_bar(job, done, total),
)
```

## Handling failures

```python
with TransferEngine(opts) as engine:
    engine.submit_many(jobs)

stats = engine.stats()
if stats.failed:
    print(f"{stats.failed} jobs failed — re-run to retry")
```

Failed jobs are recorded in the state database. Re-running with the same
`state_path` will retry them automatically.

## CLI

```bash
recua get https://example.com/file.bin --dest ./downloads
recua batch urls.txt --dest ./downloads --workers 8 --state run.db
```

See the [CLI reference](guide/cli.md) for all options.
