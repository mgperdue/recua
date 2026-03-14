"""
Terminal progress display for TransferEngine.

Uses `rich` if available, falls back to a plain-text logger if not.
This keeps `rich` a soft optional dependency — the engine works fine
without it; you just get less visual feedback.

Architecture
------------
ProgressDisplay is created by TransferEngine.start() when
TransferOptions.progress=True. It registers itself as the on_progress
callback (wrapping any existing callback the caller provided) and wires
into on_complete / on_error similarly.

The display runs its own refresh loop in a background daemon thread so
it never blocks worker threads. Workers call _on_progress(), which
updates an in-memory dict of per-job state under a lock. The refresh
loop reads that dict and redraws at ~10 FPS.

Graceful shutdown: stop() is called by TransferEngine just before join()
returns. It flushes the final state and terminates the refresh thread.

Rich availability
-----------------
We import rich lazily at display startup, not at module import time.
If rich is not installed, _RichDisplay falls back to _PlainDisplay,
which just logs completion events. This means the recua core never
has a hard import dependency on rich.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.progress import Progress as RichProgress

    from recua.job import TransferJob
    from recua.metrics import MetricsCollector

logger = logging.getLogger(__name__)

_REFRESH_HZ = 10
_REFRESH_INTERVAL = 1.0 / _REFRESH_HZ


def make_display(metrics: MetricsCollector) -> ProgressDisplay:
    """
    Return the best available ProgressDisplay.

    Tries rich first; falls back to plain logging if not installed.
    """
    try:
        import rich  # noqa: F401

        return _RichDisplay(metrics)
    except ImportError:
        logger.debug("rich not installed — using plain progress logging")
        return _PlainDisplay(metrics)


class ProgressDisplay:
    """Base class / interface for progress displays."""

    def start(self) -> None: ...
    def stop(self) -> None: ...

    def on_progress(
        self,
        job: TransferJob,
        bytes_done: int,
        bytes_total: int | None,
    ) -> None: ...

    def on_complete(self, job: TransferJob) -> None: ...
    def on_error(self, job: TransferJob, exc: Exception) -> None: ...


class _PlainDisplay(ProgressDisplay):
    """
    Fallback display when rich is not installed.

    Logs a completion/failure message for each job. No live bars.
    """

    def __init__(self, metrics: MetricsCollector) -> None:
        self._metrics = metrics

    def start(self) -> None:
        logger.info("recua transfer started (install rich for live progress)")

    def stop(self) -> None:
        stats = self._metrics.snapshot()
        logger.info(
            "recua transfer complete — %d completed, %d failed",
            stats.completed,
            stats.failed,
        )

    def on_progress(self, job: TransferJob, bytes_done: int, bytes_total: int | None) -> None:
        pass  # no per-chunk output in plain mode

    def on_complete(self, job: TransferJob) -> None:
        logger.info("✓ %s", job.display_name)

    def on_error(self, job: TransferJob, exc: Exception) -> None:
        logger.error("✗ %s — %s", job.display_name, exc)


class _RichDisplay(ProgressDisplay):
    """
    Live progress display using rich.Progress.

    Shows:
      - One bar per active job (file name, bytes transferred, speed)
      - A summary bar: N completed / M failed / K queued
      - Overall speed (MB/s) from MetricsCollector

    Thread safety: all rich operations go through the Progress context
    manager which is thread-safe. Per-job task IDs are stored in a
    dict protected by self._lock.
    """

    def __init__(self, metrics: MetricsCollector) -> None:
        self._metrics = metrics
        self._lock = threading.Lock()
        self._task_ids: dict[str, int] = {}  # job.display_name → rich task id
        self._progress: RichProgress | None = None
        self._overall_task: int | None = None
        self._refresh_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            refresh_per_second=_REFRESH_HZ,
        )
        self._progress.start()
        self._overall_task = self._progress.add_task("[green]Overall", total=None)

        self._refresh_thread = threading.Thread(
            target=self._refresh_loop, daemon=True, name="recua-progress"
        )
        self._refresh_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._refresh_thread is not None:
            self._refresh_thread.join(timeout=2.0)
        if self._progress is not None:
            self._refresh_overall()
            self._progress.stop()

    def on_progress(
        self,
        job: TransferJob,
        bytes_done: int,
        bytes_total: int | None,
    ) -> None:
        if self._progress is None:
            return
        with self._lock:
            task_id = self._task_ids.get(job.display_name)
            if task_id is None:
                task_id = self._progress.add_task(
                    job.display_name,
                    total=bytes_total,
                    completed=bytes_done,
                )
                self._task_ids[job.display_name] = task_id
            else:
                self._progress.update(
                    task_id,
                    completed=bytes_done,
                    total=bytes_total,
                )

    def on_complete(self, job: TransferJob) -> None:
        if self._progress is None:
            return
        with self._lock:
            task_id = self._task_ids.pop(job.display_name, None)
            if task_id is not None:
                self._progress.update(task_id, visible=False)

    def on_error(self, job: TransferJob, exc: Exception) -> None:
        if self._progress is None:
            return
        with self._lock:
            task_id = self._task_ids.pop(job.display_name, None)
            if task_id is not None:
                self._progress.update(
                    task_id,
                    description=f"[red]✗ {job.display_name}",
                    visible=False,
                )

    def _refresh_overall(self) -> None:
        if self._progress is None or self._overall_task is None:
            return
        stats = self._metrics.snapshot()
        self._progress.update(
            self._overall_task,
            description=(
                f"[green]✓ {stats.completed}  "
                f"[red]✗ {stats.failed}  "
                f"[yellow]⏳ {stats.queued + stats.active}  "
                f"[cyan]{stats.speed_mb_per_sec:.1f} MB/s"
            ),
        )

    def _refresh_loop(self) -> None:
        while not self._stop_event.is_set():
            self._refresh_overall()
            time.sleep(_REFRESH_INTERVAL)
