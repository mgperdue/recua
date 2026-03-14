"""
Tests for progress.py.

We test the logic and interfaces without needing a real TTY.
The _RichDisplay tests use rich if installed; _PlainDisplay tests
always run regardless of rich availability.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from recua.job import TransferJob
from recua.metrics import MetricsCollector
from recua.progress import _PlainDisplay, make_display


def _job(name: str = "file.bin") -> TransferJob:
    return TransferJob(
        source=f"https://example.com/{name}",
        dest=Path(f"/tmp/{name}"),
        name=name,
    )


# ---------------------------------------------------------------------------
# make_display factory
# ---------------------------------------------------------------------------


class TestMakeDisplay:
    def test_returns_rich_display_when_rich_available(self) -> None:
        pytest.importorskip("rich")
        from recua.progress import _RichDisplay

        display = make_display(MetricsCollector())
        assert isinstance(display, _RichDisplay)

    def test_returns_plain_display_when_rich_unavailable(self) -> None:
        with patch.dict("sys.modules", {"rich": None}):
            display = make_display(MetricsCollector())
        assert isinstance(display, _PlainDisplay)


# ---------------------------------------------------------------------------
# _PlainDisplay
# ---------------------------------------------------------------------------


class TestPlainDisplay:
    def test_start_does_not_raise(self) -> None:
        d = _PlainDisplay(MetricsCollector())
        d.start()

    def test_stop_does_not_raise(self) -> None:
        d = _PlainDisplay(MetricsCollector())
        d.start()
        d.stop()

    def test_on_complete_does_not_raise(self) -> None:
        d = _PlainDisplay(MetricsCollector())
        d.on_complete(_job())

    def test_on_error_does_not_raise(self) -> None:
        d = _PlainDisplay(MetricsCollector())
        d.on_error(_job(), RuntimeError("boom"))

    def test_on_progress_does_not_raise(self) -> None:
        d = _PlainDisplay(MetricsCollector())
        d.on_progress(_job(), 1024, 4096)

    def test_stop_logs_summary(self, caplog) -> None:
        import logging

        metrics = MetricsCollector()
        metrics.job_queued()
        metrics.job_started()
        metrics.job_completed()

        d = _PlainDisplay(metrics)
        with caplog.at_level(logging.INFO, logger="recua.progress"):
            d.start()
            d.stop()

        assert any("complete" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# _RichDisplay (only runs when rich is installed)
# ---------------------------------------------------------------------------


class TestRichDisplay:
    @pytest.fixture(autouse=True)
    def require_rich(self) -> None:
        pytest.importorskip("rich")

    def test_start_and_stop(self) -> None:
        from recua.progress import _RichDisplay

        d = _RichDisplay(MetricsCollector())
        d.start()
        d.stop()

    def test_on_progress_creates_task(self) -> None:
        from recua.progress import _RichDisplay

        d = _RichDisplay(MetricsCollector())
        d.start()
        job = _job("test.bin")
        d.on_progress(job, 512, 1024)
        assert "test.bin" in d._task_ids
        d.stop()

    def test_on_complete_removes_task(self) -> None:
        from recua.progress import _RichDisplay

        d = _RichDisplay(MetricsCollector())
        d.start()
        job = _job("done.bin")
        d.on_progress(job, 512, 1024)
        d.on_complete(job)
        assert "done.bin" not in d._task_ids
        d.stop()

    def test_on_error_removes_task(self) -> None:
        from recua.progress import _RichDisplay

        d = _RichDisplay(MetricsCollector())
        d.start()
        job = _job("failed.bin")
        d.on_progress(job, 100, 200)
        d.on_error(job, RuntimeError("network error"))
        assert "failed.bin" not in d._task_ids
        d.stop()

    def test_stop_is_idempotent(self) -> None:
        from recua.progress import _RichDisplay

        d = _RichDisplay(MetricsCollector())
        d.start()
        d.stop()
        d.stop()  # must not raise


# ---------------------------------------------------------------------------
# Progress display wired through engine
# ---------------------------------------------------------------------------


class TestProgressWiredThroughEngine:
    def test_progress_false_does_not_create_display(self) -> None:
        from recua.engine import TransferEngine
        from recua.options import TransferOptions

        engine = TransferEngine(TransferOptions(progress=False))
        engine.start()
        assert engine._display is None
        engine.cancel()
        engine.join()

    def test_progress_true_creates_display(self) -> None:
        from recua.engine import TransferEngine
        from recua.options import TransferOptions

        engine = TransferEngine(TransferOptions(progress=True))
        engine.start()
        assert engine._display is not None
        engine.cancel()
        engine.join()
