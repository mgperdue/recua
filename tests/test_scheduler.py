"""
Tests for Scheduler.

Coverage targets
----------------
Construction:
  - maxsize property
  - qsize starts at zero

put():
  - basic enqueue
  - QueueFullError on block=False when full
  - QueueFullError on block=True with timeout expiry
  - block=True waits until space is available

get():
  - returns None on timeout when empty
  - returns job when available
  - unpacks tuple correctly (returns TransferJob, not raw tuple)

Priority ordering:
  - lower priority value dequeued first
  - equal priority preserves FIFO (submission order)
  - mixed priority: correct interleaving
  - no TypeError on equal-priority jobs (sequence_number tie-breaker)

task_done() / join():
  - join() unblocks after all task_done() calls
  - join() returns immediately on empty queue

qsize / maxsize:
  - qsize tracks queue depth
  - maxsize reflects constructor argument

Thread safety:
  - concurrent producers, correct job count
  - concurrent producers and consumers, no job lost or duplicated
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from recua.exceptions import QueueFullError
from recua.job import TransferJob
from recua.scheduler import Scheduler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _job(name: str, priority: int = 0) -> TransferJob:
    return TransferJob(
        source=f"https://example.com/{name}.bin",
        dest=Path(f"/tmp/{name}.bin"),
        name=name,  # set explicit name so display_name == name, not "name.bin"
        priority=priority,
    )


def _drain(scheduler: Scheduler, n: int) -> list[TransferJob]:
    """Get n jobs, calling task_done() after each. Fails fast on timeout."""
    jobs = []
    for _ in range(n):
        job = scheduler.get(timeout=2.0)
        assert job is not None, "Expected a job but timed out"
        scheduler.task_done()
        jobs.append(job)
    return jobs


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_qsize_starts_at_zero(self) -> None:
        s = Scheduler(maxsize=10)
        assert s.qsize == 0

    def test_maxsize_property(self) -> None:
        s = Scheduler(maxsize=42)
        assert s.maxsize == 42

    def test_unlimited_maxsize(self) -> None:
        s = Scheduler(maxsize=0)
        assert s.maxsize == 0


# ---------------------------------------------------------------------------
# put()
# ---------------------------------------------------------------------------


class TestPut:
    def test_basic_enqueue(self) -> None:
        s = Scheduler(maxsize=10)
        s.put(_job("a"))
        assert s.qsize == 1

    def test_multiple_enqueue(self) -> None:
        s = Scheduler(maxsize=10)
        for i in range(5):
            s.put(_job(str(i)))
        assert s.qsize == 5

    def test_raises_queue_full_error_non_blocking(self) -> None:
        s = Scheduler(maxsize=2)
        s.put(_job("a"))
        s.put(_job("b"))
        with pytest.raises(QueueFullError):
            s.put(_job("c"), block=False)

    def test_raises_queue_full_error_on_timeout(self) -> None:
        s = Scheduler(maxsize=1)
        s.put(_job("a"))
        start = time.monotonic()
        with pytest.raises(QueueFullError):
            s.put(_job("b"), block=True, timeout=0.15)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1

    def test_blocking_put_waits_for_space(self) -> None:
        """
        Producer blocks on a full queue. A consumer drains one slot,
        allowing the blocked put() to complete.
        """
        s = Scheduler(maxsize=1)
        s.put(_job("a"))

        result: list[bool] = []

        def producer() -> None:
            s.put(_job("b"), block=True)
            result.append(True)

        t = threading.Thread(target=producer)
        t.start()

        time.sleep(0.05)
        assert not result, "put() should still be blocking"

        job = s.get(timeout=1.0)
        assert job is not None
        s.task_done()

        t.join(timeout=1.0)
        assert result == [True], "put() should have unblocked after consumer"


# ---------------------------------------------------------------------------
# get()
# ---------------------------------------------------------------------------


class TestGet:
    def test_returns_none_on_empty_queue(self) -> None:
        s = Scheduler(maxsize=10)
        result = s.get(timeout=0.05)
        assert result is None

    def test_returns_job_when_available(self) -> None:
        s = Scheduler(maxsize=10)
        job = _job("a")
        s.put(job)
        result = s.get(timeout=1.0)
        assert result is job

    def test_returns_transfer_job_not_tuple(self) -> None:
        """get() must unwrap the (priority, seq, job) tuple."""
        s = Scheduler(maxsize=10)
        s.put(_job("a"))
        result = s.get(timeout=1.0)
        assert isinstance(result, TransferJob)

    def test_qsize_decrements_on_get(self) -> None:
        s = Scheduler(maxsize=10)
        s.put(_job("a"))
        s.put(_job("b"))
        s.get(timeout=1.0)
        assert s.qsize == 1


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


class TestPriorityOrdering:
    def test_lower_priority_value_dequeued_first(self) -> None:
        s = Scheduler(maxsize=10)
        s.put(_job("low-urgency", priority=10))
        s.put(_job("high-urgency", priority=1))
        s.put(_job("medium", priority=5))

        jobs = _drain(s, 3)
        assert jobs[0].display_name == "high-urgency"
        assert jobs[1].display_name == "medium"
        assert jobs[2].display_name == "low-urgency"

    def test_equal_priority_preserves_fifo(self) -> None:
        """
        Jobs at the same priority must come out in submission order.
        This is the sequence_number tie-breaker guarantee.
        """
        s = Scheduler(maxsize=10)
        names = ["first", "second", "third", "fourth", "fifth"]
        for name in names:
            s.put(_job(name, priority=0))

        jobs = _drain(s, len(names))
        assert [j.display_name for j in jobs] == names

    def test_mixed_priority_correct_interleaving(self) -> None:
        s = Scheduler(maxsize=10)
        s.put(_job("p2-a", priority=2))
        s.put(_job("p0-a", priority=0))
        s.put(_job("p1-a", priority=1))
        s.put(_job("p0-b", priority=0))
        s.put(_job("p2-b", priority=2))

        jobs = _drain(s, 5)
        names = [j.display_name for j in jobs]

        assert names.index("p0-a") < names.index("p1-a")
        assert names.index("p0-b") < names.index("p1-a")
        assert names.index("p1-a") < names.index("p2-a")
        assert names.index("p1-a") < names.index("p2-b")
        assert names.index("p0-a") < names.index("p0-b")

    def test_no_type_error_on_equal_priority(self) -> None:
        """
        Regression: without a sequence number, PriorityQueue would fall
        through to comparing TransferJob instances, causing TypeError.
        """
        s = Scheduler(maxsize=25)
        for i in range(20):
            s.put(_job(str(i), priority=0))
        _drain(s, 20)  # must not raise TypeError


# ---------------------------------------------------------------------------
# task_done() / join()
# ---------------------------------------------------------------------------


class TestTaskDoneJoin:
    def test_join_unblocks_after_all_task_done(self) -> None:
        """
        Put N jobs, consume and task_done() each one in a thread,
        then join() on the main thread must unblock cleanly.

        Structure: start consumer thread first, then join() — this avoids
        the race where join() is called before any task_done() has fired.
        """
        s = Scheduler(maxsize=10)
        n = 5
        for i in range(n):
            s.put(_job(str(i)))

        consumed = threading.Barrier(2)  # synchronise: consumer ready + main

        def consumer() -> None:
            for _ in range(n):
                job = s.get(timeout=2.0)
                assert job is not None
                s.task_done()
            consumed.wait()  # signal main thread all task_done()s are done

        t = threading.Thread(target=consumer)
        t.start()

        # Wait until consumer has called all task_done()s, then join()
        consumed.wait()
        s.join()  # must return immediately — all tasks already done
        t.join(timeout=1.0)
        assert not t.is_alive()

    def test_join_returns_immediately_on_empty_queue(self) -> None:
        s = Scheduler(maxsize=10)
        start = time.monotonic()
        s.join()
        assert time.monotonic() - start < 0.1


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_producers_correct_count(self) -> None:
        """
        8 producer threads each enqueue 50 jobs.
        Total jobs in queue must equal 8 * 50 = 400.
        """
        n_threads = 8
        n_jobs_each = 50
        s = Scheduler(maxsize=n_threads * n_jobs_each + 1)
        errors: list[Exception] = []

        def producer(thread_id: int) -> None:
            try:
                for i in range(n_jobs_each):
                    s.put(_job(f"t{thread_id}-j{i}"))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=producer, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert s.qsize == n_threads * n_jobs_each

    def test_concurrent_producers_and_consumers_no_loss(self) -> None:
        """
        4 producers × 25 jobs = 100 jobs total.
        4 consumers drain the queue.
        Every job must be received exactly once.

        Shutdown pattern: use a shared atomic counter rather than qsize
        to determine when all jobs are consumed, avoiding the race where
        a consumer sees qsize==0 before other consumers have dequeued.
        join() is the authoritative "all processed" signal.
        """
        n_producers = 4
        n_jobs_each = 25
        total = n_producers * n_jobs_each
        s = Scheduler(maxsize=total)

        produced: list[str] = []
        consumed: list[str] = []
        list_lock = threading.Lock()
        # Counter tracks how many jobs consumers have fully processed.
        remaining = [total]
        remaining_lock = threading.Lock()
        stop_event = threading.Event()
        errors: list[Exception] = []

        def producer(thread_id: int) -> None:
            try:
                for i in range(n_jobs_each):
                    name = f"t{thread_id}-j{i}"
                    with list_lock:
                        produced.append(name)
                    s.put(_job(name))
            except Exception as e:
                errors.append(e)

        def consumer() -> None:
            try:
                while not stop_event.is_set():
                    job = s.get(timeout=0.05)
                    if job is None:
                        continue
                    with list_lock:
                        consumed.append(job.display_name)
                    s.task_done()
                    with remaining_lock:
                        remaining[0] -= 1
                        if remaining[0] == 0:
                            stop_event.set()
            except Exception as e:
                errors.append(e)

        producers = [threading.Thread(target=producer, args=(i,)) for i in range(n_producers)]
        consumers = [threading.Thread(target=consumer) for _ in range(4)]

        for t in consumers:
            t.start()
        for t in producers:
            t.start()
        for t in producers:
            t.join()

        # join() blocks until every put()ted job has had task_done() called
        s.join()

        stop_event.set()  # ensure consumers exit even if remaining counter has a bug
        for t in consumers:
            t.join(timeout=2.0)

        assert errors == []
        assert len(consumed) == total
        assert sorted(consumed) == sorted(produced)
