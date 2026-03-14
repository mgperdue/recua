"""Tests for TransferJob."""
from pathlib import Path

from recua.job import TransferJob


def test_resume_key_is_composite():
    job = TransferJob(source="https://example.com/file.bin", dest=Path("/tmp/file.bin"))
    key = job.resume_key
    assert key[0] == "https://example.com/file.bin"
    assert key[1] == str(Path("/tmp/file.bin").resolve())


def test_display_name_falls_back_to_dest_name():
    job = TransferJob(source="https://example.com/file.bin", dest=Path("/tmp/file.bin"))
    assert job.display_name == "file.bin"


def test_display_name_uses_name_when_provided():
    job = TransferJob(source="https://example.com/file.bin", dest=Path("/tmp/file.bin"), name="My File")
    assert job.display_name == "My File"
