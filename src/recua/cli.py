"""
recua CLI — command-line interface for the transfer engine.

Usage examples
--------------
Download a single file:

    recua get https://example.com/file.bin

Download multiple URLs from a file (one URL per line):

    recua batch urls.txt --dest ./downloads --workers 8

Download with checksum verification:

    recua get https://example.com/file.bin --sha256 a3f5...

Show help:

    recua --help
    recua get --help
    recua batch --help

Installation
------------
The CLI is available when recua is installed:

    pip install recua

Entry point: recua (defined in pyproject.toml [project.scripts])
"""

from __future__ import annotations

import sys
import argparse
import logging
from pathlib import Path

from recua.engine import TransferEngine
from recua.job import TransferJob
from recua.options import TransferOptions


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="recua",
        description="Concurrent, resumable file transfer engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the rich progress display.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------ get
    get_p = sub.add_parser(
        "get",
        help="Download one or more URLs.",
        description="Download one or more URLs to a destination directory.",
    )
    get_p.add_argument(
        "urls",
        nargs="+",
        metavar="URL",
        help="URL(s) to download.",
    )
    get_p.add_argument(
        "--dest", "-d",
        type=Path,
        default=Path("."),
        metavar="DIR",
        help="Destination directory (default: current directory).",
    )
    get_p.add_argument(
        "--workers", "-w",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel download workers (default: 4).",
    )
    get_p.add_argument(
        "--retries", "-r",
        type=int,
        default=5,
        metavar="N",
        help="Max retry attempts per file (default: 5).",
    )
    get_p.add_argument(
        "--limit",
        type=float,
        default=None,
        metavar="MB/S",
        help="Bandwidth cap in MB/s (default: unlimited).",
    )
    get_p.add_argument(
        "--state",
        type=Path,
        default=None,
        metavar="FILE",
        help="SQLite state file for resumable downloads.",
    )
    get_p.add_argument(
        "--sha256",
        default=None,
        metavar="HEX",
        help="Expected SHA-256 checksum (only valid with a single URL).",
    )
    get_p.add_argument(
        "--sha512",
        default=None,
        metavar="HEX",
        help="Expected SHA-512 checksum (only valid with a single URL).",
    )
    get_p.add_argument(
        "--md5",
        default=None,
        metavar="HEX",
        help="Expected MD5 checksum (only valid with a single URL).",
    )

    # ------------------------------------------------------------------ batch
    batch_p = sub.add_parser(
        "batch",
        help="Download URLs listed in a file.",
        description=(
            "Read URLs from a text file (one per line, # comments ignored) "
            "and download them all."
        ),
    )
    batch_p.add_argument(
        "url_file",
        type=Path,
        metavar="FILE",
        help="Text file containing URLs, one per line.",
    )
    batch_p.add_argument(
        "--dest", "-d",
        type=Path,
        default=Path("."),
        metavar="DIR",
        help="Destination directory (default: current directory).",
    )
    batch_p.add_argument(
        "--workers", "-w",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel download workers (default: 4).",
    )
    batch_p.add_argument(
        "--retries", "-r",
        type=int,
        default=5,
        metavar="N",
        help="Max retry attempts per file (default: 5).",
    )
    batch_p.add_argument(
        "--limit",
        type=float,
        default=None,
        metavar="MB/S",
        help="Bandwidth cap in MB/s (default: unlimited).",
    )
    batch_p.add_argument(
        "--state",
        type=Path,
        default=None,
        metavar="FILE",
        help="SQLite state file for resumable downloads.",
    )
    batch_p.add_argument(
        "--checksum-algo",
        choices=["md5", "sha1", "sha256", "sha512"],
        default=None,
        metavar="ALGO",
        help="Checksum algorithm. Jobs must include expected checksums.",
    )

    return parser


def _resolve_checksum(args: argparse.Namespace) -> tuple[str | None, str | None]:
    """Return (algorithm, expected_hex) from CLI args, or (None, None)."""
    if args.sha256:
        return "sha256", args.sha256
    if getattr(args, "sha512", None):
        return "sha512", args.sha512
    if getattr(args, "md5", None):
        return "md5", args.md5
    return None, None


def _read_url_file(path: Path) -> list[str]:
    """Read URLs from a text file, skipping blank lines and # comments."""
    urls = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


def _run_get(args: argparse.Namespace) -> int:
    checksum_algo, expected_checksum = _resolve_checksum(args)

    if expected_checksum is not None and len(args.urls) > 1:
        print(
            "error: --sha256/--sha512/--md5 can only be used with a single URL",
            file=sys.stderr,
        )
        return 1

    dest: Path = args.dest
    dest.mkdir(parents=True, exist_ok=True)

    opts = TransferOptions(
        max_workers=args.workers,
        retries=args.retries,
        max_mb_per_sec=args.limit,
        state_path=args.state,
        checksum_algorithm=checksum_algo,
        progress=not args.no_progress,
    )

    jobs = [
        TransferJob(
            source=url,
            dest=dest / Path(url).name,
            expected_checksum=expected_checksum if len(args.urls) == 1 else None,
        )
        for url in args.urls
    ]

    with TransferEngine(opts) as engine:
        engine.submit_many(jobs)

    stats = engine.stats()
    if stats.failed:
        print(f"\n{stats.failed} file(s) failed. Re-run to retry.", file=sys.stderr)
        return 1
    return 0


def _run_batch(args: argparse.Namespace) -> int:
    if not args.url_file.exists():
        print(f"error: URL file not found: {args.url_file}", file=sys.stderr)
        return 1

    urls = _read_url_file(args.url_file)
    if not urls:
        print("error: URL file is empty or contains only comments", file=sys.stderr)
        return 1

    dest: Path = args.dest
    dest.mkdir(parents=True, exist_ok=True)

    opts = TransferOptions(
        max_workers=args.workers,
        retries=args.retries,
        max_mb_per_sec=args.limit,
        state_path=args.state,
        checksum_algorithm=getattr(args, "checksum_algo", None),
        progress=not args.no_progress,
    )

    with TransferEngine(opts) as engine:
        engine.submit_many(
            TransferJob(source=url, dest=dest / Path(url).name)
            for url in urls
        )

    stats = engine.stats()
    print(
        f"\n{stats.completed} completed, {stats.failed} failed "
        f"of {stats.completed + stats.failed} total."
    )
    if stats.failed:
        print(f"Re-run with --state to resume failed downloads.", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """
    CLI entry point. Returns an exit code (0 = success, 1 = failure).

    Importable for testing:
        from recua.cli import main
        exit_code = main(["get", "https://example.com/file.bin", "--dest", "/tmp"])
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    if args.command == "get":
        return _run_get(args)
    if args.command == "batch":
        return _run_batch(args)

    parser.print_help()
    return 1


def _entry_point() -> None:
    """setuptools entry point — calls main() and exits with its return code."""
    sys.exit(main())


if __name__ == "__main__":
    _entry_point()
