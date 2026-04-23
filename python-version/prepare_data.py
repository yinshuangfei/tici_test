#!/usr/bin/env python3
"""Download source data files for later import."""

import argparse
import csv
import gzip
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence
from urllib.parse import urlparse
from urllib.request import Request, urlopen


DEFAULT_OUTPUT_DIR = "data"
DEFAULT_TIMEOUT = 30.0
DEFAULT_PROGRESS_INTERVAL = 3.0
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_URL = "https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz"
DEFAULT_CONVERT_BATCH_SIZE = 50000
CSV_FIELDNAMES = ["timestamp", "severity_text", "body", "tenant_id"]


def infer_filename(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    if not name:
        raise ValueError("cannot infer output filename from url; please use --output-file")
    return name


@dataclass
class DownloadPlan:
    url: str
    output_file: Path
    timeout: float
    progress_interval: float
    chunk_size: int
    overwrite: bool


@dataclass
class ConvertPlan:
    input_file: Path
    output_file: Path
    max_rows: int
    batch_size: int
    progress_interval: float
    overwrite: bool


def build_download_plan(args: argparse.Namespace) -> DownloadPlan:
    output_dir = Path(args.output_dir)
    filename = args.output_file or infer_filename(args.url)
    output_file = output_dir / filename
    return DownloadPlan(
        url=args.url,
        output_file=output_file,
        timeout=args.timeout,
        progress_interval=args.progress_interval,
        chunk_size=args.chunk_size,
        overwrite=args.overwrite,
    )


def format_size(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{num_bytes}B"


def print_plan(plan: DownloadPlan) -> None:
    print(f"url: {plan.url}")
    print(f"output: {plan.output_file}")
    print(f"timeout: {plan.timeout:.1f}s")
    print(f"chunk_size: {plan.chunk_size}")
    print(f"overwrite: {plan.overwrite}")


def print_inline_progress(message: str) -> None:
    sys.stdout.write("\r" + message)
    sys.stdout.flush()


def finish_inline_progress() -> None:
    sys.stdout.write("\n")
    sys.stdout.flush()


def print_convert_plan(plan: ConvertPlan) -> None:
    print(f"convert_input: {plan.input_file}")
    print(f"convert_output: {plan.output_file}")
    print(f"convert_max_rows: {plan.max_rows}")
    print(f"convert_batch_size: {plan.batch_size}")
    print(f"convert_overwrite: {plan.overwrite}")


def download_file(plan: DownloadPlan) -> None:
    plan.output_file.parent.mkdir(parents=True, exist_ok=True)
    if plan.output_file.exists() and not plan.overwrite:
        raise ValueError(f"output file already exists: {plan.output_file}")

    request = Request(plan.url, headers={"User-Agent": "tici-test-prepare-data/1.0"})
    started_at = time.monotonic()
    next_progress_at = started_at + plan.progress_interval if plan.progress_interval > 0 else started_at
    bytes_written = 0
    tmp_file = plan.output_file.with_suffix(plan.output_file.suffix + ".part")

    if tmp_file.exists():
        tmp_file.unlink()

    try:
        with urlopen(request, timeout=plan.timeout) as response:
            total_size_header = response.headers.get("Content-Length")
            total_size = int(total_size_header) if total_size_header else 0
            with tmp_file.open("wb") as handle:
                while True:
                    if hasattr(response, "read1"):
                        # Prefer read1() so the loop regains control sooner and
                        # progress updates stay closer to the configured interval.
                        chunk = response.read1(plan.chunk_size)
                    else:
                        chunk = response.read(plan.chunk_size)
                    if not chunk:
                        break
                    handle.write(chunk)
                    bytes_written += len(chunk)
                    now = time.monotonic()
                    if plan.progress_interval == 0 or now >= next_progress_at:
                        elapsed = now - started_at
                        if total_size > 0:
                            print_inline_progress(
                                f"progress downloaded {format_size(bytes_written)} / "
                                f"{format_size(total_size)} in {elapsed:.1f}s"
                            )
                        else:
                            print_inline_progress(
                                f"progress downloaded {format_size(bytes_written)} in {elapsed:.1f}s"
                            )
                        if plan.progress_interval > 0:
                            while next_progress_at <= now:
                                next_progress_at += plan.progress_interval
        tmp_file.replace(plan.output_file)
    except Exception:
        finish_inline_progress()
        if tmp_file.exists():
            tmp_file.unlink()
        raise

    elapsed = time.monotonic() - started_at
    if bytes_written > 0:
        finish_inline_progress()
    print(f"completed download {format_size(bytes_written)} in {elapsed:.1f}s")


def open_text_file(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def iter_hdfs_batches(input_file: Path, max_rows: int, batch_size: int) -> Iterator[List[Dict[str, object]]]:
    batch: List[Dict[str, object]] = []
    with open_text_file(input_file) as handle:
        for index, line in enumerate(handle, start=1):
            if max_rows > 0 and index > max_rows:
                break
            raw = line.strip()
            if not raw:
                continue
            try:
                log_entry = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid json at line {index}: {exc}") from exc
            batch.append(log_entry)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def build_convert_plan(args: argparse.Namespace, download_plan: DownloadPlan) -> Optional[ConvertPlan]:
    if not args.convert_out:
        return None
    input_file = Path(args.input_file) if args.input_file else download_plan.output_file
    return ConvertPlan(
        input_file=input_file,
        output_file=Path(args.convert_out),
        max_rows=args.max_rows,
        batch_size=args.convert_batch_size,
        progress_interval=args.progress_interval,
        overwrite=args.overwrite,
    )


def write_csv_batch(writer: csv.DictWriter, logs: Iterable[Dict[str, object]]) -> int:
    written = 0
    for log in logs:
        writer.writerow(
            {
                "timestamp": log.get("timestamp"),
                "severity_text": log.get("severity_text"),
                "body": log.get("body"),
                "tenant_id": log.get("tenant_id"),
            }
        )
        written += 1
    return written


def convert_to_csv(plan: ConvertPlan) -> None:
    if not plan.input_file.is_file():
        raise ValueError(f"convert input file not found: {plan.input_file}")
    if plan.output_file.exists() and not plan.overwrite:
        raise ValueError(f"convert output file already exists: {plan.output_file}")
    if plan.batch_size < 1:
        raise ValueError("convert batch size must be >= 1")
    if plan.max_rows < 0:
        raise ValueError("max rows must be >= 0")

    plan.output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = plan.output_file.with_suffix(plan.output_file.suffix + ".part")
    if tmp_file.exists():
        tmp_file.unlink()

    started_at = time.monotonic()
    last_progress_at = started_at
    total_rows = 0

    try:
        with tmp_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            for batch in iter_hdfs_batches(plan.input_file, plan.max_rows, plan.batch_size):
                total_rows += write_csv_batch(writer, batch)
                now = time.monotonic()
                if plan.progress_interval == 0 or now - last_progress_at >= plan.progress_interval:
                    elapsed = now - started_at
                    print(f"progress converted {total_rows} rows in {elapsed:.1f}s")
                    last_progress_at = now
        tmp_file.replace(plan.output_file)
    except Exception:
        if tmp_file.exists():
            tmp_file.unlink()
        raise

    elapsed = time.monotonic() - started_at
    print(f"completed convert {total_rows} rows in {elapsed:.1f}s")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download source files for data preparation")
    parser.add_argument("url", nargs="?", default=DEFAULT_URL, help=f"Source file URL, default: {DEFAULT_URL}")
    parser.add_argument("--download", action="store_true", help="Download the source file")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Download directory, default: data")
    parser.add_argument("--output-file", default="", help="Override output filename")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output file")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Network timeout in seconds")
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=DEFAULT_PROGRESS_INTERVAL,
        help="Progress output interval in seconds, default: 3",
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Bytes read per download chunk")
    parser.add_argument("--input-file", default="", help="Existing local source file for convert mode")
    parser.add_argument("--convert-out", default="", help="Convert downloaded json/json.gz file into CSV")
    parser.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to convert, default: 0 means no limit")
    parser.add_argument(
        "--convert-batch-size",
        type=int,
        default=DEFAULT_CONVERT_BATCH_SIZE,
        help="Rows per conversion batch, default: 50000",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the download plan only")
    return parser


def run_prepare_data(args: argparse.Namespace, parser: Optional[argparse.ArgumentParser] = None) -> int:
    if not getattr(args, "url", None):
        if parser is not None:
            parser.print_help()
            return 0
        print("url is required", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("timeout must be > 0", file=sys.stderr)
        return 2
    if args.progress_interval < 0:
        print("progress interval must be >= 0", file=sys.stderr)
        return 2
    if args.chunk_size < 1:
        print("chunk size must be >= 1", file=sys.stderr)
        return 2
    if args.max_rows < 0:
        print("max rows must be >= 0", file=sys.stderr)
        return 2
    if args.convert_batch_size < 1:
        print("convert batch size must be >= 1", file=sys.stderr)
        return 2
    if not args.download and not args.convert_out:
        print("at least one action is required: use --download and/or --convert-out", file=sys.stderr)
        return 2

    try:
        plan = build_download_plan(args)
        convert_plan = build_convert_plan(args, plan)
        if args.dry_run:
            if args.download:
                print_plan(plan)
            if convert_plan is not None:
                print_convert_plan(convert_plan)
            return 0
        if args.download:
            download_file(plan)
        if convert_plan is not None:
            convert_to_csv(convert_plan)
        return 0
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"download failed: {exc}", file=sys.stderr)
        return 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    else:
        argv = list(argv)

    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    return run_prepare_data(args, parser)


if __name__ == "__main__":
    raise SystemExit(main())
