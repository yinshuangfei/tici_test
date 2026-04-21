#!/usr/bin/env python3
"""Analyze freshness result logs and print grouped statistics by file."""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\] "
    r"\[(?P<database>[^.]+)\.(?P<table>[^\]]+)\] "
    r"freshness reached "
    r"elapsed=(?P<elapsed>\d+(?:\.\d+)?)s "
    r"baseline_row_count=(?P<baseline>\d+) "
    r"imported_rows=(?P<imported>\d+) "
    r"visible_rows=(?P<visible>\d+) "
    r"total_rows=(?P<total>\d+)$"
)


@dataclass
class FreshnessEntry:
    elapsed: float
    baseline_row_count: int
    imported_rows: int
    visible_rows: int
    total_rows: int


@dataclass
class InvalidLine:
    file_path: Path
    line_number: int
    line: str
    reason: str


@dataclass
class GroupStats:
    elapsed_sum: float = 0.0
    visible_sum: int = 0
    lines: int = 0

    def add(self, entry: FreshnessEntry) -> None:
        self.elapsed_sum += entry.elapsed
        self.visible_sum += entry.visible_rows
        self.lines += 1

    @property
    def avg_elapsed(self) -> float:
        return self.elapsed_sum / self.lines

    @property
    def avg_inserted(self) -> float:
        return self.visible_sum / self.lines


@dataclass
class FileStats:
    grouped: Dict[str, GroupStats]
    invalid_lines: List[InvalidLine]


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze freshness_result_*.log files and print grouped statistics by file.",
    )
    parser.add_argument(
        "--log-dir",
        default="log",
        help="Directory that contains freshness result logs. Default: %(default)s",
    )
    parser.add_argument(
        "--pattern",
        default="freshness_result_*.log",
        help="Glob pattern used under --log-dir. Default: %(default)s",
    )
    parser.add_argument(
        "--show-invalid",
        action="store_true",
        help="Print invalid lines where total_rows != baseline_row_count + imported_rows.",
    )
    if not argv:
        parser.print_help()
        raise SystemExit(0)
    return parser.parse_args(argv)


def iter_log_files(log_dir: Path, pattern: str) -> List[Path]:
    return sorted(path for path in log_dir.glob(pattern) if path.is_file())


def parse_entry(line: str) -> Optional[FreshnessEntry]:
    match = LINE_PATTERN.match(line)
    if match is None:
        return None
    return FreshnessEntry(
        elapsed=float(match.group("elapsed")),
        baseline_row_count=int(match.group("baseline")),
        imported_rows=int(match.group("imported")),
        visible_rows=int(match.group("visible")),
        total_rows=int(match.group("total")),
    )


def group_name(baseline_row_count: int) -> str:
    bucket_floor = (baseline_row_count // 10000) * 10000
    return f"group-{bucket_floor}"


def analyze_file(file_path: Path) -> FileStats:
    grouped: Dict[str, GroupStats] = {}
    invalid_lines: List[InvalidLine] = []

    with file_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.rstrip("\n")
            entry = parse_entry(line)
            if entry is None:
                continue
            if entry.total_rows != entry.baseline_row_count + entry.imported_rows:
                invalid_lines.append(
                    InvalidLine(
                        file_path=file_path,
                        line_number=line_number,
                        line=line,
                        reason=(
                            "total_rows != baseline_row_count + imported_rows "
                            f"({entry.total_rows} != "
                            f"{entry.baseline_row_count} + {entry.imported_rows})"
                        ),
                    )
                )
                continue
            current_group = group_name(entry.baseline_row_count)
            grouped.setdefault(current_group, GroupStats()).add(entry)

    return FileStats(
        grouped=grouped,
        invalid_lines=invalid_lines,
    )


def analyze_logs(file_paths: Iterable[Path]) -> Dict[Path, FileStats]:
    results: Dict[Path, FileStats] = {}
    for file_path in file_paths:
        results[file_path] = analyze_file(file_path)
    return results


def merge_grouped_stats(results: Dict[Path, FileStats]) -> Dict[str, GroupStats]:
    merged: Dict[str, GroupStats] = {}
    for file_stats in results.values():
        for name, stats in file_stats.grouped.items():
            target = merged.setdefault(name, GroupStats())
            target.elapsed_sum += stats.elapsed_sum
            target.visible_sum += stats.visible_sum
            target.lines += stats.lines
    return merged


def sorted_group_names(grouped: Dict[str, GroupStats]) -> List[str]:
    return sorted(grouped, key=lambda item: int(item.split("-", 1)[1]))


def print_grouped_stats(title: str, grouped: Dict[str, GroupStats]) -> None:
    print(title)
    print("起始行数量级, 平均插入行数, 统计条目数, 平均耗时(s)")
    for name in sorted_group_names(grouped):
        group_stats = grouped[name]
        print(
            f"{name}, {group_stats.avg_inserted:.2f}, "
            f"{group_stats.lines}, {group_stats.avg_elapsed:.2f}"
        )
    print()


def main() -> int:
    args = parse_args(sys.argv[1:])
    log_dir = Path(args.log_dir)
    file_paths = iter_log_files(log_dir, args.pattern)

    if not file_paths:
        print(f"no log files found in {log_dir} matching {args.pattern}")
        return 1

    results = analyze_logs(file_paths)

    for file_path in file_paths:
        stats_by_file = results[file_path]
        print_grouped_stats(file_path.name, stats_by_file.grouped)
        if args.show_invalid and stats_by_file.invalid_lines:
            print("invalid_details")
            for item in stats_by_file.invalid_lines:
                print(f"{item.file_path}:{item.line_number}: {item.reason}")
                print(item.line)
        print()

    print_grouped_stats("Summarize", merge_grouped_stats(results))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
