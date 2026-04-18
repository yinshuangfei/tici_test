#!/usr/bin/env python3
"""Concurrent freshness polling helpers with bounded capacity."""

import argparse
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

import mysql.connector


DEFAULT_HOST = "10.2.12.81"
DEFAULT_PORT = 9529
DEFAULT_USER = "root"
DEFAULT_DATABASE = "test"
DEFAULT_TABLE = "hdfs_log"
DEFAULT_MAX_WORKERS = 10
DEFAULT_FRESHNESS_TIMEOUT = 30 * 60.0
DEFAULT_FRESHNESS_POLL_INTERVAL = 5.0
DEFAULT_FRESHNESS_WHERE = "where fts_match_word('china',body) or not fts_match_word('china',body)"
FRESHNESS_LOG_SUFFIX = time.strftime("%Y%m%d_%H%M%S")


def with_timestamp_suffix(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")


DEFAULT_FRESHNESS_PROGRESS_LOG = with_timestamp_suffix(
    Path("log/freshness_progress.log"),
    FRESHNESS_LOG_SUFFIX,
)
DEFAULT_FRESHNESS_RESULT_LOG = with_timestamp_suffix(
    Path("log/freshness_result.log"),
    FRESHNESS_LOG_SUFFIX,
)


def quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("identifier must not be empty")
    return f"`{identifier.replace('`', '``')}`"


def append_progress_log(output_file: Path, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def emit_log(message: str, *, output_file: Optional[Path] = None, stderr: bool = False) -> None:
    if stderr:
        print(message, file=sys.stderr)
    else:
        print(message)
    if output_file is not None:
        append_progress_log(output_file, message)


def build_count_sql(database: str, table: str) -> str:
    target = f"{quote_identifier(database)}.{quote_identifier(table)}"
    return f"SELECT COUNT(*) FROM {target} {DEFAULT_FRESHNESS_WHERE}"


@dataclass(frozen=True)
class MySQLConnectionConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    user: str = DEFAULT_USER
    password: str = ""
    database: str = DEFAULT_DATABASE

    def connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )


@dataclass(frozen=True)
class FreshnessTask:
    target_row_count: int
    database: str = DEFAULT_DATABASE
    table: str = DEFAULT_TABLE
    connection: MySQLConnectionConfig = MySQLConnectionConfig()
    poll_interval: float = DEFAULT_FRESHNESS_POLL_INTERVAL
    timeout: float = DEFAULT_FRESHNESS_TIMEOUT

    @property
    def target_name(self) -> str:
        return f"{self.database}.{self.table}"


class FreshnessCheckerPool:
    def __init__(
        self,
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
        progress_log: Path = DEFAULT_FRESHNESS_PROGRESS_LOG,
        result_log: Path = DEFAULT_FRESHNESS_RESULT_LOG,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.max_workers = max_workers
        self.progress_log = progress_log
        self.result_log = result_log
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="freshness")
        self._lock = threading.Lock()
        self._active_futures: Set[Future] = set()

    def submit(self, task: FreshnessTask) -> Future:
        with self._lock:
            self._active_futures = {future for future in self._active_futures if not future.done()}
            if len(self._active_futures) >= self.max_workers:
                message = (
                    f"[{task.target_name}] freshness task rejected "
                    f"active_tasks={len(self._active_futures)} pool_size={self.max_workers}"
                )
                emit_log(message, output_file=self.result_log, stderr=True)
                raise RuntimeError(message)
            future = self._executor.submit(self._run_task, task)
            self._active_futures.add(future)
            future.add_done_callback(self._discard_future)
            return future

    def active_task_count(self) -> int:
        with self._lock:
            self._active_futures = {future for future in self._active_futures if not future.done()}
            return len(self._active_futures)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def _discard_future(self, future: Future) -> None:
        with self._lock:
            self._active_futures.discard(future)

    def _run_task(self, task: FreshnessTask) -> bool:
        started_at = time.monotonic()
        connection = None
        cursor = None
        try:
            connection = task.connection.connect()
            cursor = connection.cursor()
            append_progress_log(
                self.progress_log,
                f"[{task.target_name}] freshness start target_row_count={task.target_row_count} "
                f"poll_interval={task.poll_interval:.1f}s timeout={task.timeout:.1f}s",
            )
            while True:
                cursor.execute(build_count_sql(task.database, task.table))
                row = cursor.fetchone()
                if row is None:
                    raise ValueError(f"failed to query row count for {task.target_name}")

                current_row_count = int(row[0])
                elapsed = time.monotonic() - started_at
                append_progress_log(
                    self.progress_log,
                    f"[{task.target_name}] freshness poll elapsed={elapsed:.1f}s "
                    f"target_row_count={task.target_row_count} current_row_count={current_row_count}",
                )

                if current_row_count >= task.target_row_count:
                    message = (
                        f"[{task.target_name}] freshness reached elapsed={elapsed:.1f}s "
                        f"target_row_count={task.target_row_count} current_row_count={current_row_count}"
                    )
                    emit_log(message, output_file=self.result_log)
                    return True

                if elapsed >= task.timeout:
                    message = (
                        f"[{task.target_name}] freshness timeout elapsed={elapsed:.1f}s "
                        f"target_row_count={task.target_row_count} current_row_count={current_row_count} "
                        f"timeout={task.timeout:.1f}s"
                    )
                    emit_log(message, output_file=self.result_log, stderr=True)
                    return False

                time.sleep(task.poll_interval)
        except Exception as exc:
            message = f"[{task.target_name}] freshness failed error={exc}"
            emit_log(message, output_file=self.result_log, stderr=True)
            raise
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll table freshness until target row count is visible")
    parser.add_argument("target_row_count", type=int, help="Target visible row count")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MySQL host, default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MySQL port, default: {DEFAULT_PORT}")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"MySQL user, default: {DEFAULT_USER}")
    parser.add_argument("--password", default="", help="MySQL password, optional")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"Database name, default: {DEFAULT_DATABASE}")
    parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Table name, default: {DEFAULT_TABLE}")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_FRESHNESS_POLL_INTERVAL,
        help=f"Polling interval in seconds, default: {DEFAULT_FRESHNESS_POLL_INTERVAL}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_FRESHNESS_TIMEOUT,
        help=f"Timeout in seconds, default: {int(DEFAULT_FRESHNESS_TIMEOUT)}",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Freshness worker pool size, default: {DEFAULT_MAX_WORKERS}",
    )
    return parser


def run_once(args: argparse.Namespace) -> int:
    if args.target_row_count < 0:
        print("target_row_count must be >= 0", file=sys.stderr)
        return 2
    if args.poll_interval <= 0:
        print("poll_interval must be > 0", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("timeout must be > 0", file=sys.stderr)
        return 2
    if args.max_workers < 1:
        print("max_workers must be >= 1", file=sys.stderr)
        return 2

    connection = MySQLConnectionConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    task = FreshnessTask(
        target_row_count=args.target_row_count,
        database=args.database,
        table=args.table,
        connection=connection,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )
    pool = FreshnessCheckerPool(max_workers=args.max_workers)
    try:
        future = pool.submit(task)
        return 0 if future.result() else 1
    finally:
        pool.shutdown(wait=True)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
