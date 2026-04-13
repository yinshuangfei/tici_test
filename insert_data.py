#!/usr/bin/env python3
"""Read CSV rows and insert them into test.hdfs_log."""

import argparse
import concurrent.futures
import csv
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

import mysql.connector


DEFAULT_HOST = "10.2.12.81"
DEFAULT_PORT = 9529
DEFAULT_USER = "root"
DEFAULT_DATABASE = "test"
DEFAULT_TABLE = "hdfs_log"
DEFAULT_COLUMNS = ("timestamp", "severity_text", "body", "tenant_id")
DEFAULT_BATCH_SIZE = 1000
DEFAULT_ROW_LIMIT = 100000
DEFAULT_PROGRESS_INTERVAL = 3.0
DEFAULT_INSERT_RETRY_COUNT = 10
DEFAULT_INSERT_RETRY_INTERVAL = 1.0
DEFAULT_CSV_FILE = "data/hdfs-logs-multitenants.csv"


def quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("identifier must not be empty")
    return f"`{identifier.replace('`', '``')}`"


def quote_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def build_table_names(base_name: str, count: int) -> List[str]:
    if count < 1:
        raise ValueError("table count must be >= 1")
    if count == 1:
        return [base_name]
    return [f"{base_name}_{idx}" for idx in range(1, count + 1)]


@dataclass
class MySQLClient:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    user: str = DEFAULT_USER
    database: str = DEFAULT_DATABASE
    password: str = ""

    def connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )


def normalize_delimiter(value: str) -> str:
    mapping = {
        r"\t": "\t",
        r"\n": "\n",
        r"\r": "\r",
    }
    return mapping.get(value, value)


def parse_row(raw_row: Sequence[str], line_number: int) -> Tuple[int, str, str, int]:
    if len(raw_row) != 4:
        raise ValueError(f"line {line_number}: expected 4 columns, got {len(raw_row)}")

    timestamp_raw, severity_text, body, tenant_id_raw = raw_row
    try:
        timestamp = int(timestamp_raw.strip())
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid timestamp {timestamp_raw!r}") from exc

    try:
        tenant_id = int(tenant_id_raw.strip())
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid tenant_id {tenant_id_raw!r}") from exc

    return timestamp, severity_text, body, tenant_id


def read_csv_batches(
    csv_path: Path,
    *,
    encoding: str,
    delimiter: str,
    has_header: bool,
    row_limit: int,
    batch_size: int,
) -> Iterator[List[Tuple[int, str, str, int]]]:
    with csv_path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        if has_header:
            header = next(reader, None)
            if header is None:
                return
        start_line = 2 if has_header else 1
        emitted = 0
        batch: List[Tuple[int, str, str, int]] = []
        for offset, row in enumerate(reader, start=start_line):
            if row_limit > 0 and emitted >= row_limit:
                break
            if not row:
                continue
            batch.append(parse_row(row, offset))
            emitted += 1
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def build_insert_sql(database: str, table: str, rows: Sequence[Tuple[int, str, str, int]]) -> str:
    target = f"{quote_identifier(database)}.{quote_identifier(table)}"
    columns = ", ".join(quote_identifier(column) for column in DEFAULT_COLUMNS)
    values = []
    for timestamp, severity_text, body, tenant_id in rows:
        values.append(
            "("
            f"{timestamp}, "
            f"{quote_literal(severity_text)}, "
            f"{quote_literal(body)}, "
            f"{tenant_id}"
            ")"
        )
    return f"INSERT INTO {target} ({columns}) VALUES\n" + ",\n".join(values) + ";"


def build_insert_statement(database: str, table: str) -> str:
    target = f"{quote_identifier(database)}.{quote_identifier(table)}"
    columns = ", ".join(quote_identifier(column) for column in DEFAULT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(DEFAULT_COLUMNS))
    return f"INSERT INTO {target} ({columns}) VALUES ({placeholders})"


def reconnect_client(
    client: MySQLClient,
    connection: Optional[mysql.connector.MySQLConnection],
    cursor,
):
    if cursor is not None:
        cursor.close()
    if connection is not None:
        connection.close()
    connection = client.connect()
    cursor = connection.cursor()
    return connection, cursor


def add_insert_data_args(parser: argparse.ArgumentParser, *, csv_file_nargs: str = "?") -> None:
    parser.add_argument(
        "csv_file",
        nargs=csv_file_nargs,
        help=f"CSV file path, default: {DEFAULT_CSV_FILE}",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MySQL host, default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MySQL port, default: {DEFAULT_PORT}")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"MySQL user, default: {DEFAULT_USER}")
    parser.add_argument("--password", default="", help="MySQL password, optional")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"Database name, default: {DEFAULT_DATABASE}")
    parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Base table name, default: {DEFAULT_TABLE}")
    parser.add_argument("--count", type=int, default=1, help="Table count. >1 creates <table>_<num>")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per batch insert statement")
    parser.add_argument(
        "--row-limit",
        type=int,
        default=DEFAULT_ROW_LIMIT,
        help=f"Maximum number of data rows to import, default: {DEFAULT_ROW_LIMIT}",
    )
    parser.add_argument(
        "--print-interval",
        type=float,
        default=DEFAULT_PROGRESS_INTERVAL,
        help="Progress output interval in seconds, default: 3",
    )
    parser.add_argument("--encoding", default="utf-8", help="CSV file encoding, default: utf-8")
    parser.add_argument("--delimiter", default=",", help=r"CSV delimiter, default: ','; use '\t' for tab")
    parser.add_argument("--has-header", action="store_true", help="Skip the first CSV row as header")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only, do not execute")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Insert CSV data into test.hdfs_log")
    add_insert_data_args(parser)
    return parser


def build_client(args: argparse.Namespace) -> MySQLClient:
    return MySQLClient(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )


def format_table_target(database: str, table: str) -> str:
    return f"{database}.{table}"


def build_table_insert_args(args: argparse.Namespace, table_name: str) -> argparse.Namespace:
    table_args = argparse.Namespace(**vars(args))
    table_args.table = table_name
    table_args.count = 1
    return table_args


def run_insert_data_for_table(args: argparse.Namespace) -> int:
    csv_path = Path(getattr(args, "csv_file", None) or DEFAULT_CSV_FILE)
    target_name = format_table_target(args.database, args.table)
    batches = read_csv_batches(
        csv_path,
        encoding=args.encoding,
        delimiter=normalize_delimiter(args.delimiter),
        has_header=args.has_header,
        row_limit=args.row_limit,
        batch_size=args.batch_size,
    )
    client = build_client(args)
    insert_statement = build_insert_statement(args.database, args.table)
    imported_rows = 0
    started_at = time.monotonic()
    last_progress_at = started_at
    connection = None
    cursor = None
    try:
        if not args.dry_run:
            connection = client.connect()
            cursor = connection.cursor()

        for batch in batches:
            sql = build_insert_sql(args.database, args.table, batch)
            if args.dry_run:
                print(sql)
            else:
                for attempt in range(1, DEFAULT_INSERT_RETRY_COUNT + 1):
                    try:
                        cursor.executemany(insert_statement, batch)
                        connection.commit()
                        break
                    except mysql.connector.Error as exc:
                        try:
                            connection.rollback()
                        except mysql.connector.Error:
                            pass

                        if attempt == DEFAULT_INSERT_RETRY_COUNT:
                            print(f"[{target_name}] [FATAL ERROR] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} "
                                  f"rows={len(batch)} error={exc}",
                                  file=sys.stderr)
                            raise

                        print(
                            f"[{target_name}] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} "
                            f"rows={len(batch)} retry_in={DEFAULT_INSERT_RETRY_INTERVAL:.1f}s error={exc}",
                            file=sys.stderr,
                        )
                        time.sleep(DEFAULT_INSERT_RETRY_INTERVAL)
                        connection, cursor = reconnect_client(client, connection, cursor)
            imported_rows += len(batch)
            now = time.monotonic()
            if args.print_interval == 0 or now - last_progress_at >= args.print_interval:
                elapsed = now - started_at
                print(f"[{target_name}] progress imported {imported_rows} rows in {elapsed:.1f}s")
                last_progress_at = now
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    if imported_rows == 0:
        print(f"[{target_name}] no data rows found")
        return 0

    elapsed = time.monotonic() - started_at
    print(f"[{target_name}] completed import {imported_rows} rows in {elapsed:.1f}s")
    return 0


def run_insert_data(args: argparse.Namespace, parser: Optional[argparse.ArgumentParser] = None) -> int:
    if args.batch_size < 1:
        print("batch size must be >= 1", file=sys.stderr)
        return 2
    if args.count < 1:
        print("table count must be >= 1", file=sys.stderr)
        return 2
    if args.row_limit < 0:
        print("row limit must be >= 0", file=sys.stderr)
        return 2
    if args.print_interval < 0:
        print("print interval must be >= 0", file=sys.stderr)
        return 2

    csv_path = Path(getattr(args, "csv_file", None) or DEFAULT_CSV_FILE)
    if not csv_path.is_file():
        print(f"csv file not found: {csv_path}", file=sys.stderr)
        return 2

    try:
        table_names = build_table_names(args.table, args.count)
        if len(table_names) > 1:
            joined_targets = ", ".join(format_table_target(args.database, table_name) for table_name in table_names)
            mode = "dry-run" if args.dry_run else "execute"
            print(f"[insert-data] mode={mode} tables={joined_targets}")

        if args.dry_run or len(table_names) == 1:
            for table_name in table_names:
                exit_code = run_insert_data_for_table(build_table_insert_args(args, table_name))
                if exit_code != 0:
                    return exit_code
            return 0

        print(f"[insert-data] threads={len(table_names)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(table_names)) as executor:
            future_map = {
                executor.submit(run_insert_data_for_table, build_table_insert_args(args, table_name)): table_name
                for table_name in table_names
            }
            for future in concurrent.futures.as_completed(future_map):
                table_name = future_map[future]
                exit_code = future.result()
                if exit_code != 0:
                    print(
                        f"[insert-data] failed target={format_table_target(args.database, table_name)} "
                        f"exit_code={exit_code}",
                        file=sys.stderr,
                    )
                    return exit_code
        return 0
    except mysql.connector.Error as exc:
        print(f"database operation failed: {exc}", file=sys.stderr)
        return 1
    except UnicodeDecodeError as exc:
        print(f"failed to decode csv file with encoding {args.encoding}: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
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
    return run_insert_data(args, parser)


if __name__ == "__main__":
    raise SystemExit(main())
