#!/usr/bin/env python3
"""CLI helpers for TiCI table management and IMPORT INTO."""

import argparse
import concurrent.futures
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import insert_data
import mysql.connector


DEFAULT_HOST = "10.2.12.81"
DEFAULT_PORT = 9529
DEFAULT_USER = "root"
DEFAULT_DATABASE = "test"
DEFAULT_TABLE = "hdfs_log"
DEFAULT_INDEX = "ft_idx"
DEFAULT_IMPORT_SOURCE = (
    "s3://data/import-src/hdfs-logs-multitenants.csv"
    "?access-key=minioadmin&secret-access-key=minioadmin"
    "&endpoint=http://10.2.12.81:19008&force-path-style=true"
)
DEFAULT_IMPORT_SORT_DIR = (
    "s3://ticidefaultbucket/tici/import-sort"
    "?access-key=minioadmin&secret-access-key=minioadmin"
    "&endpoint=http://10.2.12.81:19008&force-path-style=true"
)
DEFAULT_IMPORT_COLUMNS = ("timestamp", "severity_text", "body", "tenant_id")
DEFAULT_IMPORT_THREAD = 8
DEFAULT_AUTO_ROW_LIMIT = 100000
DEFAULT_QUERY_SQL = (
    "select '1' as table_idx, count(*) from test.hdfs_log "
    "where fts_match_word('china',body) or not fts_match_word('china',body);"
)
SQL_BARE_VALUE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    mysql_bin: str = "mysql"
    comments: bool = True

    def connect(self) -> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def base_command(self) -> List[str]:
        cmd = [
            self.mysql_bin,
            "--host",
            self.host,
            "--port",
            str(self.port),
            "-u",
            self.user,
        ]
        if self.comments:
            cmd.append("--comments")
        if self.password:
            cmd.append(f"-p{self.password}")
        return cmd

    def base_command_text(self) -> str:
        return shlex.join(self.base_command())

    def execute(self, sql: str, *, echo: bool = False) -> None:
        cmd = self.base_command() + ["-D", self.database, "-e", sql]
        if echo:
            print(f"$ {shlex.join(cmd)}")
            print(sql)
        subprocess.run(cmd, check=True)

    def query(self, sql: str, *, echo: bool = False) -> str:
        if echo:
            cmd = self.base_command() + ["-D", self.database, "-e", sql]
            print(f"$ {shlex.join(cmd)}")
            print(sql)
        connection = self.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            lines = ["\t".join("" if value is None else str(value) for value in row) for row in cursor.fetchall()]
            return "\n".join(lines)
        finally:
            cursor.close()
            connection.close()


def create_table_sql(database: str, table_name: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {quote_identifier(database)}.{quote_identifier(table_name)} (
    id BIGINT AUTO_INCREMENT,
    timestamp BIGINT,
    severity_text VARCHAR(50),
    body TEXT,
    tenant_id INT,
    PRIMARY KEY (tenant_id, id)
);"""


def drop_table_sql(database: str, table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {quote_identifier(database)}.{quote_identifier(table_name)};"


def add_index_sql(database: str, table_name: str, index_name: str, column_name: str) -> str:
    return (
        f"ALTER TABLE {quote_identifier(database)}.{quote_identifier(table_name)} "
        f"ADD FULLTEXT INDEX {quote_identifier(index_name)}({quote_identifier(column_name)});"
    )


def drop_index_sql(database: str, table_name: str, index_name: str) -> str:
    return (
        f"ALTER TABLE {quote_identifier(database)}.{quote_identifier(table_name)} "
        f"DROP INDEX {quote_identifier(index_name)};"
    )


def import_into_sql(
    database: str,
    table_name: str,
    source: str,
    columns: Sequence[str],
    cloud_storage_uri: str,
    thread: int,
    detached: bool,
    disable_precheck: bool,
    with_options: Sequence[str],
) -> str:
    target = f"{quote_identifier(database)}.{quote_identifier(table_name)}"
    column_clause = ""
    if columns:
        quoted_columns = ", ".join(quote_identifier(column) for column in columns)
        column_clause = f" ({quoted_columns})"

    clauses = [f"CLOUD_STORAGE_URI={quote_literal(cloud_storage_uri)}"]
    if detached:
        clauses.append("DETACHED")
    if disable_precheck:
        clauses.append("DISABLE_PRECHECK")
    clauses.append(f"THREAD={thread}")
    clauses.extend(with_options)

    return (
        f"IMPORT INTO {target}{column_clause}\n"
        f"FROM {quote_literal(source)}\n"
        f"WITH \n    " + ",\n    ".join(clauses) + ";"
    )


def parse_columns(raw_columns: str) -> List[str]:
    if not raw_columns:
        return []
    return [column.strip() for column in raw_columns.split(",") if column.strip()]


def parse_with_options(values: Iterable[str]) -> List[str]:
    options = []
    for value in values:
        item = value.strip()
        if not item:
            continue
        if "=" in item:
            key, raw_value = item.split("=", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            if not key:
                raise ValueError(f"invalid WITH option: {value}")
            if raw_value.lower() in {"true", "false", "null"}:
                options.append(f"{key.upper()}={raw_value.lower()}")
            elif SQL_BARE_VALUE_RE.match(raw_value):
                options.append(f"{key.upper()}={raw_value.upper()}")
            else:
                try:
                    float(raw_value)
                    options.append(f"{key.upper()}={raw_value}")
                except ValueError:
                    options.append(f"{key.upper()}={quote_literal(raw_value)}")
        else:
            options.append(item.upper())
    return options


def format_table_target(database: str, table_name: str) -> str:
    return f"{database}.{table_name}"


def print_command_summary(command: str, database: str, table_names: Sequence[str], *, dry_run: bool) -> None:
    joined_tables = ", ".join(format_table_target(database, name) for name in table_names)
    mode = "dry-run" if dry_run else "execute"
    print(f"[{command}] mode={mode} tables={joined_tables}")


def print_query_summary(command: str, *, dry_run: bool) -> None:
    mode = "dry-run" if dry_run else "execute"
    print(f"[{command}] mode={mode}")


def build_query_sql(
    args: argparse.Namespace,
    table_name: str,
    table_idx: int,
    *,
    tikv: Optional[bool] = None,
) -> str:
    target_name = format_table_target(args.database, table_name)
    if tikv is None:
        tikv = getattr(args, "tikv", False)
    if tikv:
        return f"select '{table_idx}' as table_idx,count(*) from {table_name};"
    if "{table}" in args.sql:
        return args.sql.replace("{table}", target_name)
    if args.sql == DEFAULT_QUERY_SQL:
        return (
            args.sql.replace("select '1' as table_idx", f"select '{table_idx}' as table_idx", 1)
            .replace(f"{DEFAULT_DATABASE}.{DEFAULT_TABLE}", target_name)
        )
    return args.sql


def run_sqls(
    client: MySQLClient,
    sql_statements: Sequence[Tuple[str, str, str, Optional[str]]],
    dry_run: bool,
    *,
    parallel: bool = False,
) -> None:
    if dry_run or not parallel or len(sql_statements) <= 1:
        for action_name, target_name, sql, detail in sql_statements:
            print(f"[{action_name}] target={target_name}")
            if detail is not None:
                print(f"[{action_name}] {detail}")
            if dry_run:
                print(sql)
            else:
                client.execute(sql)
        return

    print(f"[sql] threads={len(sql_statements)}")

    def run_one_sql(item: Tuple[str, str, str, Optional[str]]) -> Tuple[str, str, Optional[str]]:
        action_name, target_name, sql, detail = item
        print(f"[{action_name}] target={target_name}")
        if detail is not None:
            print(f"[{action_name}] {detail}")
        client.execute(sql)
        return action_name, target_name, detail

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sql_statements)) as executor:
        future_map = {executor.submit(run_one_sql, item): item for item in sql_statements}
        for future in concurrent.futures.as_completed(future_map):
            action_name, target_name, _, detail = future_map[future]
            future.result()
            done_suffix = f" {detail}" if detail is not None else ""
            print(f"[{action_name}] done target={target_name}{done_suffix}")


def normalize_query_output(output: str) -> str:
    return "\n".join(line.rstrip() for line in output.strip().splitlines())


def run_queries(
    client: MySQLClient,
    sql_statements: Sequence[Tuple[str, str, str, Optional[str]]],
    dry_run: bool,
    *,
    parallel: bool = False,
) -> None:
    if dry_run or not parallel or len(sql_statements) <= 1:
        for action_name, target_name, sql, detail in sql_statements:
            
            if detail is not None:
                print(f"[{action_name}] {detail}")
            if dry_run:
                print(sql)
            else:
                output = client.query(sql)
                if output:
                    print(f"[{action_name}] target={target_name}, {output}")
        return

    print(f"[query] threads={len(sql_statements)}")

    def run_one_query(item: Tuple[str, str, str, Optional[str]]) -> Tuple[str, str, Optional[str], str]:
        action_name, target_name, sql, detail = item
        print(f"[{action_name}] target={target_name}")
        if detail is not None:
            print(f"[{action_name}] {detail}")
        output = client.query(sql)
        return action_name, target_name, detail, output

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(sql_statements)) as executor:
        future_map = {executor.submit(run_one_query, item): item for item in sql_statements}
        for future in concurrent.futures.as_completed(future_map):
            action_name, target_name, detail, output = future.result()
            if output:
                print(output)
            done_suffix = f" {detail}" if detail is not None else ""
            print(f"[{action_name}] done target={target_name}{done_suffix}")


def run_query(args: argparse.Namespace) -> int:
    if args.query_loop_count < 1:
        print("query loop count must be >= 1", file=sys.stderr)
        return 2
    if args.count < 1:
        print("query table count must be >= 1", file=sys.stderr)
        return 2

    print_query_summary(args.command, dry_run=args.dry_run)
    query_table_names = build_table_names(args.table, args.count)
    print_command_summary(args.command, args.database, query_table_names, dry_run=args.dry_run)

    query_sqls = []
    for idx in range(1, args.query_loop_count + 1):
        print(f"[query] round={idx}/{args.query_loop_count}")
        for table_idx, table_name in enumerate(query_table_names, start=1):
            target_name = format_table_target(args.database, table_name)
            sql = build_query_sql(args, table_name, table_idx)
            query_sqls.append(("query", target_name, sql, None))

    run_queries(build_client(args), query_sqls, args.dry_run, parallel=args.query_loop_count != 1)
    return 0


def run_check(args: argparse.Namespace) -> int:
    if args.query_loop_count < 1:
        print("check loop count must be >= 1", file=sys.stderr)
        return 2
    if args.count < 1:
        print("check table count must be >= 1", file=sys.stderr)
        return 2

    print_query_summary(args.command, dry_run=args.dry_run)
    query_table_names = build_table_names(args.table, args.count)
    print_command_summary(args.command, args.database, query_table_names, dry_run=args.dry_run)

    client = build_client(args)
    for idx in range(1, args.query_loop_count + 1):
        round_detail = f"round={idx}/{args.query_loop_count}"
        print(f"[check] {round_detail}")
        for table_idx, table_name in enumerate(query_table_names, start=1):
            target_name = format_table_target(args.database, table_name)
            normal_sql = build_query_sql(args, table_name, table_idx, tikv=False)
            tikv_sql = build_query_sql(args, table_name, table_idx, tikv=True)

            if args.dry_run:
                print("[check] variant=normal")
                print(normal_sql)
                print("[check] variant=tikv")
                print(tikv_sql)
                continue

            normal_output = normalize_query_output(client.query(normal_sql))
            tikv_output = normalize_query_output(client.query(tikv_sql))
            if normal_output != tikv_output:
                print(f"[check] mismatch target={target_name} {round_detail}", file=sys.stderr)
                print("[check] normal result:", file=sys.stderr)
                print(f"tici: {normal_output or '<empty>'}", file=sys.stderr)
                print("[check] tikv result:", file=sys.stderr)
                print(f"tikv: {tikv_output or '<empty>'}", file=sys.stderr)
                return 1
            print(f"[check] match target={target_name} {normal_output}")

    return 0


def add_common_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MySQL host, default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MySQL port, default: {DEFAULT_PORT}")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"MySQL user, default: {DEFAULT_USER}")
    parser.add_argument("--password", default="", help="MySQL password, optional")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"Database name, default: {DEFAULT_DATABASE}")
    parser.add_argument("--mysql-bin", default="mysql", help="Path to mysql client binary")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only, do not execute")


def add_common_table_args(parser: argparse.ArgumentParser, *, include_count: bool = True) -> None:
    parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Base table name, default: {DEFAULT_TABLE}")
    if include_count:
        parser.add_argument("--count", type=int, default=1, help="Table count. >1 creates <table>_<num>")


def add_insert_runtime_args(
    parser: argparse.ArgumentParser,
    *,
    default_row_limit: int = insert_data.DEFAULT_ROW_LIMIT,
) -> None:
    parser.add_argument(
        "csv_file",
        nargs="?",
        help=f"CSV file path, default: {insert_data.DEFAULT_CSV_FILE}",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=insert_data.DEFAULT_BATCH_SIZE,
        help=f"Rows per batch insert statement, default: {insert_data.DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=default_row_limit,
        help=f"Maximum number of data rows to import, default: {default_row_limit}",
    )
    parser.add_argument(
        "--freshness-batch",
        type=int,
        default=insert_data.DEFAULT_FRESHNESS_BATCH,
        help=(
            "Split row-limit into sequential batches for each insert/freshness round, "
            f"default: {insert_data.DEFAULT_FRESHNESS_BATCH}"
        ),
    )
    parser.add_argument(
        "--print-interval",
        type=float,
        default=insert_data.DEFAULT_PROGRESS_INTERVAL,
        help=f"Progress output interval in seconds, default: {insert_data.DEFAULT_PROGRESS_INTERVAL:g}",
    )
    parser.add_argument("--encoding", default="utf-8", help="CSV file encoding, default: utf-8")
    parser.add_argument("--delimiter", default=",", help=r"CSV delimiter, default: ','; use '\t' for tab")
    parser.add_argument("--has-header", action="store_true", help="Skip the first CSV row as header")
    parser.add_argument(
        "--no-freshness",
        dest="freshness",
        action="store_false",
        help=(
            "Disable the default post-insert freshness check "
            f"that polls every {int(insert_data.DEFAULT_FRESHNESS_POLL_INTERVAL)} seconds for up to "
            f"{int(insert_data.DEFAULT_FRESHNESS_TIMEOUT // 60)} minutes"
        ),
    )
    parser.set_defaults(freshness=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Table tools for TiCI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create-table", help="Create table(s)")
    add_common_connection_args(create_parser)
    add_common_table_args(create_parser)

    drop_parser = subparsers.add_parser("drop-table", help="Drop table(s)")
    add_common_connection_args(drop_parser)
    add_common_table_args(drop_parser)

    add_index_parser = subparsers.add_parser("add-index", help="Add FULLTEXT index to table(s)")
    add_common_connection_args(add_index_parser)
    add_common_table_args(add_index_parser)
    add_index_parser.add_argument("--index-name", default=DEFAULT_INDEX, help=f"Index name, default: {DEFAULT_INDEX}")
    add_index_parser.add_argument("--column", default="body", help="Indexed column, default: body")

    drop_index_parser = subparsers.add_parser("drop-index", help="Drop index from table(s)")
    add_common_connection_args(drop_index_parser)
    add_common_table_args(drop_index_parser)
    drop_index_parser.add_argument("--index-name", default=DEFAULT_INDEX, help=f"Index name, default: {DEFAULT_INDEX}")

    import_parser = subparsers.add_parser("import-into", help="Run IMPORT INTO for a table")
    add_common_connection_args(import_parser)
    add_common_table_args(import_parser, include_count=False)
    import_parser.add_argument("--source", default=DEFAULT_IMPORT_SOURCE, help="Import source path or URI")
    import_parser.add_argument(
        "--columns",
        default=",".join(DEFAULT_IMPORT_COLUMNS),
        help="Comma-separated target columns",
    )
    import_parser.add_argument(
        "--cloud-storage-uri",
        default=DEFAULT_IMPORT_SORT_DIR,
        help="Cloud storage URI used by IMPORT INTO for sorted data",
    )
    import_parser.add_argument(
        "--thread",
        type=int,
        default=DEFAULT_IMPORT_THREAD,
        help=f"IMPORT INTO thread count, default: {DEFAULT_IMPORT_THREAD}",
    )
    import_parser.add_argument(
        "--no-detached",
        action="store_true",
        help="Disable the default DETACHED option",
    )
    import_parser.add_argument(
        "--no-disable-precheck",
        action="store_true",
        help="Disable the default DISABLE_PRECHECK option",
    )
    import_parser.add_argument(
        "--with-option",
        action="append",
        default=[],
        help="Extra WITH option, e.g. max_write_speed=1048576",
    )

    query_parser = subparsers.add_parser("query", help="Run a read query")
    add_common_connection_args(query_parser)
    query_parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Base table name, default: {DEFAULT_TABLE}")
    query_parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Table count for query targets. >1 creates <table>_<num>",
    )
    query_parser.add_argument(
        "--sql",
        default=DEFAULT_QUERY_SQL,
        help="Query SQL text",
    )
    query_parser.add_argument(
        "--tikv",
        action="store_true",
        help="Use select '<idx>' as table_idx,count(*) from <table_name> for each target table",
    )
    query_parser.add_argument(
        "--query-loop-count",
        type=int,
        default=1,
        help="How many times to execute the query, default: 1",
    )

    check_parser = subparsers.add_parser("check", help="Compare query results with and without TiKV mode")
    add_common_connection_args(check_parser)
    check_parser.add_argument("--table", default=DEFAULT_TABLE, help=f"Base table name, default: {DEFAULT_TABLE}")
    check_parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Table count for check targets. >1 creates <table>_<num>",
    )
    check_parser.add_argument(
        "--sql",
        default=DEFAULT_QUERY_SQL,
        help="Query SQL text used for the non-TiKV side",
    )
    check_parser.add_argument(
        "--query-loop-count",
        type=int,
        default=1,
        help="How many times to execute the comparison, default: 1",
    )

    insert_parser = subparsers.add_parser("insert-data", help="Read CSV data and insert into a table")
    insert_data.add_insert_data_args(insert_parser, csv_file_nargs="?")

    auto_parser = subparsers.add_parser("auto", help="Run a template flow: create table, add index, insert data")
    add_common_connection_args(auto_parser)
    add_common_table_args(auto_parser)
    auto_parser.add_argument("--index-name", default=DEFAULT_INDEX, help=f"Index name, default: {DEFAULT_INDEX}")
    auto_parser.add_argument("--column", default="body", help="Indexed column, default: body")
    add_insert_runtime_args(auto_parser, default_row_limit=DEFAULT_AUTO_ROW_LIMIT)

    return parser


def build_client(args: argparse.Namespace) -> MySQLClient:
    return MySQLClient(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        mysql_bin=args.mysql_bin,
    )


def build_insert_namespace(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        csv_file=getattr(args, "csv_file", None),
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        table=args.table,
        count=getattr(args, "count", 1),
        batch_size=args.batch_size,
        row_limit=args.row_limit,
        freshness_batch=args.freshness_batch,
        print_interval=args.print_interval,
        encoding=args.encoding,
        delimiter=args.delimiter,
        has_header=args.has_header,
        freshness=args.freshness,
        dry_run=args.dry_run,
    )


def run_auto_inserts(args: argparse.Namespace, table_names: Sequence[str]) -> int:
    del table_names
    return insert_data.run_insert_data(build_insert_namespace(args))


def build_auto_create_sqls(args: argparse.Namespace, table_names: Sequence[str]) -> List[Tuple[str, str, str, Optional[str]]]:
    return [
        (
            "create-table",
            format_table_target(args.database, table_name),
            create_table_sql(args.database, table_name),
            None,
        )
        for table_name in table_names
    ]


def build_auto_add_index_sqls(args: argparse.Namespace, table_names: Sequence[str]) -> List[Tuple[str, str, str, Optional[str]]]:
    return [
        (
            "add-index",
            format_table_target(args.database, table_name),
            add_index_sql(args.database, table_name, args.index_name, args.column),
            None,
        )
        for table_name in table_names
    ]


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

    try:
        if args.command == "insert-data":
            return insert_data.run_insert_data(args)

        if args.command == "query":
            return run_query(args)

        if args.command == "check":
            return run_check(args)

        client = build_client(args)
        table_names = build_table_names(args.table, getattr(args, "count", 1))
        print_command_summary(args.command, args.database, table_names, dry_run=args.dry_run)

        if args.command == "create-table":
            sqls = [
                (
                    "create-table",
                    format_table_target(args.database, table_name),
                    create_table_sql(args.database, table_name),
                    None,
                )
                for table_name in table_names
            ]
        elif args.command == "drop-table":
            sqls = [
                (
                    "drop-table",
                    format_table_target(args.database, table_name),
                    drop_table_sql(args.database, table_name),
                    None,
                )
                for table_name in table_names
            ]
        elif args.command == "add-index":
            sqls = [
                (
                    "add-index",
                    format_table_target(args.database, table_name),
                    add_index_sql(args.database, table_name, args.index_name, args.column),
                    None,
                )
                for table_name in table_names
            ]
        elif args.command == "drop-index":
            sqls = [
                (
                    "drop-index",
                    format_table_target(args.database, table_name),
                    drop_index_sql(args.database, table_name, args.index_name),
                    None,
                )
                for table_name in table_names
            ]
        elif args.command == "import-into":
            sqls = [
                (
                    "import-into",
                    format_table_target(args.database, args.table),
                    import_into_sql(
                        args.database,
                        args.table,
                        args.source,
                        parse_columns(args.columns),
                        args.cloud_storage_uri,
                        args.thread,
                        not args.no_detached,
                        not args.no_disable_precheck,
                        parse_with_options(args.with_option),
                    ),
                    None,
                )
            ]
        elif args.command == "auto":
            create_sqls = build_auto_create_sqls(args, table_names)
            add_index_sqls = build_auto_add_index_sqls(args, table_names)
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2

        parallel_sql_commands = {"create-table", "drop-table", "add-index", "drop-index"}
        if args.command == "auto":
            run_sqls(client, create_sqls, args.dry_run, parallel=True)
            run_sqls(client, add_index_sqls, args.dry_run, parallel=True)
            return run_auto_inserts(args, table_names)
            
        run_sqls(client, sqls, args.dry_run, parallel=args.command in parallel_sql_commands)
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    except mysql.connector.Error as exc:
        print(f"mysql error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
