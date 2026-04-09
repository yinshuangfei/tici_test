#!/usr/bin/env python3
"""Query TiCI metadata tables."""

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Optional, Sequence


DEFAULT_HOST = "10.2.12.81"
DEFAULT_PORT = 9529
DEFAULT_USER = "root"
DEFAULT_DATABASE = "tici"
DEFAULT_META_TABLE = "tici_shard_meta"


def quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("identifier must not be empty")
    return f"`{identifier.replace('`', '``')}`"


@dataclass
class MySQLClient:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    user: str = DEFAULT_USER
    database: str = DEFAULT_DATABASE
    password: str = ""
    mysql_bin: str = "mysql"
    comments: bool = True

    def command(self) -> List[str]:
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
        cmd.extend(["-D", self.database])
        return cmd

    def execute(self, sql: str, *, echo: bool = False) -> None:
        cmd = self.command()
        if echo:
            print(f"$ {shlex.join(cmd)}")
            print(sql)
        subprocess.run(cmd, input=sql, text=True, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query TiCI shard metadata")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"MySQL host, default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MySQL port, default: {DEFAULT_PORT}")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"MySQL user, default: {DEFAULT_USER}")
    parser.add_argument("--password", default="", help="MySQL password, optional")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help=f"Database name for mysql session, default: {DEFAULT_DATABASE}")
    parser.add_argument("--mysql-bin", default="mysql", help="Path to mysql client binary")
    parser.add_argument(
        "--meta-table",
        default=DEFAULT_META_TABLE,
        help=f"Metadata table name, default: {DEFAULT_META_TABLE}",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only, do not execute")
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


def build_query_sql(meta_table: str) -> str:
    target = f"{quote_identifier('tici')}.{quote_identifier(meta_table)}"
    return f"select table_id,index_id,shard_id,progress from {target};"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        sql = build_query_sql(args.meta_table)
        if args.dry_run:
            print(sql)
            return 0
        build_client(args).execute(sql)
        return 0
    except subprocess.CalledProcessError as exc:
        print(f"command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
