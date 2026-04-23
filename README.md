This is test tools for TiDB-TiCI.

## Rust 版本

项目已经在 [Cargo.toml](/DATA/disk1/yinshuangfei/tools/tici_test/Cargo.toml) 下补充了对应的 Rust 实现，按 Python 脚本一一映射到 `src/bin/`：

- [src/bin/main.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/bin/main.rs)
- [src/bin/insert_data.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/bin/insert_data.rs)
- [src/bin/prepare_data.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/bin/prepare_data.rs)
- [src/bin/tici.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/bin/tici.rs)
- [src/bin/analyze_freshness.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/bin/analyze_freshness.rs)

共享逻辑位于 [src/common.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/common.rs) 和 [src/insert_logic.rs](/DATA/disk1/yinshuangfei/tools/tici_test/src/insert_logic.rs)。

构建与检查：

```bash
make
cargo check
cargo build
```

直接执行 `make` 会调用项目根目录下的 [Makefile](/DATA/disk1/yinshuangfei/tools/tici_test/Makefile)，并通过 `cargo build --bins` 编译全部 Rust 二进制程序。
编译完成后，会将可执行文件复制到项目根目录下的 `bin/` 中。

运行方式示例：

```bash
cargo run --bin main -- create-table --dry-run
cargo run --bin insert_data -- --dry-run
cargo run --bin prepare_data -- --download --dry-run
cargo run --bin tici -- --dry-run
cargo run --bin analyze_freshness -- --log-dir log
```

Rust 版本以“翻译当前 Python 行为”为目标，不主动修正现有 Python 逻辑差异。比如 `main.py auto` 当前只执行建表和加索引阶段，插入阶段被提前 `return` 跳过；Rust 版本的 `main` 也保持相同行为。
Rust 版本中的相对路径统一按项目根目录解析，因此即使从 `rust/` 之类的子目录执行，默认的 `data/` 和 `log/` 仍然会落到当前项目根目录下。

## analyze_freshness.py

`analyze_freshness.py` scans `log/freshness_result_*.log` and generates grouped freshness statistics by file in Markdown table format, plus a merged `Summarize` section across all files.
It parses lines in the `freshness reached` format, ignores malformed lines, and treats lines as invalid when `total_rows != baseline_row_count + imported_rows`.
Grouping is based on the ten-thousand bucket floor of `baseline_row_count`, so values within the same bucket share one group such as `group-10000` or `group-20000`.

Examples:

```bash
python analyze_freshness.py
python analyze_freshness.py --log-dir log
python analyze_freshness.py --show-invalid
python analyze_freshness.py --log-dir log --pattern 'freshness_result_20260420_*.log'
```

Running `python analyze_freshness.py` without any arguments executes the analysis directly and uses the default `log/` directory.
Use `--log-dir` only when logs are stored in a different directory.

Output format:

```text
## freshness_result_20260420_080047.log
| 起始行数量级 | 平均插入行数 | 统计条目数 | 平均耗时(s) |
| --- | ---: | ---: | ---: |
| group-0 | 10000.00 | 4616 | 31.53 |

## Summarize
| 起始行数量级 | 平均插入行数 | 统计条目数 | 平均耗时(s) |
| --- | ---: | ---: | ---: |
| group-0 | 10000.00 | 4616 | 31.53 |
```

## main.py

`main.py` provides a small CLI for common table operations. The Rust counterpart is `src/bin/main.rs`:

- create tables
- drop tables
- add fulltext indexes
- drop indexes
- run `IMPORT INTO`
- run query SQL
- compare query results with and without `--tikv`
- insert CSV data
- run an `auto` template flow

Default MySQL connection:

```bash
mysql --comments --host 10.2.12.79 --port 9528 -u root
```

Examples:

```bash
python main.py create-table
python main.py create-table --count 4
python main.py drop-table --table hdfs_log --count 4
python main.py add-index
python main.py drop-index
python main.py query
python main.py query --query-loop-count 10
python main.py query --count 4
python main.py query --count 4 --tikv
python main.py query --sql "select count(*) from test.hdfs_log;"
python main.py query --count 4 --sql "select count(*) from {table};"
python main.py check
python main.py check --query-loop-count 10
python main.py check --count 4
python main.py check --count 4 --sql "select '1' as table_idx, count(*) from {table};"
python main.py import-into
python main.py insert-data --dry-run --row-limit 100
python main.py insert-data data/hdfs-logs-multitenants.csv --dry-run --row-limit 100
python main.py auto --dry-run
python main.py auto --no-freshness
python main.py auto --row-limit 100000
python main.py auto --count 4 --dry-run
```

Use `--dry-run` to print SQL without executing it.
`auto` is a template command. The design intent is to create table(s), add one fulltext index per table, and then insert data from CSV into each table. The default table count is `1`. When `--count > 1` or `--table-offset > 0`, table names follow the `<table>_<num>` pattern, and numbering starts from `--table-offset + 1`. The default insert row limit is `100000` for each table.
`main.py` prints command summaries and target table labels before each stage so multi-table runs are easier to follow.
For `create-table`, `drop-table`, `add-index`, and `drop-index`, when the target table count is greater than `1`, execution runs in parallel. `--dry-run` still prints SQL sequentially.
In `auto`, the create-table and add-index stages also reuse the same parallel SQL execution logic, but they run in separate phases: all create-table work finishes first, then add-index starts. `--dry-run` still keeps the SQL output sequential.
In the Rust version, `add-index` treats repeated `ft_idx` creation as ignorable when the index already exists or the same index is already being added by a background DDL job. Those targets are reported as `skip` instead of failing the whole batch.
In the current checked-in Python implementation, `auto` returns after the create-table and add-index stages, so the insert stage is not reached. The Rust version keeps this same runtime behavior instead of silently changing it.
`query` defaults to `select count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);`.
`query --query-loop-count` controls how many times the same query is executed. The default is `1`.
`query --count` controls how many tables are queried with the `<table>_<num>` naming rule. `--table-offset` controls the starting suffix offset, so numbering starts from `--table-offset + 1`. For custom SQL, `{table}` can be used as a placeholder for the current `database.table`.
`query --tikv` uses `select '<idx>' as table_idx,count(*) from <table_name>;` for each target table.
In the Rust version, `create-table`, `drop-table`, `add-index`, `drop-index`, `import-into`, `query`, `check`, and `auto` all use a shared MySQL connection pool. Pool size is controlled by `--conn-pool-size`, default `1000`.
When `query --query-loop-count > 1`, the query executions run in parallel. `--dry-run` still prints SQL sequentially to keep output readable.
`check` reuses the same table selection and non-`--tikv` SQL generation logic as `query`, then runs the built-in `--tikv` count SQL for the same target table and compares the returned results.
`check --query-loop-count` controls how many times each comparison is executed. The default is `1`.
`check --dry-run` prints both SQL statements for each target and round without executing them.

## insert_data.py

`insert_data.py` reads a CSV file and inserts rows into `test.hdfs_log`. The Rust counterpart is `src/bin/insert_data.rs`.
It reads and inserts data batch by batch through the Python `mysql.connector` library. The default batch size is `1000`, the default row limit is `100000`, and progress is printed every `3` seconds by default. When a batch insert fails, it retries up to `10` times with a `1` second interval.
If `csv_file` is omitted, it uses `data/hdfs-logs-multitenants.csv`.
When `--count > 1` or `--table-offset > 0`, target tables follow the `<table>_<num>` naming rule, and numbering starts from `--table-offset + 1`. Multi-table execution runs in parallel unless `--dry-run` is used.
The Rust version adds `--conn-pool-size`, default `1000`. A single shared MySQL connection pool is created for the whole import run, and each batch insert / freshness query acquires one connection from that pool immediately before execution and returns it right after the current SQL finishes.
In the Rust version, multi-table import scheduling is driven by Tokio tasks, while the per-table blocking insert work runs inside `spawn_blocking`.
The Rust version serializes file log writes for insert tasks, so concurrent tasks appending to the same log file will not corrupt each other.
Each `completed import` line is also appended to `log/insert_result.log` under the current working directory. Insert retry and fatal insert failure messages are also appended to `log/insert_error.log`.
Freshness is enabled by default. Unless `--no-freshness` is specified, the script records the target table row count before inserting, then polls `select count(*) from <table> where fts_match_word('china',body) or not fts_match_word('china',body);` every `5` seconds after the import until the visible newly inserted row count matches the imported row count, or until a fixed `30` minute timeout is reached. The freshness logs are written to timestamp-suffixed files such as `log/freshness_progress_YYYYMMDD_HHMMSS.log` and `log/freshness_result_YYYYMMDD_HHMMSS.log` under the project root.
If `--freshness-batch` is omitted, it defaults to the effective `--row-limit` value, so one import command normally corresponds to one freshness round unless you explicitly split it into smaller windows.
When `--no-freshness` is specified, the script also skips the `--freshness-batch <= --row-limit` validation.
The current Rust version supports `utf-8` CSV input and rejects other `--encoding` values.
In the Rust version, relative `csv_file` and log paths are resolved from the project root instead of the current shell directory.

Examples:

```bash
python insert_data.py --dry-run
python insert_data.py --count 4 --dry-run
python insert_data.py --count 4 --table-offset 2 --dry-run
python insert_data.py data/hdfs-logs-multitenants.csv --dry-run
python insert_data.py data/hdfs-logs-multitenants.csv --has-header
python insert_data.py data/hdfs-logs-multitenants.csv --batch-size 1000 --row-limit 5000 --print-interval 3
python insert_data.py data/hdfs-logs-multitenants.csv --no-freshness
```

## tici.py

`tici.py` queries TiCI metadata from `tici.tici_shard_meta`. The Rust counterpart is `src/bin/tici.rs`.
By default it runs:

```sql
select table_id,index_id,shard_id,progress from `tici`.`tici_shard_meta`;
```

It accepts the same MySQL connection style as the other scripts: `--host`, `--port`, `--user`, `--password`, `--database`, and `--mysql-bin`.
The metadata database is fixed to `tici`. `--meta-table` can be used to override the metadata table name. `--dry-run` prints the SQL without executing it.

Examples:

```bash
python tici.py
python tici.py --dry-run
python tici.py --host 127.0.0.1 --port 4000 --user root
python tici.py --meta-table tici_shard_meta --dry-run
```

## prepare_data.py

`prepare_data.py` downloads source files for later import steps. The Rust counterpart is `src/bin/prepare_data.rs`.
It can also convert downloaded HDFS json/json.gz logs into CSV.
Download progress is refreshed on the same line every `3` seconds by default.
In the Rust version, relative `--output-dir`, `--input-file`, and `--convert-out` paths are resolved from the project root.

Examples:

```bash
python prepare_data.py --download --dry-run
python prepare_data.py --download
python prepare_data.py --download 'https://example.com/data.csv' --output-file hdfs-logs.csv --overwrite
python prepare_data.py --download --convert-out data/hdfs-logs-multitenants.csv --max-rows 1000
python prepare_data.py --input-file data/hdfs-logs-multitenants.json.gz --convert-out data/hdfs-logs-multitenants.csv
```
