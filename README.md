This is test tools for TiDB-TiCI.

## main.py

`main.py` provides a small CLI for common table operations:

- create tables
- drop tables
- add fulltext indexes
- drop indexes
- run `IMPORT INTO`
- run query SQL
- insert CSV data
- run an `auto` template flow

Default MySQL connection:

```bash
mysql --comments --host 10.2.12.81 --port 9529 -u root
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
python main.py import-into
python main.py insert-data --dry-run --row-limit 100
python main.py insert-data data/hdfs-logs-multitenants.csv --dry-run --row-limit 100
python main.py auto --dry-run
python main.py auto --row-limit 100000
python main.py auto --count 4 --dry-run
```

Use `--dry-run` to print SQL without executing it.
`auto` is a template command. It creates table(s), adds one fulltext index per table, and then inserts data from CSV into each table. The default table count is `1`. When `--count > 1`, table names follow the `<table>_<num>` pattern. The default insert row limit is `100000` for each table.
`main.py` prints command summaries and target table labels before each stage so multi-table runs are easier to follow.
For `create-table`, `drop-table`, `add-index`, and `drop-index`, when the target table count is greater than `1`, execution runs in parallel. `--dry-run` still prints SQL sequentially.
In `auto`, the create-table and add-index stages also reuse the same parallel SQL execution logic, but they run in separate phases: all create-table work finishes first, then add-index starts. `--dry-run` still keeps the SQL output sequential.
In `auto`, the insert stage runs in parallel by table when actually executing. `--dry-run` keeps insert output sequential so SQL text stays readable.
`query` defaults to `select count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);`.
`query --query-loop-count` controls how many times the same query is executed. The default is `1`.
`query --count` controls how many tables are queried with the `<table>_<num>` naming rule. For custom SQL, `{table}` can be used as a placeholder for the current `database.table`.
`query --tikv` uses `select '<idx>' as table_idx,count(*) from <table_name>;` for each target table.
When `query --query-loop-count > 1`, the query executions run in parallel. `--dry-run` still prints SQL sequentially to keep output readable.

## insert_data.py

`insert_data.py` reads a CSV file and inserts rows into `test.hdfs_log`.
It reads and inserts data batch by batch. The default batch size is `1000`, the default row limit is `100000`, and progress is printed every `3` seconds by default.
If `csv_file` is omitted, it uses `data/hdfs-logs-multitenants.csv`.
When `--count > 1`, target tables follow the `<table>_<num>` naming rule. Multi-table execution runs in parallel unless `--dry-run` is used.

Examples:

```bash
python insert_data.py --dry-run
python insert_data.py --count 4 --dry-run
python insert_data.py data/hdfs-logs-multitenants.csv --dry-run
python insert_data.py data/hdfs-logs-multitenants.csv --has-header
python insert_data.py data/hdfs-logs-multitenants.csv --batch-size 1000 --row-limit 5000 --progress-interval 3
```

## tici.py

`tici.py` queries TiCI metadata from `tici.tici_shard_meta`.
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

`prepare_data.py` downloads source files for later import steps.
It can also convert downloaded HDFS json/json.gz logs into CSV.
Download progress is refreshed on the same line every `3` seconds by default.

Examples:

```bash
python prepare_data.py --download --dry-run
python prepare_data.py --download
python prepare_data.py --download 'https://example.com/data.csv' --output-file hdfs-logs.csv --overwrite
python prepare_data.py --download --convert-out data/hdfs-logs-multitenants.csv --max-rows 1000
python prepare_data.py --input-file data/hdfs-logs-multitenants.json.gz --convert-out data/hdfs-logs-multitenants.csv
```
