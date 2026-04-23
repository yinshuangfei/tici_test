# 功能设计
## 文件
- main.py
- src/bin/main.rs
- src/common.rs
- src/insert_table.rs
- src/sql_pool.rs

## 要求
- 修改代码的同时，同步更新 README.md 和对应的设计文档

## 功能要求
### 命令行参数功能
- 创建表 (默认数据库选择 test)
- 删除表
- 添加索引
- 删除索引
- 执行查询语句
- 使用 import into 导入数据
- 使用 `insert-data` 作为统一入口复用 `insert_data.py`
- 提供 `auto` 模版功能

## 入口说明
- 主脚本文件改为 `main.py`
- Rust 对应实现位于 `src/bin/main.rs`；其中建表 SQL 生成逻辑提取在 `src/create_table.rs`，删表 SQL 生成逻辑提取在 `src/drop_table.rs`，查询与校验逻辑提取在 `src/query_table.rs`
- 命令行默认无参数时输出 help 信息
- `insert-data` 子命令复用 `insert_data.py` 的参数和执行逻辑
- `auto` 子命令用于执行固定流程模版
- `main.py` 的输出按命令摘要、阶段动作和目标表分层展示，方便查看多表执行进度
- Rust 版本复用 `src/common.rs` 中的通用路径与 SQL 文本辅助逻辑、`src/sql_pool.rs` 中的数据库连接池与查询执行逻辑，以及 `src/insert_table.rs` 中的导入逻辑
- Rust 版本中的相对文件路径按项目根目录解析，不依赖命令执行时所在的 shell 目录
- Rust 版本中的 `insert-data` 相关入口支持 `--conn-pool-size` 参数，默认 `1000`
- Rust 版本中的 `create-table`、`drop-table`、`add-index`、`drop-index`、`import-into`、`query`、`check` 和 `auto` 也使用共享 `mysql_async` 连接池执行 SQL，连接池大小同样由 `--conn-pool-size` 控制；每条 SQL 在执行前从池中取连接，执行结束后归还

### 数据库基础功能
- 打开和关闭数据库连接
- 默认连接参数为 "mysql --comments --host 10.2.12.79 --port 9528 -u root"

### 创建表
- 支持输入数据库表的名称，默认名称为 hdfs_log
- 支持输入数据库表的数量，默认数量为 1
- 支持通过 `--table-offset` 控制生成表名后缀的起始偏移，默认 `0`
- 当 `--count > 1` 或 `--table-offset > 0` 时，数据库的表名命名规则为 `hdfs_log_<num>`
- 后缀编号从 `--table-offset + 1` 开始
- 当表数量大于 1 且不是 `--dry-run` 时，`create-table` 按目标表使用 Tokio async task 并行执行
- 表的内容如下：
```
CREATE TABLE IF NOT EXISTS test.hdfs_log (
    id BIGINT AUTO_INCREMENT,
    timestamp BIGINT,
    severity_text VARCHAR(50),
    body TEXT,
    tenant_id INT,
    PRIMARY KEY (tenant_id, id)
);
```

### 添加索引
- 当目标表数量大于 1 且不是 `--dry-run` 时，`add-index` 和 `drop-index` 按目标表使用 Tokio async task 并行执行
- 添加索引的内容如下：
```
ALTER TABLE test.hdfs_log ADD FULLTEXT INDEX ft_idx(body);
```
- Rust 版本中的 `add-index` 在遇到 `Duplicate key name`、`index already exist` 或同名后台 DDL job 已存在时，会将该目标表视为可跳过并继续执行其余目标，而不是直接让整批任务失败

### 删除表
- 当目标表数量大于 1 且不是 `--dry-run` 时，`drop-table` 按目标表使用 Tokio async task 并行执行

### Import Into
- 默认操作如下：
```
IMPORT INTO test.hdfs_log (`timestamp`, severity_text, body, tenant_id)
FROM 's3://data/import-src/hdfs-logs-multitenants.csv?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.79:19008&force-path-style=true'
WITH 
    CLOUD_STORAGE_URI='s3://ticidefaultbucket/tici/import-sort?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.79:19008&force-path-style=true',
    DETACHED,
    DISABLE_PRECHECK,
    THREAD=8;
```

### Query
- 提供 `query` 子命令用于读取表中的数据
- 默认查询语句为：
```
select count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);
```
- `query` 支持 `--table` 指定基础表名，默认 `hdfs_log`
- `query` 支持 `--count` 指定目标表数量，默认 `1`
- `query` 支持 `--table-offset` 指定表名后缀起始偏移，默认 `0`
- 当 `--count > 1` 或 `--table-offset > 0` 时，表名命名规则为 `hdfs_log_<num>`
- 后缀编号从 `--table-offset + 1` 开始，例如 `--count 2 --table-offset 3` 会生成 `hdfs_log_4`、`hdfs_log_5`
- 默认查询语句会根据目标表自动替换表名
- 支持通过 `--sql` 传入自定义查询语句
- 当自定义 SQL 中包含 `{table}` 时，执行时会替换为当前目标表的 `database.table`
- 支持通过 `--query-loop-count` 指定同一条查询重复执行的次数，默认 `1`
- 当不是 `--dry-run` 且待执行查询语句数量大于 1 时，查询任务按目标表和轮次使用 Tokio async task 并行执行
- Rust 版本中的 `query` 会在后台并发执行这些查询，但会按照原始语句顺序输出结果，避免 stdout 顺序被并发打乱
- 当 `--dry-run` 时，查询阶段保持串行输出，避免多线程打乱 SQL 文本显示
- `query` 支持通过 `--dry-run` 只输出 SQL 而不真正执行

### Check
- 提供 `check` 子命令用于校验同一张表在普通查询和 `--tikv` 查询下返回结果是否一致
- `check` 默认复用 `query` 的默认查询语句：
```
select count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);
```
- `check` 支持 `--table` 指定基础表名，默认 `hdfs_log`
- `check` 支持 `--count` 指定目标表数量，默认 `1`
- `check` 支持 `--table-offset` 指定表名后缀起始偏移，默认 `0`
- 当 `--count > 1` 或 `--table-offset > 0` 时，表名命名规则为 `hdfs_log_<num>`
- 后缀编号从 `--table-offset + 1` 开始，例如 `--count 2 --table-offset 3` 会生成 `hdfs_log_4`、`hdfs_log_5`
- `check` 支持通过 `--sql` 指定普通查询侧使用的 SQL
- 当自定义 SQL 中包含 `{table}` 时，执行时会替换为当前目标表的 `database.table`
- `check` 的 TiKV 对照侧固定使用 `select '<idx>' as table_idx,count(*) from <table_name>;`
- `check` 支持通过 `--query-loop-count` 指定同一组对比重复执行的次数，默认 `1`
- `check` 在真实执行时按目标表和轮次串行执行，每轮先执行普通查询，再执行 TiKV 对照查询
- 如果某一轮返回结果不一致，`check` 会立即输出两边结果并返回非 0
- 当 `--dry-run` 时，`check` 会打印每一轮的普通查询 SQL 和 TiKV 对照 SQL，而不真正执行

### Auto 模版
- `auto` 子命令用于提供一套固定流程模版
- 默认流程如下：
  - 创建 1 张表
  - 为该表添加 1 个全文索引
  - 从 csv 中导入数据
- `auto` 默认表名为 `hdfs_log`
- `auto` 默认表数量为 `1`
- `auto` 支持 `--table-offset`，默认 `0`，控制生成表名后缀的起始偏移
- 当 `--count > 1` 或 `--table-offset > 0` 时，表名命名规则为 `hdfs_log_<num>`
- 后缀编号从 `--table-offset + 1` 开始，例如 `--count 2 --table-offset 3` 会生成 `hdfs_log_4`、`hdfs_log_5`
- `auto` 默认索引名为 `ft_idx`
- `auto` 默认索引列为 `body`
- `auto` 默认导入 `100000` 行数据
- `auto` 默认 csv 文件路径为 `data/hdfs-logs-multitenants.csv`
- `auto` 的数据导入阶段复用 `insert_data.py` 的批量插入逻辑
- `auto` 的数据导入阶段通过 `mysql_async` 执行批量异步插入
- `auto` 导入阶段的 `completed import` 日志会追加写入项目根目录下的 `log/insert_result.log`
- `auto` 导入阶段的插入重试日志和最终失败日志会追加写入项目根目录下的 `log/insert_error.log`
- `auto` 默认开启 freshness 检查；支持通过 `--no-freshness` 参数显式关闭，并透传给导入阶段
- `auto` 中若未显式指定 `--freshness-batch`，则导入阶段默认取 `--row-limit` 的生效值
- Rust 版本中的导入阶段参数还包括 `--conn-pool-size`，用于控制共享连接池大小，默认 `1000`
- 当指定 `--no-freshness` 时，导入阶段也不检查 `--freshness-batch <= --row-limit` 这条约束
- 当 `auto` 未指定 `--no-freshness` 时，导入阶段会在插入完成后每隔 5 秒执行
```
SELECT COUNT(*) FROM <table> WHERE fts_match_word('china',body) OR NOT fts_match_word('china',body);
```
- 当查询结果减去导入前基线值等于本次导入行数时，视为数据可见；否则持续轮询直到达到 30 分钟超时
- 默认开启的 freshness 日志会写入项目根目录下带时间后缀的文件，例如 `log/freshness_progress_YYYYMMDD_HHMMSS.log` 和 `log/freshness_result_YYYYMMDD_HHMMSS.log`
- 当目标表数量大于 1 时，`auto` 会对每一张目标表依次执行建表、加索引、导入数据的相同流程
- `auto` 中的建表和加索引阶段复用 `run_sqls` 的 Tokio async 并发逻辑
- `auto` 中的建表和加索引按阶段执行：先完成所有表的建表，再开始所有表的加索引
- 当目标表数量大于 1 且不是 `--dry-run` 时，`auto` 中的建表和加索引阶段会按目标表并行执行
- `auto` 中的 `--row-limit` 表示每张表的导入行数上限
- `auto` 支持通过 `--dry-run` 输出整套模板流程而不真正执行
- 当前仓库中的 Python 实现和 Rust 实现都在 `auto` 的建表和加索引阶段完成后直接返回，因此插入阶段暂未真正执行；Rust 版本保持与当前 Python 代码一致的行为，不额外修正
