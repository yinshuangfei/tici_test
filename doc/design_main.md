# 功能设计
## 文件
- main.py

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
- 命令行默认无参数时输出 help 信息
- `insert-data` 子命令复用 `insert_data.py` 的参数和执行逻辑
- `auto` 子命令用于执行固定流程模版
- `main.py` 的输出按命令摘要、阶段动作和目标表分层展示，方便查看多表执行进度

### 数据库基础功能
- 打开和关闭数据库连接
- 默认连接参数为 "mysql --comments --host 10.2.12.81 --port 9529 -u root"

### 创建表
- 支持输入数据库表的名称，默认名称为 hdfs_log
- 支持输入数据库表的数量，默认数量为 1
- 当数量大于 1 时，数据库的表名命名规则为 hdfs_log_<num>
- 当表数量大于 1 且不是 `--dry-run` 时，`create-table` 按目标表使用多线程并行执行
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
- 当目标表数量大于 1 且不是 `--dry-run` 时，`add-index` 和 `drop-index` 按目标表使用多线程并行执行
- 添加索引的内容如下：
```
ALTER TABLE test.hdfs_log ADD FULLTEXT INDEX ft_idx(body);
```

### 删除表
- 当目标表数量大于 1 且不是 `--dry-run` 时，`drop-table` 按目标表使用多线程并行执行

### Import Into
- 默认操作如下：
```
IMPORT INTO test.hdfs_log (`timestamp`, severity_text, body, tenant_id)
FROM 's3://data/import-src/hdfs-logs-multitenants.csv?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true'
WITH 
    CLOUD_STORAGE_URI='s3://ticidefaultbucket/tici/import-sort?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true',
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
- 当 `--count > 1` 时，表名命名规则为 `hdfs_log_<num>`，例如 `hdfs_log_1`、`hdfs_log_2`
- 默认查询语句会根据目标表自动替换表名
- 支持通过 `--sql` 传入自定义查询语句
- 当自定义 SQL 中包含 `{table}` 时，执行时会替换为当前目标表的 `database.table`
- 支持通过 `--query-loop-count` 指定同一条查询重复执行的次数，默认 `1`
- 当 `--query-loop-count > 1` 且不是 `--dry-run` 时，查询任务按目标表和轮次使用多线程并行执行
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
- 当 `--count > 1` 时，表名命名规则为 `hdfs_log_<num>`，例如 `hdfs_log_1`、`hdfs_log_2`
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
- 当 `--count > 1` 时，表名命名规则为 `hdfs_log_<num>`，例如 `hdfs_log_1`、`hdfs_log_2`
- `auto` 默认索引名为 `ft_idx`
- `auto` 默认索引列为 `body`
- `auto` 默认导入 `100000` 行数据
- `auto` 默认 csv 文件路径为 `data/hdfs-logs-multitenants.csv`
- `auto` 的数据导入阶段复用 `insert_data.py` 的批量插入逻辑
- `auto` 的数据导入阶段通过 Python `mysql.connector` 库执行批量插入
- `auto` 导入阶段的 `completed import` 日志会追加写入当前目录下的 `log/insert_result.log`
- `auto` 导入阶段的插入重试日志和最终失败日志会追加写入当前目录下的 `log/insert_error.log`
- `auto` 默认开启 freshness 检查；支持通过 `--no-freshness` 参数显式关闭，并透传给导入阶段
- 当 `auto` 未指定 `--no-freshness` 时，导入阶段会在插入完成后每隔 5 秒执行
```
SELECT COUNT(*) FROM <table> WHERE fts_match_word('china',body) OR NOT fts_match_word('china',body);
```
- 当查询结果减去导入前基线值等于本次导入行数时，视为数据可见；否则持续轮询直到达到 30 分钟超时
- 默认开启的 freshness 日志会写入当前目录下带时间后缀的文件，例如 `log/freshness_progress_YYYYMMDD_HHMMSS.log` 和 `log/freshness_result_YYYYMMDD_HHMMSS.log`
- 当 `--count > 1` 时，`auto` 会对每一张目标表依次执行建表、加索引、导入数据的相同流程
- `auto` 中的建表和加索引阶段复用 `run_sqls` 的多线程逻辑
- `auto` 中的建表和加索引按阶段执行：先完成所有表的建表，再开始所有表的加索引
- 当 `--count > 1` 且不是 `--dry-run` 时，`auto` 中的建表和加索引阶段会按目标表并行执行
- `auto` 在真实执行时，导入阶段会按目标表使用多线程并行执行
- `auto` 在 `--dry-run` 模式下，导入阶段保持串行输出，避免多线程打乱 SQL 文本显示
- `auto` 中的 `--row-limit` 表示每张表的导入行数上限
- `auto` 支持通过 `--dry-run` 输出整套模板流程而不真正执行
