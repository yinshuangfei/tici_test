# 功能设计
## 文件
- insert_data.py
- src/bin/insert_data.rs
- src/insert_logic.rs

## 要求
- 修改代码的同时，同步更新 README.md 和对应的设计文档

## 功能描述
- 读取指定 csv 文件的内容
- 将文件内容插入到执行的数据库中
- 数据库默认连接参数为 "--host 10.2.12.79 --port 9528 -u root"
- 数据库表名为 test.hdfs_log
- 数据库 shema 为
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

## 实现细节
- 脚本文件为 `insert_data.py`
- Rust 对应实现位于 `src/bin/insert_data.rs`
- `insert_data.py` 持有实际的参数解析、CSV 读取、SQL 生成和执行逻辑
- `main.py` 通过 `insert-data` 子命令复用 `insert_data.py` 的实现，作为统一入口
- Rust 版本将共享导入逻辑下沉到 `src/insert_logic.rs`，并由 `src/bin/main.rs` 与 `src/bin/insert_data.rs` 共同复用
- 命令行默认无参数时输出 help 信息
- 通过位置参数 `csv_file` 指定要导入的 csv 文件路径
- 当未传入 `csv_file` 时，默认使用 `data/hdfs-logs-multitenants.csv`
- 支持的可选参数如下：
  - `--host`，默认 `10.2.12.79`
  - `--port`，默认 `9528`
  - `--user`，默认 `root`
  - `--password`，默认空
  - `--database`，默认 `test`
  - `--table`，默认 `hdfs_log`
  - `--count`，默认 `1`
  - `--table-offset`，默认 `0`，控制生成表名后缀的起始偏移
  - `--batch-size`，默认 `1000`
  - `--row-limit`，默认 `100000`
  - `--freshness-batch`，默认等于 `--row-limit` 的生效值
  - `--print-interval`，默认 `3`，表示每隔多少秒输出一次进度
  - `--conn-pool-size`，默认 `1000`，表示 Rust 版本共享连接池的最大连接数
  - `--encoding`，默认 `utf-8`
  - `--delimiter`，默认 `,`
  - `--has-header`，指定后跳过首行表头
  - `--no-freshness`，显式关闭默认开启的 freshness 检查
  - `--dry-run`，只输出 SQL，不真正执行
- CSV 默认按以下列顺序读取：
```
timestamp,severity_text,body,tenant_id
```
- 当 `--has-header` 未指定时，每一行都按固定 4 列解析
- 当 `--has-header` 指定时，跳过第一行，从第二行开始按固定 4 列解析
- 当 `--row-limit > 0` 时，只读取前 N 条有效数据行并导入
- 当 `--count > 1` 或 `--table-offset > 0` 时，表名命名规则为 `hdfs_log_<num>`
- 后缀编号从 `--table-offset + 1` 开始，例如 `--count 2 --table-offset 3` 会生成 `hdfs_log_4`、`hdfs_log_5`
- 程序按单批次插入方式执行：
  - 每次从 CSV 中读取 `--batch-size` 条数据，默认 1000 条
  - 为这一批数据构建一条 `INSERT INTO ... VALUES (...)` SQL 供 `--dry-run` 输出
  - 执行完成后继续从上次读取偏移处处理下一批
  - 直到达到 `--row-limit` 指定的上限，或者文件读取结束
- 当目标表数量大于 1 时，会对每一张目标表执行相同的导入流程
- 当目标表数量大于 1 且不是 `--dry-run` 时，多表导入按目标表使用多线程并行执行
- 当 `--dry-run` 时，多表导入保持串行输出，避免多线程打乱 SQL 文本显示
- Rust 版本中的多表导入调度使用 Tokio 协程；每个表级导入任务通过 `spawn_blocking` 执行同步的数据库写入逻辑
- Rust 版本在真实执行模式下使用一个共享 MySQL 连接池
- Rust 版本中的所有导入任务共享同一个 MySQL 连接池
- 导入前的 baseline 计数、每一轮 batch 插入、以及每一轮 freshness 查询，都会在当前 SQL 执行前临时从池中获取连接
- 当前 SQL 执行结束后立即归还连接，而不是在整个表级导入任务生命周期中持续占有连接
- Rust 版本中的文件日志写入使用全局互斥锁串行处理，避免多个并发任务同时追加同一个日志文件时产生写入交叉
- `--conn-pool-size` 控制 Rust 版本共享连接池的最大连接数，默认 `1000`
- 对每一行做基础校验：
  - 列数必须为 4
  - `timestamp` 必须可转换为整数
  - `tenant_id` 必须可转换为整数
- 插入 SQL 使用批量写入方式：
```
INSERT INTO test.hdfs_log (`timestamp`, `severity_text`, `body`, `tenant_id`) VALUES
(...),
(...);
```
- 程序不会先拼接整份 CSV 的总 SQL，而是逐批构建并逐批执行
- `--row-limit` 控制总导入行数上限，`--batch-size` 只控制单条 `INSERT INTO` 语句包含的行数
- 当未显式指定 `--freshness-batch` 时，默认取当前 `--row-limit` 的生效值，因此默认只会执行一轮导入和一轮 freshness 检查；显式指定更小的 `--freshness-batch` 时，才会将一次导入拆成多轮顺序执行
- 非 `--dry-run` 模式下，数据库写入通过 Python `mysql.connector` 库按批次执行
- 每批数据使用参数化批量插入，避免手工拼接值再交给外部 `mysql` 客户端执行
- 当单批次插入失败时，程序会对该批次最多重试 10 次，每次重试前等待 1 秒，并重新建立数据库连接
- 插入重试日志和最终失败日志除了输出到标准错误外，还会追加写入项目根目录下的 `log/insert_error.log`
- 导入结束后的 `completed import` 日志除了输出到标准输出外，还会追加写入项目根目录下的 `log/insert_result.log`
- 默认开启 freshness 检查；当未指定 `--no-freshness` 时，程序会在导入前先查询一次当前目标表总行数，记为基线值
- 导入结束后，如果未指定 `--no-freshness`，则循环执行：
```
SELECT COUNT(*) FROM <table> WHERE fts_match_word('china',body) OR NOT fts_match_word('china',body);
```
  - 当当前总行数减去基线值等于本次成功导入行数时，视为 freshness 达成
  - 轮询超时时间固定为 30 分钟
  - 轮询间隔固定为 5 秒
  - 每次 freshness 查询的开始、轮询结果和最终状态都会写入项目根目录下带时间后缀的 freshness 日志文件，例如 `log/freshness_progress_YYYYMMDD_HHMMSS.log` 和 `log/freshness_result_YYYYMMDD_HHMMSS.log`
  - 超时后脚本返回非 0
- 当指定 `--no-freshness` 时，不执行 freshness 流程，也不检查 `--freshness-batch <= --row-limit` 这条约束
- 进度输出按时间间隔控制，不再按每个批次输出
- 当达到 `--print-interval` 指定的秒数间隔时，输出当前表名、累计导入行数和已耗时
- 导入结束后，输出当前表名、最终总导入行数和总耗时
- 当 CSV 文件不存在、解码失败、列数不正确、整数转换失败时，脚本直接报错退出
- 当 CSV 中没有有效数据行时，输出带表名的 `no data rows found`
- Rust 版本当前只支持 `utf-8` 编码输入；若传入其他 `--encoding`，会直接报错退出
- Rust 版本中的相对 `csv_file` 路径和日志输出路径按项目根目录解析，因此从子目录执行命令时也会稳定读写项目根下的 `data/` 和 `log/`
