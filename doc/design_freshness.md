# 功能设计
## 文件
- freshness.py

## 要求
- 修改代码的同时，同步更新 README.md 和对应的设计文档

## 功能描述
- 提供独立的 freshness 查询模块
- 通过内部线程池并发执行多个表的 freshness 检查任务
- 线程池容量可配置，默认 `10`
- 当线程池已满时，新提交的任务直接报错并丢弃，不排队
- 每个任务根据给定的目标 row 数、数据库连接信息、表名、查询间隔和超时时间执行轮询查询
- 支持简单命令行入口，允许直接执行单个 freshness 任务并等待结果
- 查询 SQL 固定为：
```
SELECT COUNT(*) FROM <table> WHERE fts_match_word('china',body) OR NOT fts_match_word('china',body);
```
- 查询过程写入当前目录下带时间后缀的 `freshness_progress` 日志文件
- 查询最终结果写入当前目录下带时间后缀的 `freshness_result` 日志文件

## 实现细节
- 脚本文件为 `freshness.py`
- 主要常量包括：
  - 默认线程池大小 `10`
  - 默认轮询间隔 `5` 秒
  - 默认超时时间 `30` 分钟
  - 默认日志文件名模式为 `log/freshness_progress_YYYYMMDD_HHMMSS.log` 和 `log/freshness_result_YYYYMMDD_HHMMSS.log`
- `MySQLConnectionConfig` 封装数据库连接参数：
  - `host`，默认 `10.2.12.81`
  - `port`，默认 `9529`
  - `user`，默认 `root`
  - `password`，默认空
  - `database`，默认 `test`
- `FreshnessTask` 描述单个 freshness 任务：
  - `target_row_count`，目标总行数
  - `database`，目标数据库名
  - `table`，目标表名
  - `connection`，数据库连接配置
  - `poll_interval`，轮询间隔
  - `timeout`，超时时间
- `FreshnessCheckerPool` 在内部维护一个 `ThreadPoolExecutor`
- 命令行入口通过 `main()` 和 `run_once()` 组织
- 直接执行 `python freshness.py <target_row_count>` 时：
  - 解析数据库连接参数、表名、轮询间隔、超时时间和线程池大小
  - 创建一个线程池并提交单个 freshness 任务
  - 等待任务结束
  - freshness 达成时返回码为 `0`
  - freshness 超时时返回码为 `1`
  - 参数非法时返回码为 `2`
- `submit()` 提交任务时会先检查当前活跃任务数：
  - 当活跃任务数小于线程池容量时，立即提交执行
  - 当活跃任务数大于等于线程池容量时，直接记录 rejected 日志并抛出异常
- 线程池不保留等待队列；超过容量的任务不会延后执行
- 每个任务的执行流程如下：
  - 建立数据库连接
  - 在 `freshness_progress` 日志中记录 start 信息
  - 按轮询间隔重复执行 `COUNT(*)` 查询
  - 每次查询后在 `freshness_progress` 日志中记录当前耗时、目标行数和当前查询结果
  - 当当前行数大于等于目标行数时，在 `freshness_result` 日志中记录 `freshness reached`
  - 当耗时超过超时阈值时，在 `freshness_result` 日志中记录 `freshness timeout`
  - 当查询异常或连接异常发生时，在 `freshness_result` 日志中记录 `freshness failed` 并向上抛出异常
- 线程完成后会自动从活跃任务集合中移除
- `shutdown()` 用于关闭线程池
