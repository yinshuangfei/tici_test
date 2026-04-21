# 功能设计
## 文件
- analyze_freshness.py

## 要求
- 修改代码的同时，同步更新 README.md 和对应的设计文档

## 功能描述
- 扫描 `log/` 目录下的 `freshness_result_*.log`
- 解析 freshness 达成日志
- 过滤格式不符合要求的行
- 校验 `total_rows == baseline_row_count + imported_rows`
- 按文件输出统计结果
- 按 `baseline_row_count` 的万级区间下界分组并输出统计结果
- 汇总所有文件并输出 `Summarize`

## 实现细节
- 脚本文件为 `analyze_freshness.py`
- 命令行默认无参数时直接执行统计，默认扫描 `log/` 目录
- 支持的可选参数如下：
  - `--log-dir`，默认 `log`
  - `--pattern`，默认 `freshness_result_*.log`
  - `--show-invalid`，指定后输出无效行明细
- 日志行按以下格式解析：
```
[2026-04-20 11:46:22] [test.hdfs_log_1876] freshness reached elapsed=17.1s baseline_row_count=49000 imported_rows=10000 visible_rows=10000 total_rows=59000
```
- 当日志行格式不匹配时，忽略该行，不参与统计
- 当 `total_rows != baseline_row_count + imported_rows` 时，该行记为无效行，不参与分组统计
- 分组规则为：
  - 取 `(baseline_row_count // 10000) * 10000`
  - 组名格式为 `group-<num>`
  - 例如 `13000` 和 `19344` 归到 `group-10000`
  - 例如 `21344` 和 `26000` 归到 `group-20000`
- 每个分组输出以下字段：
  - `avg_inserted`
  - `lines`
  - `avg_elapsed`
- 统计结果按文件分类输出
- 每个文件输出时会先打印文件名
- 每个文件的表头为：
```
起始行数量级, 平均插入行数, 统计条目数, 平均耗时(s)
```
- 输出格式为：
```
freshness_result_20260420_080047.log
起始行数量级, 平均插入行数, 统计条目数, 平均耗时(s)
group-10000, 10000.00, 10, 12.34
```
- 所有文件输出完成后追加：
```
Summarize
起始行数量级, 平均插入行数, 统计条目数, 平均耗时(s)
group-10000, 10000.00, 20, 23.45
```
- 当指定 `--show-invalid` 且存在无效行时，在对应文件统计结果后附加输出无效行文件位置、原因和原始内容
