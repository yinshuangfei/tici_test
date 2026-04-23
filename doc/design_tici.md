# 功能设计
## 文件
- tici.py
- src/bin/tici.rs

## 要求
- 修改代码的同时，同步更新 README.md 和对应的设计文档

## 功能要求
### 命令行参数功能
- 根据传入的数据库连接参数查询 TiCI 元数据表
- 默认查询 `tici.tici_shard_meta`
- 支持只输出 SQL 而不真正执行

## 入口说明
- 主脚本文件为 `tici.py`
- Rust 对应实现位于 `src/bin/tici.rs`
- 命令行默认无参数时直接执行默认查询
- 支持通过参数覆盖连接信息和目标元数据表

### 数据库基础功能
- 默认连接参数为 `mysql --comments --host 10.2.12.79 --port 9528 -u root`
- 支持通过 `--host`、`--port`、`--user`、`--password`、`--database`、`--mysql-bin` 传入连接信息

### 元数据查询
- 默认查询的元数据表为 `tici.tici_shard_meta`
- 默认查询语句如下：
```sql
select table_id,index_id,shard_id,progress from `tici`.`tici_shard_meta`;
```
- 元数据所在数据库固定为 `tici`
- 支持通过 `--meta-table` 覆盖元数据表名，默认值为 `tici_shard_meta`
- 支持通过 `--dry-run` 只输出 SQL 而不真正执行
