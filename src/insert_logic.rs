use crate::common::{
    MysqlConfig, append_progress_log, build_table_names, format_table_target, normalize_delimiter,
    now_log_suffix, project_root, quote_identifier, quote_literal, resolve_project_path,
    with_timestamp_suffix,
};
use mysql::params;
use mysql::prelude::Queryable;
use mysql::{Pool, PooledConn};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::task::JoinSet;

pub const DEFAULT_HOST: &str = "10.2.12.79";
pub const DEFAULT_PORT: u16 = 9528;
pub const DEFAULT_USER: &str = "root";
pub const DEFAULT_DATABASE: &str = "test";
pub const DEFAULT_TABLE: &str = "hdfs_log";
pub const DEFAULT_CSV_FILE: &str = "data/hdfs-logs-multitenants.csv";
pub const DEFAULT_BATCH_SIZE: usize = 1000;
pub const DEFAULT_ROW_LIMIT: usize = 100000;
pub const DEFAULT_PROGRESS_INTERVAL: f64 = 3.0;
pub const DEFAULT_CONN_POOL_SIZE: usize = 1000;
pub const DEFAULT_INSERT_RETRY_COUNT: usize = 10;
pub const DEFAULT_INSERT_RETRY_INTERVAL: f64 = 1.0;
pub const DEFAULT_FRESHNESS_TIMEOUT: f64 = 10.0 * 60.0;
pub const DEFAULT_FRESHNESS_POLL_INTERVAL: f64 = 5.0;
pub const DEFAULT_FRESHNESS_PROGRESS_INTERVAL: f64 = 60.0;
pub const DEFAULT_FRESHNESS_WHERE: &str =
    "where fts_match_word('china',body) or not fts_match_word('china',body)";
pub const DEFAULT_COLUMNS: [&str; 4] = ["timestamp", "severity_text", "body", "tenant_id"];

#[derive(Clone, Debug)]
pub struct InsertArgs {
    pub csv_file: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    pub count: usize,
    pub table_offset: usize,
    pub batch_size: usize,
    pub row_limit: usize,
    pub freshness_batch: Option<usize>,
    pub print_interval: f64,
    pub conn_pool_size: usize,
    pub encoding: String,
    pub delimiter: String,
    pub has_header: bool,
    pub freshness: bool,
    pub dry_run: bool,
}

impl Default for InsertArgs {
    fn default() -> Self {
        Self {
            csv_file: DEFAULT_CSV_FILE.to_string(),
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            user: DEFAULT_USER.to_string(),
            password: String::new(),
            database: DEFAULT_DATABASE.to_string(),
            table: DEFAULT_TABLE.to_string(),
            count: 1,
            table_offset: 0,
            batch_size: DEFAULT_BATCH_SIZE,
            row_limit: DEFAULT_ROW_LIMIT,
            freshness_batch: None,
            print_interval: DEFAULT_PROGRESS_INTERVAL,
            conn_pool_size: DEFAULT_CONN_POOL_SIZE,
            encoding: "utf-8".to_string(),
            delimiter: ",".to_string(),
            has_header: false,
            freshness: true,
            dry_run: false,
        }
    }
}

#[derive(Clone)]
pub struct InsertLogs {
    pub insert_result_log: Arc<PathBuf>,
    pub insert_error_log: Arc<PathBuf>,
    pub freshness_progress_log: Arc<PathBuf>,
    pub freshness_result_log: Arc<PathBuf>,
}

impl InsertLogs {
    pub fn new() -> Self {
        let suffix = now_log_suffix();
        let log_dir = project_root().join("log");
        Self {
            insert_result_log: Arc::new(with_timestamp_suffix(
                &log_dir.join("insert_result.log"),
                &suffix,
            )),
            insert_error_log: Arc::new(with_timestamp_suffix(
                &log_dir.join("insert_error.log"),
                &suffix,
            )),
            freshness_progress_log: Arc::new(with_timestamp_suffix(
                &log_dir.join("freshness_progress.log"),
                &suffix,
            )),
            freshness_result_log: Arc::new(with_timestamp_suffix(
                &log_dir.join("freshness_result.log"),
                &suffix,
            )),
        }
    }
}

#[derive(Clone)]
struct TableRunArgs {
    args: InsertArgs,
    row_offset: usize,
    row_limit: usize,
    logs: InsertLogs,
    pool: Option<Arc<Pool>>,
}

type InsertRow = (i64, String, String, i64);

pub fn build_mysql_config(args: &InsertArgs) -> MysqlConfig {
    MysqlConfig {
        host: args.host.clone(),
        port: args.port,
        user: args.user.clone(),
        password: args.password.clone(),
        database: args.database.clone(),
        mysql_bin: "mysql".to_string(),
        comments: true,
    }
}

pub fn resolve_freshness_batch(args: &InsertArgs) -> usize {
    if let Some(value) = args.freshness_batch {
        return value;
    }
    if args.row_limit > 0 {
        return args.row_limit;
    }
    1
}

fn emit_log(message: &str, output_file: Option<&Path>, stderr: bool) {
    if stderr {
        eprintln!("{message}");
    } else {
        println!("{message}");
    }
    if let Some(path) = output_file {
        let _ = append_progress_log(path, message);
    }
}

fn parse_row(raw_row: &csv::StringRecord, line_number: usize) -> Result<InsertRow, String> {
    if raw_row.len() != 4 {
        return Err(format!(
            "line {line_number}: expected 4 columns, got {}",
            raw_row.len()
        ));
    }
    let timestamp_raw = raw_row.get(0).unwrap_or_default();
    let severity_text = raw_row.get(1).unwrap_or_default().to_string();
    let body = raw_row.get(2).unwrap_or_default().to_string();
    let tenant_id_raw = raw_row.get(3).unwrap_or_default();
    let timestamp = timestamp_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| format!("line {line_number}: invalid timestamp {timestamp_raw:?}"))?;
    let tenant_id = tenant_id_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| format!("line {line_number}: invalid tenant_id {tenant_id_raw:?}"))?;
    Ok((timestamp, severity_text, body, tenant_id))
}

fn read_csv_batches(
    csv_path: &Path,
    delimiter: u8,
    has_header: bool,
    row_offset: usize,
    row_limit: usize,
    batch_size: usize,
) -> Result<Vec<Vec<InsertRow>>, String> {
    let file = File::open(csv_path).map_err(|err| err.to_string())?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter)
        .from_reader(file);
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    let mut line_number = if has_header { 2 } else { 1 };
    let mut batch = Vec::new();
    let mut batches = Vec::new();
    for record in reader.records() {
        let row = record.map_err(|err| err.to_string())?;
        if row.is_empty() {
            line_number += 1;
            continue;
        }
        if row_limit > 0 && emitted >= row_limit {
            break;
        }
        if skipped < row_offset {
            skipped += 1;
            line_number += 1;
            continue;
        }
        batch.push(parse_row(&row, line_number)?);
        emitted += 1;
        line_number += 1;
        if batch.len() >= batch_size {
            batches.push(std::mem::take(&mut batch));
        }
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}

pub fn build_insert_sql(database: &str, table: &str, rows: &[InsertRow]) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier(database)?,
        quote_identifier(table)?
    );
    let columns = DEFAULT_COLUMNS
        .iter()
        .map(|item| quote_identifier(item))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let values = rows
        .iter()
        .map(|(timestamp, severity_text, body, tenant_id)| {
            format!(
                "({timestamp}, {}, {}, {tenant_id})",
                quote_literal(severity_text),
                quote_literal(body)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    Ok(format!(
        "INSERT INTO {target} ({columns}) VALUES\n{values};"
    ))
}

fn build_insert_statement(database: &str, table: &str) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier(database)?,
        quote_identifier(table)?
    );
    let columns = DEFAULT_COLUMNS
        .iter()
        .map(|item| quote_identifier(item))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "INSERT INTO {target} ({columns}) VALUES (:timestamp, :severity_text, :body, :tenant_id)"
    ))
}

fn build_count_sql(database: &str, table: &str) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier(database)?,
        quote_identifier(table)?
    );
    Ok(format!(
        "SELECT COUNT(*) FROM {target} {DEFAULT_FRESHNESS_WHERE}"
    ))
}

fn build_tikv_count_sql(database: &str, table: &str) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier(database)?,
        quote_identifier(table)?
    );
    Ok(format!("SELECT COUNT(*) FROM {target}"))
}

fn fetch_count(conn: &mut mysql::PooledConn, sql: &str, target: &str) -> Result<u64, String> {
    let row: Option<(u64,)> = conn.query_first(sql).map_err(|err| err.to_string())?;
    row.map(|value| value.0)
        .ok_or_else(|| format!("failed to query row count for {target}"))
}

fn iter_row_windows(row_limit: usize, freshness_batch: usize) -> Vec<(usize, usize)> {
    if row_limit == 0 {
        return vec![(0, 0)];
    }
    let mut windows = Vec::new();
    let mut row_offset = 0usize;
    while row_offset < row_limit {
        let current_limit = freshness_batch.min(row_limit - row_offset);
        windows.push((row_offset, current_limit));
        row_offset += current_limit;
    }
    windows
}

fn reconnect_pooled_conn(pool: &Pool) -> Result<PooledConn, String> {
    pool.get_conn().map_err(|err| err.to_string())
}

async fn run_insert_tasks_tokio(
    table_runs: Vec<TableRunArgs>,
    database: String,
    table_names: Vec<String>,
    error_log: Arc<PathBuf>,
) -> i32 {
    let mut join_set = JoinSet::new();
    for (index, table_run) in table_runs.into_iter().enumerate() {
        join_set.spawn_blocking(move || (index, run_insert_data_for_table(&table_run)));
    }

    let mut failed = false;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((index, exit_code)) => {
                if exit_code != 0 {
                    failed = true;
                    let message = format!(
                        "[insert-data] failed target={} exit_code={exit_code}",
                        format_table_target(&database, &table_names[index])
                    );
                    emit_log(&message, Some(&error_log), true);
                }
            }
            Err(err) => {
                failed = true;
                let message = format!("[insert-data] task join failed error={err}");
                emit_log(&message, Some(&error_log), true);
            }
        }
    }

    if failed { 1 } else { 0 }
}

fn run_insert_data_for_table(input: &TableRunArgs) -> i32 {
    let csv_path = resolve_project_path(&input.args.csv_file);
    let target_name = format_table_target(&input.args.database, &input.args.table);
    let delimiter = normalize_delimiter(&input.args.delimiter);
    let delimiter_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
    let batches = match read_csv_batches(
        &csv_path,
        delimiter_byte,
        input.args.has_header,
        input.row_offset,
        input.row_limit,
        input.args.batch_size,
    ) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let insert_statement = match build_insert_statement(&input.args.database, &input.args.table) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };

    let mut imported_rows = 0usize;
    let started_at = Instant::now();
    let mut last_progress_at = started_at;
    let mut baseline_row_count = 0u64;
    if !input.args.dry_run {
        let pool = match input.pool.as_ref() {
            Some(pool) => pool,
            None => {
                eprintln!("database operation failed: missing connection pool");
                return 1;
            }
        };
        if input.args.freshness {
            let mut conn = match pool.get_conn() {
                Ok(conn) => conn,
                Err(err) => {
                    eprintln!("database operation failed: {}", err);
                    return 1;
                }
            };
            match build_tikv_count_sql(&input.args.database, &input.args.table)
                .and_then(|sql| fetch_count(&mut conn, &sql, &target_name))
            {
                Ok(value) => baseline_row_count = value,
                Err(err) => {
                    eprintln!("{err}");
                    return 1;
                }
            }
        }
    }

    for batch in &batches {
        if input.args.dry_run {
            match build_insert_sql(&input.args.database, &input.args.table, batch) {
                Ok(sql) => println!("{sql}"),
                Err(err) => {
                    eprintln!("{err}");
                    return 2;
                }
            }
        } else {
            let params = batch
                .iter()
                .map(|(timestamp, severity_text, body, tenant_id)| {
                    params! {
                        "timestamp" => *timestamp,
                        "severity_text" => severity_text.clone(),
                        "body" => body.clone(),
                        "tenant_id" => *tenant_id,
                    }
                })
                .collect::<Vec<_>>();
            let mut success = false;
            for attempt in 1..=DEFAULT_INSERT_RETRY_COUNT {
                let pool = match input.pool.as_ref() {
                    Some(pool) => pool,
                    None => {
                        eprintln!("database operation failed: missing connection pool");
                        return 1;
                    }
                };
                let mut conn = match reconnect_pooled_conn(pool) {
                    Ok(conn) => conn,
                    Err(err) => {
                        eprintln!("database operation failed: {err}");
                        return 1;
                    }
                };
                match conn.exec_batch(&insert_statement, params.clone()) {
                    Ok(_) => {
                        success = true;
                        break;
                    }
                    Err(err) => {
                        let _ = conn.query_drop("ROLLBACK");
                        if attempt == DEFAULT_INSERT_RETRY_COUNT {
                            let message = format!(
                                "[{target_name}] [FATAL ERROR] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} rows={} error={err}",
                                batch.len()
                            );
                            emit_log(&message, Some(&input.logs.insert_error_log), true);
                            return 1;
                        }
                        let message = format!(
                            "[{target_name}] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} rows={} retry_in={:.1}s error={err}",
                            batch.len(),
                            DEFAULT_INSERT_RETRY_INTERVAL
                        );
                        emit_log(&message, Some(&input.logs.insert_error_log), true);
                        thread::sleep(Duration::from_secs_f64(DEFAULT_INSERT_RETRY_INTERVAL));
                    }
                }
            }
            if !success {
                return 1;
            }
        }
        imported_rows += batch.len();
        let now = Instant::now();
        if input.args.print_interval == 0.0
            || now.duration_since(last_progress_at).as_secs_f64() >= input.args.print_interval
        {
            println!(
                "[{target_name}] progress imported {imported_rows} rows in {:.1}s",
                now.duration_since(started_at).as_secs_f64()
            );
            last_progress_at = now;
        }
    }

    if imported_rows == 0 {
        println!("[{target_name}] no data rows found");
        return 0;
    }

    let elapsed = started_at.elapsed().as_secs_f64();
    let completed_message =
        format!("[{target_name}] completed import {imported_rows} rows in {elapsed:.1}s");
    emit_log(
        &completed_message,
        Some(&input.logs.insert_result_log),
        false,
    );

    if input.args.freshness && !input.args.dry_run {
        let pool = match input.pool.as_ref() {
            Some(pool) => pool,
            None => {
                eprintln!("database operation failed: missing connection pool");
                return 1;
            }
        };
        let freshness_started_at = Instant::now();
        let mut next_progress_log_at = DEFAULT_FRESHNESS_PROGRESS_INTERVAL;
        let expected_row_count = baseline_row_count + imported_rows as u64;
        let start_message = format!(
            "[{target_name}] freshness start baseline_row_count={baseline_row_count} imported_rows={imported_rows} expected_total_rows={expected_row_count}"
        );
        emit_log(
            &start_message,
            Some(&input.logs.freshness_progress_log),
            false,
        );
        loop {
            let current_row_count = {
                let mut conn = match pool.get_conn() {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!("database operation failed: {}", err);
                        return 1;
                    }
                };
                match build_count_sql(&input.args.database, &input.args.table)
                    .and_then(|sql| fetch_count(&mut conn, &sql, &target_name))
                {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!("{err}");
                        return 1;
                    }
                }
            };
            let visible_rows = current_row_count.saturating_sub(baseline_row_count);
            let freshness_elapsed = freshness_started_at.elapsed().as_secs_f64();
            if freshness_elapsed >= next_progress_log_at {
                let message = format!(
                    "[{target_name}] freshness poll elapsed={freshness_elapsed:.1}s baseline_row_count={baseline_row_count} imported_rows={imported_rows} visible_rows={visible_rows} total_rows={current_row_count}"
                );
                emit_log(&message, Some(&input.logs.freshness_progress_log), false);
                next_progress_log_at += DEFAULT_FRESHNESS_PROGRESS_INTERVAL;
            }
            if current_row_count == expected_row_count {
                let message = format!(
                    "[{target_name}] freshness reached elapsed={freshness_elapsed:.1}s baseline_row_count={baseline_row_count} imported_rows={imported_rows} visible_rows={visible_rows} total_rows={current_row_count}"
                );
                emit_log(&message, Some(&input.logs.freshness_result_log), false);
                break;
            }
            if freshness_elapsed >= DEFAULT_FRESHNESS_TIMEOUT {
                let message = format!(
                    "[{target_name}] freshness timeout elapsed={freshness_elapsed:.1}s baseline_row_count={baseline_row_count} imported_rows={imported_rows} visible_rows={visible_rows} total_rows={current_row_count} timeout={:.0}s",
                    DEFAULT_FRESHNESS_TIMEOUT
                );
                emit_log(&message, Some(&input.logs.freshness_result_log), true);
                return 1;
            }
            thread::sleep(Duration::from_secs_f64(DEFAULT_FRESHNESS_POLL_INTERVAL));
        }
    }

    0
}

pub fn run_insert_data(args: &InsertArgs) -> i32 {
    let mut args = args.clone();
    let freshness_batch = resolve_freshness_batch(&args);
    if args.batch_size < 1 {
        eprintln!("batch size must be >= 1");
        return 2;
    }
    if freshness_batch < 1 {
        eprintln!("freshness batch must be >= 1");
        return 2;
    }
    if args.count < 1 {
        eprintln!("table count must be >= 1");
        return 2;
    }
    if args.row_limit == usize::MAX {
        eprintln!("row limit must be >= 0");
        return 2;
    }
    if args.freshness && args.row_limit > 0 && freshness_batch > args.row_limit {
        eprintln!("freshness batch must be <= row limit");
        return 2;
    }
    if args.print_interval < 0.0 {
        eprintln!("print interval must be >= 0");
        return 2;
    }
    if args.conn_pool_size < 1 {
        eprintln!("conn pool size must be >= 1");
        return 2;
    }
    let csv_path = resolve_project_path(&args.csv_file);
    if !csv_path.is_file() {
        eprintln!("csv file not found: {}", csv_path.display());
        return 2;
    }
    if args.encoding.to_lowercase() != "utf-8" {
        eprintln!(
            "failed to decode csv file with encoding {}: only utf-8 is supported in rust version",
            args.encoding
        );
        return 2;
    }

    args.freshness_batch = Some(freshness_batch);
    let row_windows = iter_row_windows(args.row_limit, freshness_batch);
    let table_names = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    if table_names.len() > 1 {
        let joined_targets = table_names
            .iter()
            .map(|table_name| format_table_target(&args.database, table_name))
            .collect::<Vec<_>>()
            .join(", ");
        let mode = if args.dry_run { "dry-run" } else { "execute" };
        println!("[insert-data] mode={mode} tables={joined_targets}");
    }

    let pool = if args.dry_run {
        None
    } else {
        match build_mysql_config(&args).build_pool(args.conn_pool_size) {
            Ok(pool) => Some(Arc::new(pool)),
            Err(err) => {
                eprintln!("database operation failed: {err}");
                return 1;
            }
        }
    };

    let logs = InsertLogs::new();
    for (batch_index, (row_offset, row_limit)) in row_windows.iter().enumerate() {
        if row_windows.len() > 1 {
            println!(
                "[insert-data] freshness-batch {}/{} row_offset={} row_limit={}",
                batch_index + 1,
                row_windows.len(),
                row_offset,
                row_limit
            );
        }

        let table_runs = table_names
            .iter()
            .map(|table_name| {
                let mut table_args = args.clone();
                table_args.table = table_name.clone();
                table_args.count = 1;
                TableRunArgs {
                    args: table_args,
                    row_offset: *row_offset,
                    row_limit: *row_limit,
                    logs: logs.clone(),
                    pool: pool.as_ref().cloned(),
                }
            })
            .collect::<Vec<_>>();

        if args.dry_run || table_runs.len() == 1 {
            for table_run in &table_runs {
                let exit_code = run_insert_data_for_table(table_run);
                if exit_code != 0 {
                    return exit_code;
                }
            }
            continue;
        }

        println!("[insert-data] tasks={}", table_runs.len());
        let runtime = match RuntimeBuilder::new_multi_thread().enable_all().build() {
            Ok(runtime) => runtime,
            Err(err) => {
                eprintln!("failed to build tokio runtime: {err}");
                return 1;
            }
        };
        let exit_code = runtime.block_on(run_insert_tasks_tokio(
            table_runs,
            args.database.clone(),
            table_names.clone(),
            logs.insert_error_log.clone(),
        ));
        if exit_code != 0 {
            return exit_code;
        }
    }
    0
}
