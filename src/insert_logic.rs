use crate::common::{
    MysqlConfig, append_progress_log, build_table_names, format_table_target,
    format_table_targets_summary, normalize_delimiter, now_log_suffix, print_stderr_log,
    print_stdout_log, project_root, quote_identifier, quote_literal, resolve_project_path,
    with_timestamp_suffix,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Pool, Value};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;

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
    batches: Arc<Vec<Vec<InsertRow>>>,
    param_batches: Option<Arc<Vec<Params>>>,
    logs: InsertLogs,
    pool: Option<Arc<Pool>>,
    import_progress: Option<ImportProgressHandle>,
    freshness_progress: Option<FreshnessProgressHandle>,
}

type InsertRow = (i64, String, String, i64);

struct InsertExecution {
    target_name: String,
    imported_rows: usize,
    baseline_row_count: u64,
}

#[derive(Clone)]
struct ImportProgressHandle {
    tracker: Arc<Mutex<ImportProgressTracker>>,
    index: usize,
}

struct ImportProgressTracker {
    started_at: Instant,
    total_rows: usize,
    total_tables: usize,
    table_states: Vec<ImportTableProgressState>,
}

struct ImportTableProgressState {
    target_name: String,
    total_rows: usize,
    imported_rows: usize,
    finished: bool,
}

#[derive(Clone)]
struct FreshnessProgressHandle {
    tracker: Arc<Mutex<FreshnessProgressTracker>>,
    index: usize,
}

struct FreshnessProgressTracker {
    started_at: Instant,
    total_rows: usize,
    total_tables: usize,
    table_states: Vec<FreshnessTableProgressState>,
}

struct FreshnessTableProgressState {
    target_name: String,
    imported_rows: usize,
    visible_rows: usize,
    waiting: bool,
    finished: bool,
}

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
        print_stderr_log(message);
    } else {
        print_stdout_log(message);
    }
    if let Some(path) = output_file {
        let _ = append_progress_log(path, message);
    }
}

fn append_log_only(output_file: &Path, message: &str) {
    let _ = append_progress_log(output_file, message);
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

fn build_multi_insert_statement(
    database: &str,
    table: &str,
    row_count: usize,
) -> Result<String, String> {
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
    let placeholders = std::iter::repeat_n("(?, ?, ?, ?)", row_count)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "INSERT INTO {target} ({columns}) VALUES {placeholders}"
    ))
}

fn build_batch_params(rows: &[InsertRow]) -> Params {
    let mut values = Vec::with_capacity(rows.len() * DEFAULT_COLUMNS.len());
    for (timestamp, severity_text, body, tenant_id) in rows {
        values.push(Value::from(*timestamp));
        values.push(Value::from(severity_text.clone()));
        values.push(Value::from(body.clone()));
        values.push(Value::from(*tenant_id));
    }
    Params::Positional(values)
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

async fn fetch_count(conn: &mut Conn, sql: &str, target: &str) -> Result<u64, String> {
    let row: Option<(u64,)> = conn.query_first(sql).await.map_err(|err| err.to_string())?;
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

async fn reconnect_pooled_conn(pool: &Pool) -> Result<Conn, String> {
    pool.get_conn().await.map_err(|err| err.to_string())
}

fn format_progress_ratio(current: usize, total: usize) -> String {
    let percent = if total == 0 {
        0.0
    } else {
        (current as f64 / total as f64) * 100.0
    };
    format!("{current}/{total}({percent:.0}%)")
}

fn emit_aggregate_import_progress(tracker: &Arc<Mutex<ImportProgressTracker>>) {
    let tracker = tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let imported_rows = tracker
        .table_states
        .iter()
        .map(|state| state.imported_rows)
        .sum::<usize>();
    let completed_tables = tracker
        .table_states
        .iter()
        .filter(|state| state.finished)
        .count();
    let running_tables = tracker.total_tables.saturating_sub(completed_tables);
    let elapsed = tracker.started_at.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        imported_rows as f64 / elapsed
    } else {
        0.0
    };
    let slowest = tracker
        .table_states
        .iter()
        .filter(|state| !state.finished)
        .min_by_key(|state| state.imported_rows)
        .map(|state| {
            format!(
                "{}:{}/{}",
                state.target_name, state.imported_rows, state.total_rows
            )
        })
        .unwrap_or_else(|| "-".to_string());
    print_stdout_log(&format!(
        "[insert-data] progress tables={} done={} running={} rows={} rate={:.0} rows/s elapsed={elapsed:.1}s slowest={slowest}",
        tracker.total_tables,
        completed_tables,
        running_tables,
        format_progress_ratio(imported_rows, tracker.total_rows),
        rate
    ));
}

fn emit_aggregate_freshness_progress(tracker: &Arc<Mutex<FreshnessProgressTracker>>) {
    let tracker = tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let visible_rows = tracker
        .table_states
        .iter()
        .map(|state| state.visible_rows)
        .sum::<usize>();
    let reached_tables = tracker
        .table_states
        .iter()
        .filter(|state| state.finished)
        .count();
    let waiting_tables = tracker
        .table_states
        .iter()
        .filter(|state| state.waiting && !state.finished)
        .count();
    let elapsed = tracker.started_at.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        visible_rows as f64 / elapsed
    } else {
        0.0
    };
    let slowest = tracker
        .table_states
        .iter()
        .filter(|state| state.waiting && !state.finished)
        .min_by_key(|state| state.visible_rows)
        .map(|state| {
            format!(
                "{}:{}/{}",
                state.target_name, state.visible_rows, state.imported_rows
            )
        })
        .unwrap_or_else(|| "-".to_string());
    print_stdout_log(&format!(
        "[insert-data] freshness tables={} reached={} waiting={} visible_rows={} rate={:.0} rows/s elapsed={elapsed:.1}s slowest={slowest}",
        tracker.total_tables,
        reached_tables,
        waiting_tables,
        format_progress_ratio(visible_rows, tracker.total_rows),
        rate
    ));
}

fn update_import_progress_imported(handle: &ImportProgressHandle, imported_rows: usize) {
    let mut tracker = handle
        .tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    tracker.table_states[handle.index].imported_rows = imported_rows;
}

fn mark_import_progress_finished(handle: &ImportProgressHandle, imported_rows: usize) {
    let mut tracker = handle
        .tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let table = &mut tracker.table_states[handle.index];
    table.imported_rows = imported_rows;
    table.finished = true;
}

fn start_freshness_progress(handle: &FreshnessProgressHandle, imported_rows: usize) {
    let mut tracker = handle
        .tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let table = &mut tracker.table_states[handle.index];
    table.imported_rows = imported_rows;
    table.waiting = true;
}

fn update_freshness_progress_visible(handle: &FreshnessProgressHandle, visible_rows: usize) {
    let mut tracker = handle
        .tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    tracker.table_states[handle.index].visible_rows = visible_rows;
}

fn mark_freshness_progress_finished(handle: &FreshnessProgressHandle, visible_rows: usize) {
    let mut tracker = handle
        .tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let table = &mut tracker.table_states[handle.index];
    table.visible_rows = visible_rows;
    table.waiting = false;
    table.finished = true;
}

async fn run_insert_tasks_tokio(
    table_runs: Vec<TableRunArgs>,
    database: String,
    table_names: Vec<String>,
    error_log: Arc<PathBuf>,
) -> i32 {
    let print_interval = table_runs
        .first()
        .map(|table_run| table_run.args.print_interval)
        .unwrap_or(DEFAULT_PROGRESS_INTERVAL);
    let import_progress_tracker = table_runs.first().and_then(|table_run| {
        table_run
            .import_progress
            .as_ref()
            .map(|handle| handle.tracker.clone())
    });
    let freshness_progress_tracker = table_runs.first().and_then(|table_run| {
        table_run
            .freshness_progress
            .as_ref()
            .map(|handle| handle.tracker.clone())
    });
    let (progress_stop_tx, mut progress_stop_rx) = watch::channel(false);
    let import_progress_tracker_for_task = import_progress_tracker.clone();
    let freshness_progress_tracker_for_task = freshness_progress_tracker.clone();
    let progress_task = if import_progress_tracker.is_some() || freshness_progress_tracker.is_some()
    {
        if print_interval > 0.0 {
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_secs_f64(print_interval)) => {
                            let import_finished = import_progress_tracker_for_task.as_ref().is_none_or(|tracker| {
                                let tracker = tracker.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                tracker.table_states.iter().all(|state| state.finished)
                            });
                            if !import_finished {
                                if let Some(tracker) = import_progress_tracker_for_task.as_ref() {
                                    emit_aggregate_import_progress(tracker);
                                }
                            } else if let Some(tracker) = freshness_progress_tracker_for_task.as_ref() {
                                let any_waiting = {
                                    let tracker = tracker.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                    tracker.table_states.iter().any(|state| state.waiting || state.finished)
                                };
                                if any_waiting {
                                    emit_aggregate_freshness_progress(tracker);
                                }
                            }
                        }
                        changed = progress_stop_rx.changed() => {
                            if changed.is_ok() && *progress_stop_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            }))
        } else {
            None
        }
    } else {
        None
    };
    let mut join_set = JoinSet::new();
    for (index, table_run) in table_runs.into_iter().enumerate() {
        join_set.spawn(async move { (index, run_insert_data_for_table(&table_run).await) });
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
    if let Some(tracker) = import_progress_tracker.as_ref() {
        emit_aggregate_import_progress(tracker);
    }
    if let Some(tracker) = freshness_progress_tracker.as_ref() {
        let any_waiting = {
            let tracker = tracker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            tracker
                .table_states
                .iter()
                .any(|state| state.waiting || state.finished)
        };
        if any_waiting {
            emit_aggregate_freshness_progress(tracker);
        }
    }
    let _ = progress_stop_tx.send(true);
    if let Some(progress_task) = progress_task {
        let _ = progress_task.await;
    }

    if failed { 1 } else { 0 }
}

async fn run_insert_batches_for_table(input: &TableRunArgs) -> Result<InsertExecution, i32> {
    let target_name = format_table_target(&input.args.database, &input.args.table);

    let started_at = Instant::now();
    let mut imported_rows = 0usize;
    let mut baseline_row_count = 0u64;
    if !input.args.dry_run {
        let pool = match input.pool.as_ref() {
            Some(pool) => pool,
            None => {
                print_stderr_log("database operation failed: missing connection pool");
                return Err(1);
            }
        };
        if input.args.freshness {
            let mut conn = match pool.get_conn().await {
                Ok(conn) => conn,
                Err(err) => {
                    print_stderr_log(&format!("database operation failed: {}", err));
                    return Err(1);
                }
            };
            match build_tikv_count_sql(&input.args.database, &input.args.table) {
                Ok(sql) => match fetch_count(&mut conn, &sql, &target_name).await {
                    Ok(value) => baseline_row_count = value,
                    Err(err) => {
                        print_stderr_log(&err);
                        return Err(1);
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    return Err(1);
                }
            }
        }
    }

    let mut insert_conn = if input.args.dry_run {
        None
    } else {
        let pool = match input.pool.as_ref() {
            Some(pool) => pool,
            None => {
                print_stderr_log("database operation failed: missing connection pool");
                return Err(1);
            }
        };
        match reconnect_pooled_conn(pool).await {
            Ok(conn) => Some(conn),
            Err(err) => {
                print_stderr_log(&format!("database operation failed: {err}"));
                return Err(1);
            }
        }
    };

    for (batch_index, batch) in input.batches.iter().enumerate() {
        if input.args.dry_run {
            match build_insert_sql(&input.args.database, &input.args.table, batch) {
                Ok(sql) => println!("{sql}"),
                Err(err) => {
                    print_stderr_log(&err);
                    return Err(2);
                }
            }
        } else {
            let params = match input
                .param_batches
                .as_ref()
                .and_then(|batches| batches.get(batch_index))
            {
                Some(params) => params.clone(),
                None => {
                    print_stderr_log("database operation failed: missing parameterized batches");
                    return Err(1);
                }
            };
            let insert_statement = match build_multi_insert_statement(
                &input.args.database,
                &input.args.table,
                batch.len(),
            ) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    return Err(2);
                }
            };
            let mut success = false;
            for attempt in 1..=DEFAULT_INSERT_RETRY_COUNT {
                let pool = match input.pool.as_ref() {
                    Some(pool) => pool,
                    None => {
                        print_stderr_log("database operation failed: missing connection pool");
                        return Err(1);
                    }
                };
                if insert_conn.is_none() {
                    insert_conn = match reconnect_pooled_conn(pool).await {
                        Ok(conn) => Some(conn),
                        Err(err) => {
                            print_stderr_log(&format!("database operation failed: {err}"));
                            return Err(1);
                        }
                    };
                }
                let conn = insert_conn
                    .as_mut()
                    .expect("insert connection must exist before exec_drop");
                match conn.exec_drop(&insert_statement, params.clone()).await {
                    Ok(_) => {
                        success = true;
                        break;
                    }
                    Err(err) => {
                        let _ = conn.query_drop("ROLLBACK").await;
                        insert_conn = None;
                        if attempt == DEFAULT_INSERT_RETRY_COUNT {
                            let message = format!(
                                "[{target_name}] [FATAL ERROR] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} rows={} error={err}",
                                batch.len()
                            );
                            emit_log(&message, Some(&input.logs.insert_error_log), true);
                            return Err(1);
                        }
                        let message = format!(
                            "[{target_name}] insert batch failed attempt={attempt}/{DEFAULT_INSERT_RETRY_COUNT} rows={} retry_in={:.1}s error={err}",
                            batch.len(),
                            DEFAULT_INSERT_RETRY_INTERVAL
                        );
                        emit_log(&message, Some(&input.logs.insert_error_log), true);
                        sleep(Duration::from_secs_f64(DEFAULT_INSERT_RETRY_INTERVAL)).await;
                    }
                }
            }
            if !success {
                return Err(1);
            }
        }
        imported_rows += batch.len();
        if let Some(progress) = input.import_progress.as_ref() {
            update_import_progress_imported(progress, imported_rows);
        }
    }

    if imported_rows == 0 {
        print_stdout_log(&format!("[{target_name}] no data rows found"));
        if let Some(progress) = input.import_progress.as_ref() {
            mark_import_progress_finished(progress, imported_rows);
        }
        return Ok(InsertExecution {
            target_name,
            imported_rows,
            baseline_row_count,
        });
    }

    let elapsed = started_at.elapsed().as_secs_f64();
    let completed_message =
        format!("[{target_name}] completed import {imported_rows} rows in {elapsed:.1}s");
    append_log_only(&input.logs.insert_result_log, &completed_message);
    if let Some(progress) = input.import_progress.as_ref() {
        mark_import_progress_finished(progress, imported_rows);
    }

    Ok(InsertExecution {
        target_name,
        imported_rows,
        baseline_row_count,
    })
}

async fn wait_freshness_for_table(
    input: &TableRunArgs,
    execution: &InsertExecution,
) -> Result<(), i32> {
    if !input.args.freshness || input.args.dry_run || execution.imported_rows == 0 {
        return Ok(());
    }

    let pool = match input.pool.as_ref() {
        Some(pool) => pool,
        None => {
            print_stderr_log("database operation failed: missing connection pool");
            return Err(1);
        }
    };
    let freshness_started_at = Instant::now();
    let mut next_progress_log_at = DEFAULT_FRESHNESS_PROGRESS_INTERVAL;
    let expected_row_count = execution.baseline_row_count + execution.imported_rows as u64;
    if let Some(progress) = input.freshness_progress.as_ref() {
        start_freshness_progress(progress, execution.imported_rows);
    } else {
        let start_message = format!(
            "[{}] freshness start baseline_row_count={} imported_rows={} expected_total_rows={expected_row_count}",
            execution.target_name, execution.baseline_row_count, execution.imported_rows
        );
        emit_log(
            &start_message,
            Some(&input.logs.freshness_progress_log),
            false,
        );
    }
    loop {
        let current_row_count = {
            let mut conn = match pool.get_conn().await {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&format!("database operation failed: {}", err));
                    return Err(1);
                }
            };
            match build_count_sql(&input.args.database, &input.args.table) {
                Ok(sql) => match fetch_count(&mut conn, &sql, &execution.target_name).await {
                    Ok(value) => value,
                    Err(err) => {
                        print_stderr_log(&err);
                        return Err(1);
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    return Err(1);
                }
            }
        };
        let visible_rows = current_row_count.saturating_sub(execution.baseline_row_count);
        if let Some(progress) = input.freshness_progress.as_ref() {
            update_freshness_progress_visible(progress, visible_rows as usize);
        }
        let freshness_elapsed = freshness_started_at.elapsed().as_secs_f64();
        if input.freshness_progress.is_none() && freshness_elapsed >= next_progress_log_at {
            let message = format!(
                "[{}] freshness poll elapsed={freshness_elapsed:.1}s baseline_row_count={} imported_rows={} visible_rows={visible_rows} total_rows={current_row_count}",
                execution.target_name, execution.baseline_row_count, execution.imported_rows
            );
            emit_log(&message, Some(&input.logs.freshness_progress_log), false);
            next_progress_log_at += DEFAULT_FRESHNESS_PROGRESS_INTERVAL;
        }
        if current_row_count == expected_row_count {
            if let Some(progress) = input.freshness_progress.as_ref() {
                mark_freshness_progress_finished(progress, visible_rows as usize);
            }
            let message = format!(
                "[{}] freshness reached elapsed={freshness_elapsed:.1}s baseline_row_count={} imported_rows={} visible_rows={visible_rows} total_rows={current_row_count}",
                execution.target_name, execution.baseline_row_count, execution.imported_rows
            );
            emit_log(&message, Some(&input.logs.freshness_result_log), false);
            return Ok(());
        }
        if freshness_elapsed >= DEFAULT_FRESHNESS_TIMEOUT {
            if let Some(progress) = input.freshness_progress.as_ref() {
                mark_freshness_progress_finished(progress, visible_rows as usize);
            }
            let message = format!(
                "[{}] freshness timeout elapsed={freshness_elapsed:.1}s baseline_row_count={} imported_rows={} visible_rows={visible_rows} total_rows={current_row_count} timeout={:.0}s",
                execution.target_name,
                execution.baseline_row_count,
                execution.imported_rows,
                DEFAULT_FRESHNESS_TIMEOUT
            );
            emit_log(&message, Some(&input.logs.freshness_result_log), true);
            return Err(1);
        }
        sleep(Duration::from_secs_f64(DEFAULT_FRESHNESS_POLL_INTERVAL)).await;
    }
}

async fn run_insert_data_for_table(input: &TableRunArgs) -> i32 {
    let execution = match run_insert_batches_for_table(input).await {
        Ok(execution) => execution,
        Err(exit_code) => return exit_code,
    };
    match wait_freshness_for_table(input, &execution).await {
        Ok(()) => 0,
        Err(exit_code) => exit_code,
    }
}

pub async fn run_insert_data(args: &InsertArgs) -> i32 {
    let mut args = args.clone();
    let freshness_batch = resolve_freshness_batch(&args);
    if args.batch_size < 1 {
        print_stderr_log("batch size must be >= 1");
        return 2;
    }
    if freshness_batch < 1 {
        print_stderr_log("freshness batch must be >= 1");
        return 2;
    }
    if args.count < 1 {
        print_stderr_log("table count must be >= 1");
        return 2;
    }
    if args.row_limit == usize::MAX {
        print_stderr_log("row limit must be >= 0");
        return 2;
    }
    if args.freshness && args.row_limit > 0 && freshness_batch > args.row_limit {
        print_stderr_log("freshness batch must be <= row limit");
        return 2;
    }
    if args.print_interval < 0.0 {
        print_stderr_log("print interval must be >= 0");
        return 2;
    }
    if args.conn_pool_size < 1 {
        print_stderr_log("conn pool size must be >= 1");
        return 2;
    }
    let csv_path = resolve_project_path(&args.csv_file);
    if !csv_path.is_file() {
        print_stderr_log(&format!("csv file not found: {}", csv_path.display()));
        return 2;
    }
    if args.encoding.to_lowercase() != "utf-8" {
        print_stderr_log(&format!(
            "failed to decode csv file with encoding {}: only utf-8 is supported in rust version",
            args.encoding
        ));
        return 2;
    }

    args.freshness_batch = Some(freshness_batch);
    let row_windows = iter_row_windows(args.row_limit, freshness_batch);
    let table_names = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            return 2;
        }
    };
    if table_names.len() > 1 {
        let joined_targets = format_table_targets_summary(&args.database, &table_names);
        let mode = if args.dry_run { "dry-run" } else { "execute" };
        print_stdout_log(&format!(
            "[insert-data] mode={mode} tables={joined_targets}"
        ));
    }

    let pool = if args.dry_run {
        None
    } else {
        match build_mysql_config(&args).build_pool(args.conn_pool_size) {
            Ok(pool) => Some(Arc::new(pool)),
            Err(err) => {
                print_stderr_log(&format!("database operation failed: {err}"));
                return 1;
            }
        }
    };

    let logs = InsertLogs::new();
    for (batch_index, (row_offset, row_limit)) in row_windows.iter().enumerate() {
        if row_windows.len() > 1 {
            print_stdout_log(&format!(
                "[insert-data] freshness-batch {}/{} row_offset={} row_limit={}",
                batch_index + 1,
                row_windows.len(),
                row_offset,
                row_limit
            ));
        }

        let delimiter = normalize_delimiter(&args.delimiter);
        let delimiter_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        let batches = match read_csv_batches(
            &csv_path,
            delimiter_byte,
            args.has_header,
            *row_offset,
            *row_limit,
            args.batch_size,
        ) {
            Ok(value) => Arc::new(value),
            Err(err) => {
                print_stderr_log(&err);
                return 2;
            }
        };
        let param_batches = if args.dry_run {
            None
        } else {
            Some(Arc::new(
                batches
                    .iter()
                    .map(|batch| build_batch_params(batch))
                    .collect::<Vec<_>>(),
            ))
        };
        let total_rows_per_table = batches.iter().map(|batch| batch.len()).sum::<usize>();
        let progress_tracker = if args.dry_run || args.print_interval <= 0.0 {
            None
        } else {
            Some(Arc::new(Mutex::new(ImportProgressTracker {
                started_at: Instant::now(),
                total_rows: total_rows_per_table * table_names.len(),
                total_tables: table_names.len(),
                table_states: table_names
                    .iter()
                    .map(|table_name| ImportTableProgressState {
                        target_name: format_table_target(&args.database, table_name),
                        total_rows: total_rows_per_table,
                        imported_rows: 0,
                        finished: false,
                    })
                    .collect(),
            })))
        };
        let freshness_progress_tracker =
            if args.dry_run || !args.freshness || args.print_interval <= 0.0 {
                None
            } else {
                Some(Arc::new(Mutex::new(FreshnessProgressTracker {
                    started_at: Instant::now(),
                    total_rows: total_rows_per_table * table_names.len(),
                    total_tables: table_names.len(),
                    table_states: table_names
                        .iter()
                        .map(|table_name| FreshnessTableProgressState {
                            target_name: format_table_target(&args.database, table_name),
                            imported_rows: 0,
                            visible_rows: 0,
                            waiting: false,
                            finished: false,
                        })
                        .collect(),
                })))
            };

        let table_runs = table_names
            .iter()
            .enumerate()
            .map(|(index, table_name)| {
                let mut table_args = args.clone();
                table_args.table = table_name.clone();
                table_args.count = 1;
                TableRunArgs {
                    args: table_args,
                    batches: batches.clone(),
                    param_batches: param_batches.clone(),
                    logs: logs.clone(),
                    pool: pool.as_ref().cloned(),
                    import_progress: progress_tracker.as_ref().map(|tracker| {
                        ImportProgressHandle {
                            tracker: tracker.clone(),
                            index,
                        }
                    }),
                    freshness_progress: freshness_progress_tracker.as_ref().map(|tracker| {
                        FreshnessProgressHandle {
                            tracker: tracker.clone(),
                            index,
                        }
                    }),
                }
            })
            .collect::<Vec<_>>();

        if args.dry_run || table_runs.len() == 1 {
            for table_run in &table_runs {
                let exit_code = run_insert_data_for_table(table_run).await;
                if exit_code != 0 {
                    return exit_code;
                }
            }
            continue;
        }

        print_stdout_log(&format!("[insert-data] tasks={}", table_runs.len()));
        let exit_code = run_insert_tasks_tokio(
            table_runs,
            args.database.clone(),
            table_names.clone(),
            logs.insert_error_log.clone(),
        )
        .await;
        if exit_code != 0 {
            return exit_code;
        }
    }
    0
}
