use clap::{Parser, Subcommand};
use mysql_async::Pool;
use tici_test_rust::common::{
    build_table_names, format_table_target, format_table_targets_summary, quote_identifier,
    quote_literal,
};
use tici_test_rust::create_table::build_create_table_items;
use tici_test_rust::drop_table::build_drop_table_items;
use tici_test_rust::insert_table::{
    DEFAULT_BATCH_SIZE, DEFAULT_CONN_POOL_SIZE, DEFAULT_CSV_FILE, DEFAULT_DATABASE, DEFAULT_HOST,
    DEFAULT_PORT, DEFAULT_PROGRESS_INTERVAL, DEFAULT_ROW_LIMIT, DEFAULT_TABLE, DEFAULT_USER,
    InsertArgs, run_insert_data,
};
use tici_test_rust::log::{print_stderr_log, print_stdout_log};
use tici_test_rust::query_table::{
    CheckTableArgs, DEFAULT_QUERY_SQL, QueryConnArgs, QueryTableArgs, run_check, run_query,
};
use tici_test_rust::sql_pool::{MysqlConfig, build_optional_pool, execute_sql_with_pool};
use tokio::task::JoinSet;

const DEFAULT_INDEX: &str = "ft_idx";
const DEFAULT_IMPORT_SOURCE: &str = "s3://data/import-src/hdfs-logs-multitenants.csv?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true";
const DEFAULT_IMPORT_SORT_DIR: &str = "s3://ticidefaultbucket/tici/import-sort?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true";
const DEFAULT_IMPORT_THREAD: usize = 8;
const DEFAULT_AUTO_ROW_LIMIT: usize = 100000;
#[derive(Parser, Debug)]
#[command(about = "Table tools for TiCI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    CreateTable(CommonTableArgs),
    DropTable(CommonTableArgs),
    AddIndex(IndexArgs),
    DropIndex(DropIndexArgs),
    ImportInto(ImportArgs),
    Query(QueryArgs),
    Check(CheckArgs),
    InsertData(InsertCliArgs),
    Auto(AutoArgs),
}

#[derive(Parser, Debug, Clone)]
struct CommonConnArgs {
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    #[arg(long, default_value = DEFAULT_USER)]
    user: String,
    #[arg(long, default_value = "")]
    password: String,
    #[arg(long, default_value = DEFAULT_DATABASE)]
    database: String,
    #[arg(long, default_value = "mysql")]
    mysql_bin: String,
    #[arg(long, default_value_t = DEFAULT_CONN_POOL_SIZE)]
    conn_pool_size: usize,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Parser, Debug, Clone)]
struct CommonTableArgs {
    #[command(flatten)]
    conn: CommonConnArgs,
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    #[arg(long, default_value_t = 1)]
    count: usize,
    #[arg(long, default_value_t = 0)]
    table_offset: usize,
}

#[derive(Parser, Debug, Clone)]
struct IndexArgs {
    #[command(flatten)]
    common: CommonTableArgs,
    #[arg(long, default_value = DEFAULT_INDEX)]
    index_name: String,
    #[arg(long, default_value = "body")]
    column: String,
}

#[derive(Parser, Debug, Clone)]
struct DropIndexArgs {
    #[command(flatten)]
    common: CommonTableArgs,
    #[arg(long, default_value = DEFAULT_INDEX)]
    index_name: String,
}

#[derive(Parser, Debug, Clone)]
struct ImportArgs {
    #[command(flatten)]
    conn: CommonConnArgs,
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    #[arg(long, default_value = "timestamp,severity_text,body,tenant_id")]
    columns: String,
    #[arg(long, default_value = DEFAULT_IMPORT_SOURCE)]
    source: String,
    #[arg(long, default_value = DEFAULT_IMPORT_SORT_DIR)]
    cloud_storage_uri: String,
    #[arg(long, default_value_t = DEFAULT_IMPORT_THREAD)]
    thread: usize,
    #[arg(long, default_value_t = false)]
    no_detached: bool,
    #[arg(long, default_value_t = false)]
    no_disable_precheck: bool,
    #[arg(long = "with-option")]
    with_option: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
struct QueryArgs {
    #[command(flatten)]
    conn: CommonConnArgs,
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    #[arg(long, default_value_t = 1)]
    count: usize,
    #[arg(long, default_value_t = 0)]
    table_offset: usize,
    #[arg(long, default_value = DEFAULT_QUERY_SQL)]
    sql: String,
    #[arg(long, default_value_t = false)]
    tikv: bool,
    #[arg(long, default_value_t = 1)]
    query_loop_count: usize,
}

#[derive(Parser, Debug, Clone)]
struct CheckArgs {
    #[command(flatten)]
    conn: CommonConnArgs,
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    #[arg(long, default_value_t = 1)]
    count: usize,
    #[arg(long, default_value_t = 0)]
    table_offset: usize,
    #[arg(long, default_value = DEFAULT_QUERY_SQL)]
    sql: String,
    #[arg(long, default_value_t = 1)]
    query_loop_count: usize,
}

#[derive(Parser, Debug, Clone)]
struct InsertCliArgs {
    #[arg(default_value = DEFAULT_CSV_FILE)]
    csv_file: String,
    #[command(flatten)]
    conn: CommonConnArgs,
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    #[arg(long, default_value_t = 1)]
    count: usize,
    #[arg(long, default_value_t = 0)]
    table_offset: usize,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_ROW_LIMIT)]
    row_limit: usize,
    #[arg(long)]
    freshness_batch: Option<usize>,
    #[arg(long, default_value_t = DEFAULT_PROGRESS_INTERVAL)]
    print_interval: f64,
    #[arg(long, default_value = "utf-8")]
    encoding: String,
    #[arg(long, default_value = ",")]
    delimiter: String,
    #[arg(long, default_value_t = false)]
    has_header: bool,
    #[arg(long = "no-freshness", default_value_t = false)]
    no_freshness: bool,
}

#[derive(Parser, Debug, Clone)]
struct AutoArgs {
    #[command(flatten)]
    common: CommonTableArgs,
    #[arg(long, default_value = DEFAULT_INDEX)]
    index_name: String,
    #[arg(long, default_value = "body")]
    column: String,
    #[arg(default_value = DEFAULT_CSV_FILE)]
    csv_file: String,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_AUTO_ROW_LIMIT)]
    row_limit: usize,
    #[arg(long)]
    freshness_batch: Option<usize>,
    #[arg(long, default_value_t = DEFAULT_PROGRESS_INTERVAL)]
    print_interval: f64,
    #[arg(long, default_value = "utf-8")]
    encoding: String,
    #[arg(long, default_value = ",")]
    delimiter: String,
    #[arg(long, default_value_t = false)]
    has_header: bool,
    #[arg(long = "no-freshness", default_value_t = false)]
    no_freshness: bool,
}

fn build_client(conn: &CommonConnArgs) -> MysqlConfig {
    MysqlConfig {
        host: conn.host.clone(),
        port: conn.port,
        user: conn.user.clone(),
        password: conn.password.clone(),
        database: conn.database.clone(),
        mysql_bin: conn.mysql_bin.clone(),
        comments: true,
    }
}

fn print_command_summary(command: &str, database: &str, table_names: &[String], dry_run: bool) {
    let joined = format_table_targets_summary(database, table_names);
    let mode = if dry_run { "dry-run" } else { "execute" };
    print_stdout_log(&format!("[{command}] mode={mode} tables={joined}"));
}

fn add_index_sql(
    database: &str,
    table_name: &str,
    index_name: &str,
    column: &str,
) -> Result<String, String> {
    Ok(format!(
        "ALTER TABLE {}.{} ADD FULLTEXT INDEX {}({});",
        quote_identifier(database)?,
        quote_identifier(table_name)?,
        quote_identifier(index_name)?,
        quote_identifier(column)?
    ))
}

fn drop_index_sql(database: &str, table_name: &str, index_name: &str) -> Result<String, String> {
    Ok(format!(
        "ALTER TABLE {}.{} DROP INDEX {};",
        quote_identifier(database)?,
        quote_identifier(table_name)?,
        quote_identifier(index_name)?
    ))
}

fn parse_columns(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_with_options(values: &[String]) -> Vec<String> {
    values
        .iter()
        .filter_map(|item| {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                return None;
            }
            if let Some((key, value)) = trimmed.split_once('=') {
                let key = key.trim().to_uppercase();
                let raw = value.trim();
                if raw.eq_ignore_ascii_case("true")
                    || raw.eq_ignore_ascii_case("false")
                    || raw.eq_ignore_ascii_case("null")
                {
                    return Some(format!("{key}={}", raw.to_lowercase()));
                }
                if raw.parse::<f64>().is_ok() {
                    return Some(format!("{key}={raw}"));
                }
                if raw
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                {
                    return Some(format!("{key}={}", raw.to_uppercase()));
                }
                return Some(format!("{key}={}", quote_literal(raw)));
            }
            Some(trimmed.to_uppercase())
        })
        .collect()
}

fn import_into_sql(args: &ImportArgs) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier(&args.conn.database)?,
        quote_identifier(&args.table)?
    );
    let columns = parse_columns(&args.columns);
    let column_clause = if columns.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            columns
                .iter()
                .map(|item| quote_identifier(item))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        )
    };
    let mut clauses = vec![format!(
        "CLOUD_STORAGE_URI={}",
        quote_literal(&args.cloud_storage_uri)
    )];
    if !args.no_detached {
        clauses.push("DETACHED".to_string());
    }
    if !args.no_disable_precheck {
        clauses.push("DISABLE_PRECHECK".to_string());
    }
    clauses.push(format!("THREAD={}", args.thread));
    clauses.extend(parse_with_options(&args.with_option));
    Ok(format!(
        "IMPORT INTO {target}{column_clause}\nFROM {}\nWITH \n    {};",
        quote_literal(&args.source),
        clauses.join(",\n    ")
    ))
}

async fn run_sqls(
    pool: Option<&Pool>,
    dry_run: bool,
    parallel: bool,
    items: &[(String, String, String)],
) -> Result<(), String> {
    fn is_ignorable_add_index_error(action: &str, err: &str) -> bool {
        action == "add-index"
            && (err.contains("Duplicate key name")
                || err.contains("index already exist")
                || err.contains("a background job is trying to add the same index"))
    }

    if dry_run || !parallel || items.len() <= 1 {
        for (action, target, sql) in items {
            print_stdout_log(&format!("[{action}] target={target}"));
            if dry_run {
                println!("{sql}");
            } else {
                let pool = pool.ok_or_else(|| "missing connection pool".to_string())?;
                match execute_sql_with_pool(pool, sql).await {
                    Ok(()) => {}
                    Err(err) if is_ignorable_add_index_error(action, &err) => {
                        print_stdout_log(&format!(
                            "[{action}] skip target={target} reason=index-exists"
                        ));
                    }
                    Err(err) => {
                        return Err(format!(
                            "[{action}] failed target={target} sql={sql:?} error={err}"
                        ));
                    }
                }
            }
        }
        return Ok(());
    }
    print_stdout_log(&format!("[sql] tasks={}", items.len()));
    let pool = pool
        .ok_or_else(|| "missing connection pool".to_string())?
        .clone();
    let mut join_set = JoinSet::new();
    for (action, target, sql) in items.iter().cloned() {
        let task_pool = pool.clone();
        join_set.spawn(async move {
            print_stdout_log(&format!("[{action}] target={target}"));
            match execute_sql_with_pool(&task_pool, &sql).await {
                Ok(()) => {
                    print_stdout_log(&format!("[{action}] done target={target}"));
                    Ok(())
                }
                Err(err) if is_ignorable_add_index_error(&action, &err) => {
                    print_stdout_log(&format!(
                        "[{action}] skip target={target} reason=index-exists"
                    ));
                    Ok(())
                }
                Err(err) => Err(format!(
                    "[{action}] failed target={target} sql={sql:?} error={err}"
                )),
            }
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => errors.push(err),
            Err(err) => errors.push(format!("[sql] task join failed error={err}")),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn to_insert_args(args: &InsertCliArgs) -> InsertArgs {
    InsertArgs {
        csv_file: args.csv_file.clone(),
        host: args.conn.host.clone(),
        port: args.conn.port,
        user: args.conn.user.clone(),
        password: args.conn.password.clone(),
        database: args.conn.database.clone(),
        table: args.table.clone(),
        count: args.count,
        table_offset: args.table_offset,
        batch_size: args.batch_size,
        row_limit: args.row_limit,
        freshness_batch: args.freshness_batch,
        print_interval: args.print_interval,
        conn_pool_size: args.conn.conn_pool_size,
        encoding: args.encoding.clone(),
        delimiter: args.delimiter.clone(),
        has_header: args.has_header,
        freshness: !args.no_freshness,
        dry_run: args.conn.dry_run,
    }
}

fn to_query_conn_args(args: &CommonConnArgs) -> QueryConnArgs {
    QueryConnArgs {
        host: args.host.clone(),
        port: args.port,
        user: args.user.clone(),
        password: args.password.clone(),
        database: args.database.clone(),
        mysql_bin: args.mysql_bin.clone(),
        conn_pool_size: args.conn_pool_size,
        dry_run: args.dry_run,
    }
}

fn to_query_table_args(args: &QueryArgs) -> QueryTableArgs {
    QueryTableArgs {
        conn: to_query_conn_args(&args.conn),
        table: args.table.clone(),
        count: args.count,
        table_offset: args.table_offset,
        sql: args.sql.clone(),
        tikv: args.tikv,
        query_loop_count: args.query_loop_count,
    }
}

fn to_check_table_args(args: &CheckArgs) -> CheckTableArgs {
    CheckTableArgs {
        conn: to_query_conn_args(&args.conn),
        table: args.table.clone(),
        count: args.count,
        table_offset: args.table_offset,
        sql: args.sql.clone(),
        query_loop_count: args.query_loop_count,
    }
}

async fn run_auto(args: &AutoArgs) -> i32 {
    let tables = match build_table_names(
        &args.common.table,
        args.common.count,
        args.common.table_offset,
    ) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            return 2;
        }
    };
    print_command_summary(
        "auto",
        &args.common.conn.database,
        &tables,
        args.common.conn.dry_run,
    );
    let pool = if args.common.conn.dry_run {
        None
    } else {
        match build_client(&args.common.conn).build_pool(args.common.conn.conn_pool_size) {
            Ok(pool) => Some(pool),
            Err(err) => {
                print_stderr_log(&err);
                return 1;
            }
        }
    };
    let create_sqls = match build_create_table_items(&args.common.conn.database, &tables) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            return 2;
        }
    };
    let index_sqls = match tables
        .iter()
        .map(|table_name| {
            Ok((
                "add-index".to_string(),
                format_table_target(&args.common.conn.database, table_name),
                add_index_sql(
                    &args.common.conn.database,
                    table_name,
                    &args.index_name,
                    &args.column,
                )?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()
    {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            return 2;
        }
    };
    if let Err(err) = run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &create_sqls).await {
        print_stderr_log(&err);
        return 1;
    }
    if let Err(err) = run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &index_sqls).await {
        print_stderr_log(&err);
        return 1;
    }
    0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::CreateTable(args) => {
            let tables = match build_table_names(&args.table, args.count, args.table_offset) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "create-table",
                &args.conn.database,
                &tables,
                args.conn.dry_run,
            );
            let sqls = build_create_table_items(&args.conn.database, &tables);
            match sqls {
                Ok(items) => match build_optional_pool(
                    &build_client(&args.conn),
                    args.conn.conn_pool_size,
                    args.conn.dry_run,
                ) {
                    Ok(pool) => {
                        match run_sqls(pool.as_ref(), args.conn.dry_run, true, &items).await {
                            Ok(_) => 0,
                            Err(err) => {
                                print_stderr_log(&err);
                                1
                            }
                        }
                    }
                    Err(err) => {
                        print_stderr_log(&err);
                        1
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    1
                }
            }
        }
        Commands::DropTable(args) => {
            let tables = match build_table_names(&args.table, args.count, args.table_offset) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "drop-table",
                &args.conn.database,
                &tables,
                args.conn.dry_run,
            );
            let sqls = build_drop_table_items(&args.conn.database, &tables);
            match sqls {
                Ok(items) => match build_optional_pool(
                    &build_client(&args.conn),
                    args.conn.conn_pool_size,
                    args.conn.dry_run,
                ) {
                    Ok(pool) => {
                        match run_sqls(pool.as_ref(), args.conn.dry_run, true, &items).await {
                            Ok(_) => 0,
                            Err(err) => {
                                print_stderr_log(&err);
                                1
                            }
                        }
                    }
                    Err(err) => {
                        print_stderr_log(&err);
                        1
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    1
                }
            }
        }
        Commands::AddIndex(args) => {
            let tables = match build_table_names(
                &args.common.table,
                args.common.count,
                args.common.table_offset,
            ) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "add-index",
                &args.common.conn.database,
                &tables,
                args.common.conn.dry_run,
            );
            let sqls = tables
                .iter()
                .map(|table_name| {
                    Ok((
                        "add-index".to_string(),
                        format_table_target(&args.common.conn.database, table_name),
                        add_index_sql(
                            &args.common.conn.database,
                            table_name,
                            &args.index_name,
                            &args.column,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>();
            match sqls {
                Ok(items) => match build_optional_pool(
                    &build_client(&args.common.conn),
                    args.common.conn.conn_pool_size,
                    args.common.conn.dry_run,
                ) {
                    Ok(pool) => {
                        match run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &items).await
                        {
                            Ok(_) => 0,
                            Err(err) => {
                                print_stderr_log(&err);
                                1
                            }
                        }
                    }
                    Err(err) => {
                        print_stderr_log(&err);
                        1
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    1
                }
            }
        }
        Commands::DropIndex(args) => {
            let tables = match build_table_names(
                &args.common.table,
                args.common.count,
                args.common.table_offset,
            ) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "drop-index",
                &args.common.conn.database,
                &tables,
                args.common.conn.dry_run,
            );
            let sqls = tables
                .iter()
                .map(|table_name| {
                    Ok((
                        "drop-index".to_string(),
                        format_table_target(&args.common.conn.database, table_name),
                        drop_index_sql(&args.common.conn.database, table_name, &args.index_name)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>();
            match sqls {
                Ok(items) => match build_optional_pool(
                    &build_client(&args.common.conn),
                    args.common.conn.conn_pool_size,
                    args.common.conn.dry_run,
                ) {
                    Ok(pool) => {
                        match run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &items).await
                        {
                            Ok(_) => 0,
                            Err(err) => {
                                print_stderr_log(&err);
                                1
                            }
                        }
                    }
                    Err(err) => {
                        print_stderr_log(&err);
                        1
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    1
                }
            }
        }
        Commands::ImportInto(args) => {
            let sql = match import_into_sql(&args) {
                Ok(value) => value,
                Err(err) => {
                    print_stderr_log(&err);
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "import-into",
                &args.conn.database,
                std::slice::from_ref(&args.table),
                args.conn.dry_run,
            );
            let items = vec![(
                "import-into".to_string(),
                format_table_target(&args.conn.database, &args.table),
                sql,
            )];
            match build_optional_pool(
                &build_client(&args.conn),
                args.conn.conn_pool_size,
                args.conn.dry_run,
            ) {
                Ok(pool) => match run_sqls(pool.as_ref(), args.conn.dry_run, false, &items).await {
                    Ok(_) => 0,
                    Err(err) => {
                        print_stderr_log(&err);
                        1
                    }
                },
                Err(err) => {
                    print_stderr_log(&err);
                    1
                }
            }
        }
        Commands::Query(args) => run_query(&to_query_table_args(&args)).await,
        Commands::Check(args) => run_check(&to_check_table_args(&args)).await,
        Commands::InsertData(args) => run_insert_data(&to_insert_args(&args)).await,
        Commands::Auto(args) => run_auto(&args).await,
    };
    std::process::exit(exit_code);
}
