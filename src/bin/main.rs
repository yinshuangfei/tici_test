use clap::{Parser, Subcommand};
use mysql::Pool;
use rayon::prelude::*;
use tici_test_rust::common::{
    MysqlConfig, build_table_names, execute_sql_with_pool, format_table_target,
    query_tsv_with_pool, quote_identifier, quote_literal,
};
use tici_test_rust::insert_logic::{
    DEFAULT_BATCH_SIZE, DEFAULT_CONN_POOL_SIZE, DEFAULT_CSV_FILE, DEFAULT_DATABASE, DEFAULT_HOST,
    DEFAULT_PORT, DEFAULT_PROGRESS_INTERVAL, DEFAULT_ROW_LIMIT, DEFAULT_TABLE, DEFAULT_USER,
    InsertArgs, run_insert_data,
};

const DEFAULT_INDEX: &str = "ft_idx";
const DEFAULT_IMPORT_SOURCE: &str = "s3://data/import-src/hdfs-logs-multitenants.csv?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true";
const DEFAULT_IMPORT_SORT_DIR: &str = "s3://ticidefaultbucket/tici/import-sort?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://10.2.12.81:19008&force-path-style=true";
const DEFAULT_IMPORT_THREAD: usize = 8;
const DEFAULT_AUTO_ROW_LIMIT: usize = 100000;
const DEFAULT_QUERY_SQL: &str = "select '1' as table_idx, count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);";

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

fn build_pool(conn: &CommonConnArgs) -> Result<Pool, String> {
    build_client(conn).build_pool(conn.conn_pool_size)
}

fn build_optional_pool(conn: &CommonConnArgs) -> Result<Option<Pool>, String> {
    if conn.dry_run {
        Ok(None)
    } else {
        build_pool(conn).map(Some)
    }
}

fn print_command_summary(command: &str, database: &str, table_names: &[String], dry_run: bool) {
    let joined = table_names
        .iter()
        .map(|name| format_table_target(database, name))
        .collect::<Vec<_>>()
        .join(", ");
    let mode = if dry_run { "dry-run" } else { "execute" };
    println!("[{command}] mode={mode} tables={joined}");
}

fn print_query_summary(command: &str, dry_run: bool) {
    let mode = if dry_run { "dry-run" } else { "execute" };
    println!("[{command}] mode={mode}");
}

fn create_table_sql(database: &str, table_name: &str) -> Result<String, String> {
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n    id BIGINT AUTO_INCREMENT,\n    timestamp BIGINT,\n    severity_text VARCHAR(50),\n    body TEXT,\n    tenant_id INT,\n    PRIMARY KEY (tenant_id, id)\n);",
        quote_identifier(database)?,
        quote_identifier(table_name)?
    ))
}

fn drop_table_sql(database: &str, table_name: &str) -> Result<String, String> {
    Ok(format!(
        "DROP TABLE IF EXISTS {}.{};",
        quote_identifier(database)?,
        quote_identifier(table_name)?
    ))
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

fn build_query_sql(
    database: &str,
    table_name: &str,
    table_idx: usize,
    sql: &str,
    tikv: bool,
) -> String {
    let target = format_table_target(database, table_name);
    if tikv {
        return format!("select '{table_idx}' as table_idx,count(*) from {table_name};");
    }
    if sql.contains("{table}") {
        return sql.replace("{table}", &target);
    }
    if sql == DEFAULT_QUERY_SQL {
        return sql
            .replacen(
                "select '1' as table_idx",
                &format!("select '{table_idx}' as table_idx"),
                1,
            )
            .replace("test.hdfs_log", &target);
    }
    sql.to_string()
}

fn normalize_query_output(output: &str) -> String {
    output
        .trim()
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_sqls(
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
            println!("[{action}] target={target}");
            if dry_run {
                println!("{sql}");
            } else {
                let pool = pool.ok_or_else(|| "missing connection pool".to_string())?;
                match execute_sql_with_pool(pool, sql) {
                    Ok(()) => {}
                    Err(err) if is_ignorable_add_index_error(action, &err) => {
                        println!("[{action}] skip target={target} reason=index-exists");
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
    println!("[sql] threads={}", items.len());
    let results = items
        .par_iter()
        .map(|(action, target, sql)| {
            println!("[{action}] target={target}");
            let pool = match pool {
                Some(pool) => pool,
                None => return Err("missing connection pool".to_string()),
            };
            match execute_sql_with_pool(pool, sql) {
                Ok(()) => {
                    println!("[{action}] done target={target}");
                    Ok(())
                }
                Err(err) if is_ignorable_add_index_error(action, &err) => {
                    println!("[{action}] skip target={target} reason=index-exists");
                    Ok(())
                }
                Err(err) => Err(format!(
                    "[{action}] failed target={target} sql={sql:?} error={err}"
                )),
            }
        })
        .collect::<Vec<_>>();

    let errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn run_query(args: &QueryArgs) -> i32 {
    if args.query_loop_count < 1 {
        eprintln!("query loop count must be >= 1");
        return 2;
    }
    if args.count < 1 {
        eprintln!("query table count must be >= 1");
        return 2;
    }
    print_query_summary("query", args.conn.dry_run);
    let tables = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    print_command_summary("query", &args.conn.database, &tables, args.conn.dry_run);
    let pool = if args.conn.dry_run {
        None
    } else {
        match build_pool(&args.conn) {
            Ok(pool) => Some(pool),
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    };
    let mut statements = Vec::new();
    for round in 1..=args.query_loop_count {
        println!("[query] round={round}/{}", args.query_loop_count);
        for (table_idx, table_name) in tables.iter().enumerate() {
            statements.push((
                "query".to_string(),
                format_table_target(&args.conn.database, table_name),
                build_query_sql(
                    &args.conn.database,
                    table_name,
                    table_idx + 1,
                    &args.sql,
                    args.tikv,
                ),
            ));
        }
    }
    if args.conn.dry_run || args.query_loop_count == 1 || statements.len() <= 1 {
        for (_, target, sql) in &statements {
            if args.conn.dry_run {
                println!("{sql}");
            } else if let Some(pool) = pool.as_ref() {
                match query_tsv_with_pool(pool, sql) {
                    Ok(output) => {
                        if !output.is_empty() {
                            println!("[query] target={target}, {output}");
                        }
                    }
                    Err(err) => {
                        eprintln!("[query] failed target={target} sql={sql:?} error={err}");
                        return 1;
                    }
                }
            }
        }
        return 0;
    }
    println!("[query] threads={}", statements.len());
    let results = statements
        .par_iter()
        .map(|(_, target, sql)| {
            (
                target.clone(),
                sql.clone(),
                query_tsv_with_pool(pool.as_ref().expect("pool must exist"), sql),
            )
        })
        .collect::<Vec<_>>();
    for (target, sql, output) in results {
        match output {
            Ok(value) => {
                if !value.is_empty() {
                    println!("{value}");
                }
                println!("[query] done target={target}");
            }
            Err(err) => {
                eprintln!("[query] failed target={target} sql={sql:?} error={err}");
                return 1;
            }
        }
    }
    0
}

fn run_check(args: &CheckArgs) -> i32 {
    if args.query_loop_count < 1 {
        eprintln!("check loop count must be >= 1");
        return 2;
    }
    let tables = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    print_query_summary("check", args.conn.dry_run);
    print_command_summary("check", &args.conn.database, &tables, args.conn.dry_run);
    let pool = if args.conn.dry_run {
        None
    } else {
        match build_pool(&args.conn) {
            Ok(pool) => Some(pool),
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    };
    for round in 1..=args.query_loop_count {
        let round_detail = format!("round={round}/{}", args.query_loop_count);
        println!("[check] {round_detail}");
        for (table_idx, table_name) in tables.iter().enumerate() {
            let normal_sql = build_query_sql(
                &args.conn.database,
                table_name,
                table_idx + 1,
                &args.sql,
                false,
            );
            let tikv_sql = build_query_sql(
                &args.conn.database,
                table_name,
                table_idx + 1,
                &args.sql,
                true,
            );
            if args.conn.dry_run {
                println!("[check] variant=normal");
                println!("{normal_sql}");
                println!("[check] variant=tikv");
                println!("{tikv_sql}");
                continue;
            }
            let normal_output =
                match query_tsv_with_pool(pool.as_ref().expect("pool must exist"), &normal_sql) {
                    Ok(value) => normalize_query_output(&value),
                    Err(err) => {
                        eprintln!(
                            "[check] failed target={} variant=normal sql={:?} error={}",
                            format_table_target(&args.conn.database, table_name),
                            normal_sql,
                            err
                        );
                        return 1;
                    }
                };
            let tikv_output =
                match query_tsv_with_pool(pool.as_ref().expect("pool must exist"), &tikv_sql) {
                    Ok(value) => normalize_query_output(&value),
                    Err(err) => {
                        eprintln!(
                            "[check] failed target={} variant=tikv sql={:?} error={}",
                            format_table_target(&args.conn.database, table_name),
                            tikv_sql,
                            err
                        );
                        return 1;
                    }
                };
            if normal_output != tikv_output {
                let target = format_table_target(&args.conn.database, table_name);
                eprintln!("[check] mismatch target={target} {round_detail}");
                eprintln!("[check] normal result:");
                eprintln!(
                    "tici: {}",
                    if normal_output.is_empty() {
                        "<empty>"
                    } else {
                        &normal_output
                    }
                );
                eprintln!("[check] tikv result:");
                eprintln!(
                    "tikv: {}",
                    if tikv_output.is_empty() {
                        "<empty>"
                    } else {
                        &tikv_output
                    }
                );
                return 1;
            }
            println!(
                "[check] match target={} {}",
                format_table_target(&args.conn.database, table_name),
                normal_output
            );
        }
    }
    0
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

fn run_auto(args: &AutoArgs) -> i32 {
    let tables = match build_table_names(
        &args.common.table,
        args.common.count,
        args.common.table_offset,
    ) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
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
        match build_pool(&args.common.conn) {
            Ok(pool) => Some(pool),
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    };
    let create_sqls = match tables
        .iter()
        .map(|table_name| {
            Ok((
                "create-table".to_string(),
                format_table_target(&args.common.conn.database, table_name),
                create_table_sql(&args.common.conn.database, table_name)?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()
    {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
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
            eprintln!("{err}");
            return 2;
        }
    };
    if let Err(err) = run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &create_sqls) {
        eprintln!("{err}");
        return 1;
    }
    if let Err(err) = run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &index_sqls) {
        eprintln!("{err}");
        return 1;
    }
    0
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::CreateTable(args) => {
            let tables = match build_table_names(&args.table, args.count, args.table_offset) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "create-table",
                &args.conn.database,
                &tables,
                args.conn.dry_run,
            );
            let sqls = tables
                .iter()
                .map(|table_name| {
                    Ok((
                        "create-table".to_string(),
                        format_table_target(&args.conn.database, table_name),
                        create_table_sql(&args.conn.database, table_name)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>();
            match sqls.and_then(|items| {
                let pool = build_optional_pool(&args.conn)?;
                run_sqls(pool.as_ref(), args.conn.dry_run, true, &items)
            }) {
                Ok(_) => 0,
                Err(err) => {
                    eprintln!("{err}");
                    1
                }
            }
        }
        Commands::DropTable(args) => {
            let tables = match build_table_names(&args.table, args.count, args.table_offset) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(2);
                }
            };
            print_command_summary(
                "drop-table",
                &args.conn.database,
                &tables,
                args.conn.dry_run,
            );
            let sqls = tables
                .iter()
                .map(|table_name| {
                    Ok((
                        "drop-table".to_string(),
                        format_table_target(&args.conn.database, table_name),
                        drop_table_sql(&args.conn.database, table_name)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>();
            match sqls.and_then(|items| {
                let pool = build_optional_pool(&args.conn)?;
                run_sqls(pool.as_ref(), args.conn.dry_run, true, &items)
            }) {
                Ok(_) => 0,
                Err(err) => {
                    eprintln!("{err}");
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
                    eprintln!("{err}");
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
            match sqls.and_then(|items| {
                let pool = build_optional_pool(&args.common.conn)?;
                run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &items)
            }) {
                Ok(_) => 0,
                Err(err) => {
                    eprintln!("{err}");
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
                    eprintln!("{err}");
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
            match sqls.and_then(|items| {
                let pool = build_optional_pool(&args.common.conn)?;
                run_sqls(pool.as_ref(), args.common.conn.dry_run, true, &items)
            }) {
                Ok(_) => 0,
                Err(err) => {
                    eprintln!("{err}");
                    1
                }
            }
        }
        Commands::ImportInto(args) => {
            let sql = match import_into_sql(&args) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("{err}");
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
            match build_optional_pool(&args.conn)
                .and_then(|pool| run_sqls(pool.as_ref(), args.conn.dry_run, false, &items))
            {
                Ok(_) => 0,
                Err(err) => {
                    eprintln!("{err}");
                    1
                }
            }
        }
        Commands::Query(args) => run_query(&args),
        Commands::Check(args) => run_check(&args),
        Commands::InsertData(args) => run_insert_data(&to_insert_args(&args)),
        Commands::Auto(args) => run_auto(&args),
    };
    std::process::exit(exit_code);
}
