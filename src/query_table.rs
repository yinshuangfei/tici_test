use crate::common::{build_table_names, format_table_target, format_table_targets_summary};
use crate::log::{print_stderr_log, print_stdout_log};
use crate::sql_pool::{MysqlConfig, query_tsv_with_pool};
use tokio::task::JoinSet;

pub const DEFAULT_QUERY_SQL: &str = "select '1' as table_idx, count(*) from test.hdfs_log where fts_match_word('china',body) or not fts_match_word('china',body);";

#[derive(Clone, Debug)]
pub struct QueryConnArgs {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub mysql_bin: String,
    pub conn_pool_size: usize,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct QueryTableArgs {
    pub conn: QueryConnArgs,
    pub table: String,
    pub count: usize,
    pub table_offset: usize,
    pub sql: String,
    pub tikv: bool,
    pub query_loop_count: usize,
}

#[derive(Clone, Debug)]
pub struct CheckTableArgs {
    pub conn: QueryConnArgs,
    pub table: String,
    pub count: usize,
    pub table_offset: usize,
    pub sql: String,
    pub query_loop_count: usize,
}

fn build_pool(conn: &QueryConnArgs) -> Result<mysql_async::Pool, String> {
    MysqlConfig {
        host: conn.host.clone(),
        port: conn.port,
        user: conn.user.clone(),
        password: conn.password.clone(),
        database: conn.database.clone(),
        mysql_bin: conn.mysql_bin.clone(),
        comments: true,
    }
    .build_pool(conn.conn_pool_size)
}

fn print_command_summary(command: &str, database: &str, table_names: &[String], dry_run: bool) {
    let joined = format_table_targets_summary(database, table_names);
    let mode = if dry_run { "dry-run" } else { "execute" };
    print_stdout_log(&format!("[{command}] mode={mode} tables={joined}"));
}

fn print_query_summary(command: &str, dry_run: bool) {
    let mode = if dry_run { "dry-run" } else { "execute" };
    print_stdout_log(&format!("[{command}] mode={mode}"));
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

pub async fn run_query(args: &QueryTableArgs) -> i32 {
    if args.query_loop_count < 1 {
        print_stderr_log("query loop count must be >= 1");
        return 2;
    }
    if args.count < 1 {
        print_stderr_log("query table count must be >= 1");
        return 2;
    }
    print_query_summary("query", args.conn.dry_run);
    let tables = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
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
                print_stderr_log(&err);
                return 1;
            }
        }
    };
    let mut statements = Vec::new();
    for round in 1..=args.query_loop_count {
        print_stdout_log(&format!("[query] round={round}/{}", args.query_loop_count));
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
    if args.conn.dry_run || statements.len() <= 1 {
        for (_, target, sql) in &statements {
            if args.conn.dry_run {
                println!("{sql}");
            } else if let Some(pool) = pool.as_ref() {
                match query_tsv_with_pool(pool, sql).await {
                    Ok(output) => {
                        if !output.is_empty() {
                            print_stdout_log(&format!("[query] target={target}, {output}"));
                        }
                    }
                    Err(err) => {
                        print_stderr_log(&format!(
                            "[query] failed target={target} sql={sql:?} error={err}"
                        ));
                        return 1;
                    }
                }
            }
        }
        return 0;
    }
    print_stdout_log(&format!("[query] tasks={}", statements.len()));
    let pool = pool.expect("pool must exist");
    let mut join_set = JoinSet::new();
    let statement_count = statements.len();
    for (index, (_, target, sql)) in statements.into_iter().enumerate() {
        let task_pool = pool.clone();
        join_set.spawn(async move {
            let output = query_tsv_with_pool(&task_pool, &sql).await;
            (index, target, sql, output)
        });
    }
    let mut results = vec![None; statement_count];
    while let Some(result) = join_set.join_next().await {
        let (index, target, sql, output) = match result {
            Ok(value) => value,
            Err(err) => {
                print_stderr_log(&format!("[query] task join failed error={err}"));
                return 1;
            }
        };
        results[index] = Some((target, sql, output));
    }
    for item in results {
        let (target, sql, output) = item.expect("query task result must exist");
        match output {
            Ok(value) => {
                if !value.is_empty() {
                    print_stdout_log(&format!("[query] target={target}, {value}"));
                }
            }
            Err(err) => {
                print_stderr_log(&format!(
                    "[query] failed target={target} sql={sql:?} error={err}"
                ));
                return 1;
            }
        }
    }
    0
}

pub async fn run_check(args: &CheckTableArgs) -> i32 {
    if args.query_loop_count < 1 {
        print_stderr_log("check loop count must be >= 1");
        return 2;
    }
    let tables = match build_table_names(&args.table, args.count, args.table_offset) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
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
                print_stderr_log(&err);
                return 1;
            }
        }
    };
    for round in 1..=args.query_loop_count {
        let round_detail = format!("round={round}/{}", args.query_loop_count);
        print_stdout_log(&format!("[check] {round_detail}"));
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
                print_stdout_log("[check] variant=normal");
                println!("{normal_sql}");
                print_stdout_log("[check] variant=tikv");
                println!("{tikv_sql}");
                continue;
            }
            let normal_output =
                match query_tsv_with_pool(pool.as_ref().expect("pool must exist"), &normal_sql)
                    .await
                {
                    Ok(value) => normalize_query_output(&value),
                    Err(err) => {
                        print_stderr_log(&format!(
                            "[check] failed target={} variant=normal sql={:?} error={}",
                            format_table_target(&args.conn.database, table_name),
                            normal_sql,
                            err
                        ));
                        return 1;
                    }
                };
            let tikv_output =
                match query_tsv_with_pool(pool.as_ref().expect("pool must exist"), &tikv_sql).await
                {
                    Ok(value) => normalize_query_output(&value),
                    Err(err) => {
                        print_stderr_log(&format!(
                            "[check] failed target={} variant=tikv sql={:?} error={}",
                            format_table_target(&args.conn.database, table_name),
                            tikv_sql,
                            err
                        ));
                        return 1;
                    }
                };
            if normal_output != tikv_output {
                let target = format_table_target(&args.conn.database, table_name);
                print_stderr_log(&format!("[check] mismatch target={target} {round_detail}"));
                print_stderr_log("[check] normal result:");
                print_stderr_log(&format!(
                    "tici: {}",
                    if normal_output.is_empty() {
                        "<empty>"
                    } else {
                        &normal_output
                    }
                ));
                print_stderr_log("[check] tikv result:");
                print_stderr_log(&format!(
                    "tikv: {}",
                    if tikv_output.is_empty() {
                        "<empty>"
                    } else {
                        &tikv_output
                    }
                ));
                return 1;
            }
            print_stdout_log(&format!(
                "[check] match target={} {}",
                format_table_target(&args.conn.database, table_name),
                normal_output
            ));
        }
    }
    0
}
