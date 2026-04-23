use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts, Row, Value, prelude::Queryable};
use std::process::{Command, Stdio};

#[derive(Clone, Debug)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub mysql_bin: String,
    pub comments: bool,
}

impl MysqlConfig {
    pub fn opts_builder(&self) -> OptsBuilder {
        OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(if self.password.is_empty() {
                None
            } else {
                Some(self.password.clone())
            })
            .db_name(Some(self.database.clone()))
    }

    pub fn cli_command(&self) -> Vec<String> {
        let mut cmd = vec![
            self.mysql_bin.clone(),
            "--host".to_string(),
            self.host.clone(),
            "--port".to_string(),
            self.port.to_string(),
            "-u".to_string(),
            self.user.clone(),
        ];
        if self.comments {
            cmd.push("--comments".to_string());
        }
        if !self.password.is_empty() {
            cmd.push(format!("-p{}", self.password));
        }
        cmd
    }

    pub fn build_pool(&self, max_size: usize) -> Result<Pool, String> {
        let constraints = PoolConstraints::new(0, max_size)
            .ok_or_else(|| format!("invalid pool constraints: 0..={max_size}"))?;
        let builder = self
            .opts_builder()
            .pool_opts(PoolOpts::default().with_constraints(constraints));
        Ok(Pool::new(builder))
    }

    pub fn execute_cli_stdin(&self, sql: &str, echo: bool) -> Result<(), String> {
        let mut args = self.cli_command();
        args.push("-D".to_string());
        args.push(self.database.clone());
        if echo {
            println!("$ {}", shell_join(&args));
            println!("{sql}");
        }
        let mut child = Command::new(&args[0])
            .args(&args[1..])
            .stdin(Stdio::piped())
            .spawn()
            .map_err(|err| err.to_string())?;
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin
                .write_all(sql.as_bytes())
                .map_err(|err| err.to_string())?;
        }
        let status = child.wait().map_err(|err| err.to_string())?;
        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "command failed with exit code {}",
                status.code().unwrap_or(1)
            ))
        }
    }
}

pub fn build_optional_pool(
    config: &MysqlConfig,
    max_size: usize,
    dry_run: bool,
) -> Result<Option<Pool>, String> {
    if dry_run {
        Ok(None)
    } else {
        config.build_pool(max_size).map(Some)
    }
}

pub async fn execute_sql_with_pool(pool: &Pool, sql: &str) -> Result<(), String> {
    let mut conn = pool.get_conn().await.map_err(|err| err.to_string())?;
    conn.query_drop(sql).await.map_err(|err| err.to_string())
}

pub async fn query_tsv_with_pool(pool: &Pool, sql: &str) -> Result<String, String> {
    let mut conn = pool.get_conn().await.map_err(|err| err.to_string())?;
    let rows: Vec<Row> = conn.query(sql).await.map_err(|err| err.to_string())?;
    Ok(mysql_rows_to_strings(rows)
        .into_iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n"))
}

fn mysql_rows_to_strings(rows: Vec<Row>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.unwrap()
                .into_iter()
                .map(|value| match value {
                    Value::NULL => String::new(),
                    other => mysql_value_to_string(other),
                })
                .collect()
        })
        .collect()
}

fn mysql_value_to_string(value: Value) -> String {
    match value {
        Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{:06}",
            micros
        ),
        Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
            let sign = if is_neg { "-" } else { "" };
            format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{:06}",
                micros
            )
        }
        Value::NULL => String::new(),
    }
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| shell_escape(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_escape(value: &str) -> String {
    if value
        .bytes()
        .all(|ch| matches!(ch, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'/' | b'.' | b'_' | b'-' | b'=' | b':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}
