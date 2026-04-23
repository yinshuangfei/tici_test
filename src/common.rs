use chrono::Local;
use mysql::{OptsBuilder, Pool, PoolConstraints, PoolOpts, prelude::Queryable};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};

pub fn project_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub fn resolve_project_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        project_root().join(path)
    }
}

fn file_log_lock() -> &'static Mutex<()> {
    static FILE_LOG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    FILE_LOG_LOCK.get_or_init(|| Mutex::new(()))
}

pub fn quote_identifier(identifier: &str) -> Result<String, String> {
    if identifier.is_empty() {
        return Err("identifier must not be empty".to_string());
    }
    Ok(format!("`{}`", identifier.replace('`', "``")))
}

pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub fn build_table_names(
    base_name: &str,
    count: usize,
    table_offset: usize,
) -> Result<Vec<String>, String> {
    if count < 1 {
        return Err("table count must be >= 1".to_string());
    }
    if count == 1 && table_offset == 0 {
        return Ok(vec![base_name.to_string()]);
    }
    Ok(((table_offset + 1)..=(table_offset + count))
        .map(|idx| format!("{base_name}_{idx}"))
        .collect())
}

pub fn format_table_target(database: &str, table_name: &str) -> String {
    format!("{database}.{table_name}")
}

pub fn normalize_delimiter(value: &str) -> String {
    match value {
        r"\t" => "\t".to_string(),
        r"\n" => "\n".to_string(),
        r"\r" => "\r".to_string(),
        other => other.to_string(),
    }
}

pub fn with_timestamp_suffix(path: &Path, suffix: &str) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|item| item.to_str())
        .unwrap_or_default();
    let extension = path
        .extension()
        .and_then(|item| item.to_str())
        .unwrap_or_default();
    let file_name = if extension.is_empty() {
        format!("{stem}_{suffix}")
    } else {
        format!("{stem}_{suffix}.{extension}")
    };
    path.with_file_name(file_name)
}

pub fn now_log_suffix() -> String {
    Local::now().format("%Y%m%d_%H%M%S").to_string()
}

pub fn timestamp_string() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn append_progress_log(output_file: &Path, message: &str) -> Result<(), String> {
    let _guard = file_log_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(parent) = output_file.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let mut handle = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_file)
        .map_err(|err| err.to_string())?;
    use std::io::Write;
    writeln!(handle, "[{}] {}", timestamp_string(), message).map_err(|err| err.to_string())
}

pub fn format_size(num_bytes: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = num_bytes as f64;
    for unit in units {
        if value < 1024.0 || unit == "TiB" {
            return format!("{value:.1}{unit}");
        }
        value /= 1024.0;
    }
    format!("{num_bytes}B")
}

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
        OptsBuilder::new()
            .ip_or_hostname(Some(self.host.clone()))
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
        Pool::new(builder).map_err(|err| err.to_string())
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

pub fn execute_sql_with_pool(pool: &Pool, sql: &str) -> Result<(), String> {
    let mut conn = pool.get_conn().map_err(|err| err.to_string())?;
    conn.query_drop(sql).map_err(|err| err.to_string())
}

pub fn query_tsv_with_pool(pool: &Pool, sql: &str) -> Result<String, String> {
    let mut conn = pool.get_conn().map_err(|err| err.to_string())?;
    let rows: Vec<mysql::Row> = conn.query(sql).map_err(|err| err.to_string())?;
    Ok(mysql_rows_to_strings(rows)
        .into_iter()
        .map(|row| row.join("\t"))
        .collect::<Vec<_>>()
        .join("\n"))
}

fn mysql_rows_to_strings(rows: Vec<mysql::Row>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|row| {
            row.unwrap()
                .into_iter()
                .map(|value| match value {
                    mysql::Value::NULL => String::new(),
                    other => mysql_value_to_string(other),
                })
                .collect()
        })
        .collect()
}

fn mysql_value_to_string(value: mysql::Value) -> String {
    match value {
        mysql::Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        mysql::Value::Int(value) => value.to_string(),
        mysql::Value::UInt(value) => value.to_string(),
        mysql::Value::Float(value) => value.to_string(),
        mysql::Value::Double(value) => value.to_string(),
        mysql::Value::Date(year, month, day, hour, minute, second, micros) => format!(
            "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{:06}",
            micros
        ),
        mysql::Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
            let sign = if is_neg { "-" } else { "" };
            format!(
                "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{:06}",
                micros
            )
        }
        mysql::Value::NULL => String::new(),
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
