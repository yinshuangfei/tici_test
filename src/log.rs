use chrono::Local;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

fn file_log_lock() -> &'static Mutex<()> {
    static FILE_LOG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    FILE_LOG_LOCK.get_or_init(|| Mutex::new(()))
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

pub fn format_stdout_log(message: &str) -> String {
    format!("[{}] {}", timestamp_string(), message)
}

pub fn print_stdout_log(message: &str) {
    println!("{}", format_stdout_log(message));
}

pub fn format_stderr_log(message: &str) -> String {
    format!("[{}] {}", timestamp_string(), message)
}

pub fn print_stderr_log(message: &str) {
    eprintln!("{}", format_stderr_log(message));
}
