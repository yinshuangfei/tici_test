use std::path::{Path, PathBuf};

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

pub fn format_table_targets_summary(database: &str, table_names: &[String]) -> String {
    match table_names {
        [] => String::new(),
        [table_name] => format_table_target(database, table_name),
        _ => {
            let first = format_table_target(database, &table_names[0]);
            let last = format_table_target(database, &table_names[table_names.len() - 1]);
            format!("{first}..{last}")
        }
    }
}

pub fn normalize_delimiter(value: &str) -> String {
    match value {
        r"\t" => "\t".to_string(),
        r"\n" => "\n".to_string(),
        r"\r" => "\r".to_string(),
        other => other.to_string(),
    }
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
