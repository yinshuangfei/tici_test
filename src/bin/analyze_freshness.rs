use clap::Parser;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tici_test_rust::common::resolve_project_path;

#[derive(Parser, Debug)]
#[command(about = "Analyze freshness_result_*.log files and print grouped statistics by file.")]
struct Cli {
    #[arg(long, default_value = "log")]
    log_dir: String,
    #[arg(long, default_value = "freshness_result_*.log")]
    pattern: String,
    #[arg(long, default_value_t = false)]
    show_invalid: bool,
}

#[derive(Clone)]
struct FreshnessEntry {
    elapsed: f64,
    baseline_row_count: u64,
    imported_rows: u64,
    visible_rows: u64,
    total_rows: u64,
}

#[derive(Clone)]
struct InvalidLine {
    file_path: PathBuf,
    line_number: usize,
    line: String,
    reason: String,
}

#[derive(Default, Clone)]
struct GroupStats {
    elapsed_sum: f64,
    visible_sum: u64,
    lines: u64,
}

impl GroupStats {
    fn add(&mut self, entry: &FreshnessEntry) {
        self.elapsed_sum += entry.elapsed;
        self.visible_sum += entry.visible_rows;
        self.lines += 1;
    }
}

#[derive(Default, Clone)]
struct FileStats {
    grouped: BTreeMap<String, GroupStats>,
    invalid_lines: Vec<InvalidLine>,
}

fn parse_entry(line: &str) -> Option<FreshnessEntry> {
    let line = line.trim();
    let (_, tail) = line.split_once("] [")?;
    let (_, tail) = tail.split_once("] freshness reached elapsed=")?;
    let (elapsed, tail) = tail.split_once("s baseline_row_count=")?;
    let (baseline, tail) = tail.split_once(" imported_rows=")?;
    let (imported, tail) = tail.split_once(" visible_rows=")?;
    let (visible, total) = tail.split_once(" total_rows=")?;
    Some(FreshnessEntry {
        elapsed: elapsed.trim().parse().ok()?,
        baseline_row_count: baseline.trim().parse().ok()?,
        imported_rows: imported.trim().parse().ok()?,
        visible_rows: visible.trim().parse().ok()?,
        total_rows: total.trim().parse().ok()?,
    })
}

fn group_name(baseline_row_count: u64) -> String {
    format!("group-{}", (baseline_row_count / 10000) * 10000)
}

fn analyze_file(file_path: &Path) -> Result<FileStats, String> {
    let content = std::fs::read_to_string(file_path).map_err(|err| err.to_string())?;
    let mut stats = FileStats::default();
    for (idx, raw_line) in content.lines().enumerate() {
        let Some(entry) = parse_entry(raw_line) else {
            continue;
        };
        if entry.total_rows != entry.baseline_row_count + entry.imported_rows {
            stats.invalid_lines.push(InvalidLine {
                file_path: file_path.to_path_buf(),
                line_number: idx + 1,
                line: raw_line.to_string(),
                reason: format!(
                    "total_rows != baseline_row_count + imported_rows ({} != {} + {})",
                    entry.total_rows, entry.baseline_row_count, entry.imported_rows
                ),
            });
            continue;
        }
        stats
            .grouped
            .entry(group_name(entry.baseline_row_count))
            .or_default()
            .add(&entry);
    }
    Ok(stats)
}

fn print_grouped_stats(title: &str, grouped: &BTreeMap<String, GroupStats>) {
    println!("## {title}");
    println!("| 起始行数量级 | 平均插入行数 | 统计条目数 | 平均耗时(s) |");
    println!("| --- | ---: | ---: | ---: |");
    for (name, stats) in grouped {
        let avg_inserted = stats.visible_sum as f64 / stats.lines as f64;
        let avg_elapsed = stats.elapsed_sum / stats.lines as f64;
        println!(
            "| {name} | {avg_inserted:.2} | {} | {avg_elapsed:.2} |",
            stats.lines
        );
    }
    println!();
}

fn print_invalid_lines(invalid_lines: &[InvalidLine]) {
    println!("### invalid_details");
    println!("| 文件 | 行号 | 原因 | 原始内容 |");
    println!("| --- | ---: | --- | --- |");
    for item in invalid_lines {
        println!(
            "| {} | {} | {} | {} |",
            item.file_path.display(),
            item.line_number,
            item.reason.replace('|', r"\|"),
            item.line.replace('|', r"\|")
        );
    }
    println!();
}

fn main() {
    let cli = Cli::parse();
    let log_dir = resolve_project_path(&cli.log_dir);
    let pattern =
        glob::Pattern::new(&cli.pattern).unwrap_or_else(|_| glob::Pattern::new("*").unwrap());
    let mut file_paths = match std::fs::read_dir(&log_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| pattern.matches(name))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    file_paths.sort();
    if file_paths.is_empty() {
        println!(
            "no log files found in {} matching {}",
            log_dir.display(),
            cli.pattern
        );
        std::process::exit(1);
    }

    let mut merged = BTreeMap::<String, GroupStats>::new();
    for file_path in &file_paths {
        let stats = match analyze_file(file_path) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(2);
            }
        };
        print_grouped_stats(
            file_path
                .file_name()
                .and_then(|item| item.to_str())
                .unwrap_or_default(),
            &stats.grouped,
        );
        if cli.show_invalid && !stats.invalid_lines.is_empty() {
            print_invalid_lines(&stats.invalid_lines);
        }
        for (name, group) in stats.grouped {
            let target = merged.entry(name).or_default();
            target.elapsed_sum += group.elapsed_sum;
            target.visible_sum += group.visible_sum;
            target.lines += group.lines;
        }
    }
    print_grouped_stats("Summarize", &merged);
}
