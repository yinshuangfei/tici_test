use clap::Parser;
use flate2::read::GzDecoder;
use reqwest::blocking::Client;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tici_test_rust::common::{format_size, print_stderr_log, resolve_project_path};

const DEFAULT_OUTPUT_DIR: &str = "data";
const DEFAULT_TIMEOUT: f64 = 30.0;
const DEFAULT_PROGRESS_INTERVAL: f64 = 3.0;
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_URL: &str =
    "https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz";
const DEFAULT_CONVERT_BATCH_SIZE: usize = 50000;
const CSV_FIELDNAMES: [&str; 4] = ["timestamp", "severity_text", "body", "tenant_id"];

#[derive(Parser, Debug)]
#[command(about = "Download source files for data preparation")]
struct Cli {
    #[arg(default_value = DEFAULT_URL)]
    url: String,
    #[arg(long, default_value_t = false)]
    download: bool,
    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: String,
    #[arg(long, default_value = "")]
    output_file: String,
    #[arg(long, default_value_t = false)]
    overwrite: bool,
    #[arg(long, default_value_t = DEFAULT_TIMEOUT)]
    timeout: f64,
    #[arg(long, default_value_t = DEFAULT_PROGRESS_INTERVAL)]
    progress_interval: f64,
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,
    #[arg(long, default_value = "")]
    input_file: String,
    #[arg(long, default_value = "")]
    convert_out: String,
    #[arg(long, default_value_t = 0)]
    max_rows: usize,
    #[arg(long, default_value_t = DEFAULT_CONVERT_BATCH_SIZE)]
    convert_batch_size: usize,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn infer_filename(url: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url).map_err(|err| err.to_string())?;
    let name = parsed
        .path_segments()
        .and_then(|segments| segments.last())
        .unwrap_or_default();
    if name.is_empty() {
        return Err("cannot infer output filename from url; please use --output-file".to_string());
    }
    Ok(name.to_string())
}

fn build_output_file(cli: &Cli) -> Result<PathBuf, String> {
    let filename = if cli.output_file.is_empty() {
        infer_filename(&cli.url)?
    } else {
        cli.output_file.clone()
    };
    Ok(resolve_project_path(&cli.output_dir).join(filename))
}

fn print_plan(cli: &Cli, output_file: &Path) {
    println!("url: {}", cli.url);
    println!("output: {}", output_file.display());
    println!("timeout: {:.1}s", cli.timeout);
    println!("chunk_size: {}", cli.chunk_size);
    println!("overwrite: {}", cli.overwrite);
}

fn print_convert_plan(cli: &Cli, input_file: &Path) {
    println!("convert_input: {}", input_file.display());
    println!("convert_output: {}", cli.convert_out);
    println!("convert_max_rows: {}", cli.max_rows);
    println!("convert_batch_size: {}", cli.convert_batch_size);
    println!("convert_overwrite: {}", cli.overwrite);
}

fn download_file(cli: &Cli, output_file: &Path) -> Result<(), String> {
    if let Some(parent) = output_file.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    if output_file.exists() && !cli.overwrite {
        return Err(format!(
            "output file already exists: {}",
            output_file.display()
        ));
    }
    let tmp_file = PathBuf::from(format!("{}.part", output_file.display()));
    if tmp_file.exists() {
        std::fs::remove_file(&tmp_file).map_err(|err| err.to_string())?;
    }
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs_f64(cli.timeout))
        .build()
        .map_err(|err| err.to_string())?;
    let started_at = Instant::now();
    let mut next_progress_at = cli.progress_interval;
    let mut bytes_written = 0u64;
    let mut response = client
        .get(&cli.url)
        .header("User-Agent", "tici-test-prepare-data/1.0")
        .send()
        .map_err(|err| err.to_string())?;
    let total_size = response.content_length().unwrap_or(0);
    let mut handle = File::create(&tmp_file).map_err(|err| err.to_string())?;
    let mut buffer = vec![0u8; cli.chunk_size];
    loop {
        let read_size = response.read(&mut buffer).map_err(|err| err.to_string())?;
        if read_size == 0 {
            break;
        }
        handle
            .write_all(&buffer[..read_size])
            .map_err(|err| err.to_string())?;
        bytes_written += read_size as u64;
        let elapsed = started_at.elapsed().as_secs_f64();
        if cli.progress_interval == 0.0 || elapsed >= next_progress_at {
            if total_size > 0 {
                print!(
                    "\rprogress downloaded {} / {} in {:.1}s",
                    format_size(bytes_written),
                    format_size(total_size),
                    elapsed
                );
            } else {
                print!(
                    "\rprogress downloaded {} in {:.1}s",
                    format_size(bytes_written),
                    elapsed
                );
            }
            let _ = std::io::stdout().flush();
            if cli.progress_interval > 0.0 {
                next_progress_at = elapsed + cli.progress_interval;
            }
        }
    }
    if bytes_written > 0 {
        println!();
    }
    std::fs::rename(&tmp_file, output_file).map_err(|err| err.to_string())?;
    println!(
        "completed download {} in {:.1}s",
        format_size(bytes_written),
        started_at.elapsed().as_secs_f64()
    );
    Ok(())
}

fn open_reader(path: &Path) -> Result<Box<dyn BufRead>, String> {
    let file = File::open(path).map_err(|err| err.to_string())?;
    if path.extension().and_then(|item| item.to_str()) == Some("gz") {
        return Ok(Box::new(BufReader::new(GzDecoder::new(file))));
    }
    Ok(Box::new(BufReader::new(file)))
}

fn convert_to_csv(cli: &Cli, input_file: &Path) -> Result<(), String> {
    if !input_file.is_file() {
        return Err(format!(
            "convert input file not found: {}",
            input_file.display()
        ));
    }
    let output_file = resolve_project_path(&cli.convert_out);
    if output_file.exists() && !cli.overwrite {
        return Err(format!(
            "convert output file already exists: {}",
            output_file.display()
        ));
    }
    if let Some(parent) = output_file.parent() {
        std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let tmp_file = PathBuf::from(format!("{}.part", output_file.display()));
    if tmp_file.exists() {
        std::fs::remove_file(&tmp_file).map_err(|err| err.to_string())?;
    }
    let started_at = Instant::now();
    let mut last_progress_at = 0.0f64;
    let mut total_rows = 0usize;
    let reader = open_reader(input_file)?;
    let mut writer = csv::Writer::from_path(&tmp_file).map_err(|err| err.to_string())?;
    writer
        .write_record(CSV_FIELDNAMES)
        .map_err(|err| err.to_string())?;
    for (index, line) in reader.lines().enumerate() {
        if cli.max_rows > 0 && total_rows >= cli.max_rows {
            break;
        }
        let raw = line.map_err(|err| err.to_string())?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)
            .map_err(|err| format!("invalid json at line {}: {}", index + 1, err))?;
        let record = [
            value
                .get("timestamp")
                .map(|v| v.to_string())
                .unwrap_or_default(),
            value
                .get("severity_text")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            value
                .get("body")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            value
                .get("tenant_id")
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ];
        writer.write_record(record).map_err(|err| err.to_string())?;
        total_rows += 1;
        let elapsed = started_at.elapsed().as_secs_f64();
        if cli.progress_interval == 0.0 || elapsed - last_progress_at >= cli.progress_interval {
            println!("progress converted {total_rows} rows in {elapsed:.1}s");
            last_progress_at = elapsed;
        }
    }
    writer.flush().map_err(|err| err.to_string())?;
    std::fs::rename(&tmp_file, output_file).map_err(|err| err.to_string())?;
    println!(
        "completed convert {total_rows} rows in {:.1}s",
        started_at.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    if cli.timeout <= 0.0 {
        print_stderr_log("timeout must be > 0");
        std::process::exit(2);
    }
    if cli.progress_interval < 0.0 {
        print_stderr_log("progress interval must be >= 0");
        std::process::exit(2);
    }
    if cli.chunk_size < 1 {
        print_stderr_log("chunk size must be >= 1");
        std::process::exit(2);
    }
    if cli.convert_batch_size < 1 {
        print_stderr_log("convert batch size must be >= 1");
        std::process::exit(2);
    }
    if !cli.download && cli.convert_out.is_empty() {
        print_stderr_log("at least one action is required: use --download and/or --convert-out");
        std::process::exit(2);
    }
    let output_file = match build_output_file(&cli) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            std::process::exit(2);
        }
    };
    let input_file = if cli.input_file.is_empty() {
        output_file.clone()
    } else {
        resolve_project_path(&cli.input_file)
    };
    if cli.dry_run {
        if cli.download {
            print_plan(&cli, &output_file);
        }
        if !cli.convert_out.is_empty() {
            print_convert_plan(&cli, &input_file);
        }
        return;
    }
    if cli.download {
        if let Err(err) = download_file(&cli, &output_file) {
            print_stderr_log(&format!("download failed: {err}"));
            std::process::exit(2);
        }
    }
    if !cli.convert_out.is_empty() {
        if let Err(err) = convert_to_csv(&cli, &input_file) {
            print_stderr_log(&format!("download failed: {err}"));
            std::process::exit(2);
        }
    }
}
