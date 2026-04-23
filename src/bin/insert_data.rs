use clap::Parser;
use tici_test_rust::insert_table::{
    DEFAULT_BATCH_SIZE, DEFAULT_CONN_POOL_SIZE, DEFAULT_CSV_FILE, DEFAULT_DATABASE,
    DEFAULT_FRESHNESS_POLL_INTERVAL, DEFAULT_FRESHNESS_TIMEOUT, DEFAULT_HOST, DEFAULT_PORT,
    DEFAULT_PROGRESS_INTERVAL, DEFAULT_ROW_LIMIT, DEFAULT_TABLE, DEFAULT_USER, InsertArgs,
    run_insert_data,
};

#[derive(Parser, Debug)]
#[command(about = "Insert CSV data into test.hdfs_log")]
struct Cli {
    #[arg(value_name = "csv_file", default_value = DEFAULT_CSV_FILE)]
    csv_file: String,
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
    #[arg(long, default_value_t = DEFAULT_CONN_POOL_SIZE)]
    conn_pool_size: usize,
    #[arg(long, default_value = "utf-8")]
    encoding: String,
    #[arg(long, default_value = ",")]
    delimiter: String,
    #[arg(long, default_value_t = false)]
    has_header: bool,
    #[arg(
        long = "no-freshness",
        default_value_t = false,
        help = "Disable the default post-insert freshness check"
    )]
    no_freshness: bool,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();
    let args = InsertArgs {
        csv_file: cli.csv_file,
        host: cli.host,
        port: cli.port,
        user: cli.user,
        password: cli.password,
        database: cli.database,
        table: cli.table,
        count: cli.count,
        table_offset: cli.table_offset,
        batch_size: cli.batch_size,
        row_limit: cli.row_limit,
        freshness_batch: cli.freshness_batch,
        print_interval: cli.print_interval,
        conn_pool_size: cli.conn_pool_size,
        encoding: cli.encoding,
        delimiter: cli.delimiter,
        has_header: cli.has_header,
        freshness: !cli.no_freshness,
        dry_run: cli.dry_run,
    };
    let _ = (DEFAULT_FRESHNESS_POLL_INTERVAL, DEFAULT_FRESHNESS_TIMEOUT);
    std::process::exit(run_insert_data(&args).await);
}
