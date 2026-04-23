use clap::Parser;
use tici_test_rust::common::{MysqlConfig, print_stderr_log, quote_identifier};

const DEFAULT_HOST: &str = "10.2.12.79";
const DEFAULT_PORT: u16 = 9528;
const DEFAULT_USER: &str = "root";
const DEFAULT_DATABASE: &str = "tici";
const DEFAULT_META_TABLE: &str = "tici_shard_meta";

#[derive(Parser, Debug)]
#[command(about = "Query TiCI shard metadata")]
struct Cli {
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
    #[arg(long, default_value = DEFAULT_META_TABLE)]
    meta_table: String,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

fn build_query_sql(meta_table: &str) -> Result<String, String> {
    let target = format!(
        "{}.{}",
        quote_identifier("tici")?,
        quote_identifier(meta_table)?
    );
    Ok(format!(
        "select table_id,index_id,shard_id,progress from {target};"
    ))
}

fn main() {
    let cli = Cli::parse();
    let sql = match build_query_sql(&cli.meta_table) {
        Ok(value) => value,
        Err(err) => {
            print_stderr_log(&err);
            std::process::exit(2);
        }
    };
    if cli.dry_run {
        println!("{sql}");
        return;
    }
    let client = MysqlConfig {
        host: cli.host,
        port: cli.port,
        user: cli.user,
        password: cli.password,
        database: cli.database,
        mysql_bin: cli.mysql_bin,
        comments: true,
    };
    if let Err(err) = client.execute_cli_stdin(&sql, false) {
        print_stderr_log(&err);
        std::process::exit(1);
    }
}
