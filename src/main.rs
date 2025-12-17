use clap::{Parser, command};
use kroxy::{config::Config, grpc};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short, long, default_value = "debug")]
    log_level: String,
    #[arg(long, default_value = "9090")]
    prometheus_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");
    let args = Args::parse();
    let config = Config::new(&args.config)?;

    Ok(())
}
