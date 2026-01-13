use std::fs::OpenOptions;

use anyhow::{Context, Result};
use clap::{Parser, command};
use kroxy::{config::Config, grpc::kafka_proxy_server::KafkaProxyServer, server};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    #[arg(short, long, default_value = "debug")]
    log_level: String,
    #[arg(short('f'), long, default_value = None)]
    log_file: Option<String>,
    #[arg(long, default_value = "9090")]
    prometheus_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let layer = tracing_subscriber::fmt::layer().json().flatten_event(true);
    let filter = EnvFilter::builder().parse(format!("{0},kroxy={0},h2=info", args.log_level))?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    if let Some(f) = args.log_file {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(f)
            .context("opening log file")?;
        let layer = tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_writer(file);
        tracing::subscriber::set_global_default(subscriber.with(layer))?;
    } else {
        tracing::subscriber::set_global_default(subscriber)?;
    }

    let config = Config::new(&args.config)?;
    let kroxy_server = server::Server::new(&config);
    let bind_addr = config.bind_addr.parse().context("parsing bind address")?;

    info!(bind_addr = config.bind_addr, "starting proxy");

    _ = tokio::spawn(async move {
        let srv = Server::builder().add_service(KafkaProxyServer::new(kroxy_server));
        srv.serve(bind_addr).await.expect("running server");
    })
    .await?;

    Ok(())
}
