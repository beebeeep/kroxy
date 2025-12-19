use anyhow::{Context, Result};
use clap::{Parser, command};
use kroxy::{config::Config, grpc::kafka_proxy_server::KafkaProxyServer, server};
use tonic::transport::Server;

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
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::new(&args.config)?;
    let kroxy_server = server::Server::new(&config);
    let bind_addr = config.bind_addr.parse().context("parsing bind address")?;

    _ = tokio::spawn(async move {
        let srv = Server::builder().add_service(KafkaProxyServer::new(kroxy_server));
        srv.serve(bind_addr).await
    })
    .await?;

    Ok(())
}
