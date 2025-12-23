use std::pin::Pin;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::error;

use crate::{
    config::Config,
    grpc::{self, ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse},
    producer::Producer,
};

pub struct Server {
    cfg: Config,
    producer: Producer,
}

impl Server {
    pub fn new(cfg: &Config) -> Self {
        Self {
            cfg: cfg.clone(),
            producer: Producer::new(cfg),
        }
    }
}

// type ConsumeResponseStream = Pin<Box<dyn Stream<Item = Result<ConsumeResponse, Status>> + Send>>;
#[tonic::async_trait]
impl grpc::kafka_proxy_server::KafkaProxy for Server {
    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ConsumeResponse, Status>> + Send>>;

    async fn produce(
        &self,
        req: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let req = req.into_inner();
        match self
            .producer
            .produce(&req.topic, &req.brokers, &req.messages)
            .await
        {
            Ok(_) => Ok(Response::from(ProduceResponse {})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
    async fn consume(
        &self,
        req: Request<Streaming<ConsumeRequest>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let mut in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            while let Some(v) = in_stream.next().await {
                match v {
                    Ok(req) => {
                        println!("topic {} ack {}", req.topic, req.ack_previous_message);
                    }
                    Err(e) => {
                        error!(error = format!("{:?}", e), "stream error");
                        // close consumer here
                        break;
                    }
                }
            }
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::ConsumeStream))
    }
}
