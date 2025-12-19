use std::pin::Pin;

use anyhow::Result;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

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
        _req: Request<Streaming<ConsumeRequest>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        unimplemented!()
    }
}
