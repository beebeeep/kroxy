use std::pin::Pin;

use anyhow::Result;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::{self, ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse};

pub(crate) struct Server {}

// type ConsumeResponseStream = Pin<Box<dyn Stream<Item = Result<ConsumeResponse, Status>> + Send>>;

#[tonic::async_trait]
impl grpc::kafka_proxy_server::KafkaProxy for Server {
    type ConsumeStream = Pin<Box<dyn Stream<Item = Result<ConsumeResponse, Status>> + Send>>;

    async fn produce(
        &self,
        _req: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        unimplemented!()
    }
    async fn consume(
        &self,
        _req: Request<Streaming<ConsumeRequest>>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        unimplemented!()
    }
}
