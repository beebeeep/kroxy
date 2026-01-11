use std::{
    collections::HashMap,
    pin::Pin,
    time::{Duration, SystemTime},
};

use anyhow::Result;
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, warn};

use crate::{
    config::Config,
    grpc::{self, ConsumeRequest, ConsumeResponse, Message, ProduceRequest, ProduceResponse},
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
            .produce(&req.topic, &req.brokers, &req.messages, &req.durability())
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
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let next_msg = ConsumeResponse {
                            message: Some(Message {
                                key: Vec::new(),
                                value: "CHLOS".into(),
                                timestamp: Some(Timestamp::from(SystemTime::now())),
                                headers: HashMap::new(),
                            }),
                            partition: 0,
                            offset: 0,
                        };
                        match tx.send(Ok(next_msg)).await {
                            Ok(_) => {
                                // ok
                            }
                            Err(e) => {
                                warn!("stream closed: {e:?}");
                                return;
                            }
                        };
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
