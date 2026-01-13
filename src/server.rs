use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error};

use crate::{
    config::Config,
    consumer::{BatchedConsumer, ConsumersManager},
    grpc::{self, ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceResponse},
    producer::Producer,
};

pub struct Server {
    producer: Producer,
    consumers_manager: ConsumersManager,
}

impl Server {
    pub fn new(cfg: &Config) -> Self {
        Self {
            producer: Producer::new(cfg),
            consumers_manager: ConsumersManager::new(cfg),
        }
    }

    async fn consumer_stream_handler(
        mut req_stream: Streaming<ConsumeRequest>,
        response: mpsc::Sender<Result<ConsumeResponse, Status>>,
        consumers_manager: ConsumersManager,
    ) {
        let mut stream_consumer: Option<Arc<BatchedConsumer>> = None;
        let mut last_message_id: Option<usize> = None;

        while let Some(req) = req_stream.next().await {
            let req = match req {
                Err(e) => {
                    error!(error = format!("{}", e), "stream error");
                    if let (Some(id), Some(consumer)) = (last_message_id, stream_consumer) {
                        let _ = consumer.release_message(id).await;
                    }
                    return;
                }
                Ok(req) => req,
            };

            debug!(
                topic = req.topic,
                ack = req.ack_previous_message,
                "got streaming request"
            );

            if stream_consumer.is_none() {
                stream_consumer = match consumers_manager.get_consumer(
                    &req.topic,
                    &req.brokers,
                    &req.consumer_group,
                ) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        let _ = response
                            .send(Err(Status::internal(format!(
                                "failed to create kafka consumer: {e}"
                            ))))
                            .await;
                        return;
                    }
                };
            };
            let consumer = stream_consumer.as_ref().unwrap();

            if let Some(id) = last_message_id {
                if req.ack_previous_message {
                    if let Err(e) = consumer.ack_message(id).await {
                        let _ = response
                            .send(Err(Status::internal(format!("ACKing message: {e}"))))
                            .await;
                        return;
                    }
                } else {
                    if let Err(e) = consumer.release_message(id).await {
                        let _ = response
                            .send(Err(Status::internal(format!("NACKing message: {e}"))))
                            .await;
                        return;
                    }
                    // TODO: send to DLQ
                }
            }

            let msg = match consumer.claim_message().await {
                Ok(msg) => msg,
                Err(e) => {
                    let _ = response
                        .send(Err(Status::internal(format!("getting next message: {e}"))))
                        .await;
                    return;
                }
            };
            last_message_id = Some(msg.id);
            let _ = response
                .send(Ok(ConsumeResponse {
                    message: Some(msg.inner),
                    partition: msg.partition,
                    offset: msg.offset,
                }))
                .await;
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
        let in_stream = req.into_inner();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(Server::consumer_stream_handler(
            in_stream,
            tx,
            self.consumers_manager.clone(),
        ));

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::ConsumeStream))
    }
}
