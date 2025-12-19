use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use rdkafka::{
    ClientConfig, ClientContext,
    config::FromClientConfigAndContext,
    error::KafkaError,
    message::DeliveryResult,
    producer::{BaseRecord, ProducerContext, ThreadedProducer},
};
use tokio::sync::mpsc;

use crate::{config::Config, grpc};

type KafkaResult = Result<(), (usize, KafkaError)>;

// DeliveryHandler implements rdkafka::producer::ProducerContext
// delivery() callback sends result of message producing to channel provided by delivery_opaque field in BaseRecord
struct DeliveryHandler {}

impl ClientContext for DeliveryHandler {}
impl ProducerContext for DeliveryHandler {
    type DeliveryOpaque = Box<(usize, mpsc::Sender<KafkaResult>)>;
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        let _ = match delivery_result {
            Ok(_) => delivery_opaque.1.blocking_send(Ok(())),
            Err(e) => delivery_opaque
                .1
                .blocking_send(Err((delivery_opaque.0, e.0.clone()))),
        };
    }
}

pub(crate) struct Producer {
    cfg: Config,
    connections: Mutex<HashMap<String, ThreadedProducer<DeliveryHandler>>>,
}

impl Producer {
    pub(crate) fn new(cfg: &Config) -> Self {
        Self {
            cfg: cfg.clone(),
            connections: Mutex::new(HashMap::new()), // TODO: implement idling connections cleanup
        }
    }

    pub(crate) async fn produce(
        &self,
        topic: &str,
        brokers: &[String],
        messages: &[grpc::Message],
    ) -> Result<()> {
        let brokers = if brokers.is_empty() {
            self.cfg
                .topics
                .get(topic)
                .context("topic not configured and no brokers specified")?
                .brokers
                .as_ref()
        } else {
            brokers
        };

        let conn = {
            let mut conns = self.connections.lock().expect("poisoned lock");
            match conns.get(topic) {
                Some(p) => p.clone(),
                None => {
                    let mut cfg = ClientConfig::new();
                    cfg.set("bootstrap.servers", brokers.join(","));
                    let producer =
                        ThreadedProducer::from_config_and_context(&cfg, DeliveryHandler {})
                            .context("connecting to kafka")?;
                    conns.insert(String::from(topic), producer.clone());
                    producer
                }
            }
        };

        let (sender, mut receiver) = mpsc::channel::<KafkaResult>(5);
        let expected_results = messages.len();
        let result_handler = tokio::spawn(async move {
            // collect delivery result for each message in batch
            let mut r: Result<()> = Ok(());
            for _ in 0..expected_results {
                if let Some(result) = receiver.recv().await {
                    match result {
                        Ok(_) => {}
                        Err(e) => r = Err(anyhow!("sending message {}: {}", e.0, e.1)),
                    }
                } else {
                    return Err(anyhow!("result channel closed?"));
                }
            }

            r
        });

        // send whole batch
        // NB: librdkafka does own buffering. This means that our batch may be split between multiple internal batches.
        // TODO: configure librdkafka buffering
        for (i, msg) in messages.iter().enumerate() {
            let record = BaseRecord {
                topic,
                partition: None,
                payload: Some(&msg.value),
                key: if msg.key.len() == 0 {
                    None
                } else {
                    Some(&msg.key)
                },
                timestamp: Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                ),
                headers: None, // TODO: add headers
                delivery_opaque: Box::new((i, sender.clone())),
            };

            if let Err(e) = conn.send(record) {
                let _ = sender.send(Err((i, e.0))).await;
            }
        }

        result_handler.await?.context("producing messages")
    }
}
