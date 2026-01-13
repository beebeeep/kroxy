use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
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
use tracing::debug;

use crate::{config::Config, grpc, util::Tracker};

const PRODUCER_TTL: Duration = Duration::from_secs(10);
const CLEANUP_PERIOD: Duration = Duration::from_secs(5);

type KafkaResult = Result<(), (usize, KafkaError)>;

// DeliveryHandler implements rdkafka::producer::ProducerContext
// delivery() callback sends result of message producing to channel provided by delivery_opaque field in BaseRecord
struct DeliveryHandler;

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

// Producer wraps all the logic required for producing messages to Kafka.
// It automatically opens connections to configured/requested Kafkas; connections are closed after period of inactivity.
pub(crate) struct Producer {
    cfg: Config,
    producers: Arc<Mutex<HashMap<String, Tracker<ThreadedProducer<DeliveryHandler>>>>>,
}

impl Producer {
    pub(crate) fn new(cfg: &Config) -> Self {
        let producers = Arc::new(Mutex::new(HashMap::new()));
        let producer = Self {
            cfg: cfg.clone(),
            producers: producers.clone(),
        };

        tokio::spawn(async move {
            // task for cleaning up idling producers
            let mut to_remove = Vec::new();
            loop {
                {
                    let mut conns = producers.lock().expect("poisoned lock");
                    for (k, v) in conns.iter() {
                        if !v.is_used() && v.is_expired() {
                            to_remove.push(k.clone());
                        }
                    }
                    for k in to_remove.iter() {
                        debug!(topic = k, "deleting idle producer");
                        conns.remove(k);
                    }
                }
                to_remove.truncate(0);
                tokio::time::sleep(CLEANUP_PERIOD).await;
            }
        });

        producer
    }

    // produce resolves after requested number of broker(s) acknowledged write
    pub(crate) async fn produce(
        &self,
        topic: &str,
        brokers: &[String],
        messages: &[grpc::Message],
        durability: &grpc::Durability,
    ) -> Result<()> {
        let brokers = if brokers.is_empty() {
            self.cfg
                .topics
                .get(topic)
                .context("requested topic is not configured and no brokers specified")?
                .brokers
                .as_ref()
        } else {
            brokers
        };

        let producer = {
            let mut producers = self.producers.lock().expect("poisoned lock");
            // TODO: hash by topic+brokers+durability
            match producers.get_mut(topic) {
                Some(p) => p.claim(),
                None => {
                    let mut cfg = ClientConfig::new();
                    cfg.set("bootstrap.servers", brokers.join(","));
                    let producer =
                        ThreadedProducer::from_config_and_context(&cfg, DeliveryHandler {})
                            .context("connecting to kafka")?;
                    producers.insert(String::from(topic), Tracker::new(producer, PRODUCER_TTL));
                    producers.get_mut(topic).unwrap().claim()
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
        debug!(
            count = messages.len(),
            topic = topic,
            "producing batch of messages"
        );
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

            if let Err(e) = producer.send(record) {
                let _ = sender.send(Err((i, e.0))).await;
            }
        }

        result_handler.await?.context("producing messages")
    }
}
