use std::{
    collections::HashMap,
    io::Read,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    config::{Config, ConsumerConfig, TopicConfig},
    grpc,
    util::Tracker,
};
use anyhow::{Context, Result, anyhow};
use rdkafka::{
    ClientConfig, ClientContext, Message, TopicPartitionList,
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::{BorrowedMessage, OwnedMessage},
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::{AbortHandle, JoinHandle},
};
use tracing::{error, info};

pub(crate) struct ConsumersManager {
    cfg: Config,
    consumers: Arc<Mutex<HashMap<String, Tracker<Consumer>>>>,
}
impl ConsumersManager {}

#[derive(Clone)]
struct Consumer {
    stop_dispatcher: AbortHandle,
}

impl Consumer {
    fn new(
        topic: &str,
        consumer_group: &str,
        brokers: &[String],
        cfg: &TopicConfig,
    ) -> Result<Self> {
        let mut consumer_cfg = ClientConfig::new();
        consumer_cfg
            .set("group.id", consumer_group)
            .set("bootstrap.servers", brokers.join(", "))
            .set("session.timeout.ms", "5000")
            .set("enable.auto.commit", "false")
            // .set("queued.max.messages.kbytes ", todo!()) // TODO: guess this defines how many BorrowedMessages we can store at the same time?
            .set_log_level(RDKafkaLogLevel::Debug);
        let consumer: StreamConsumer<CustomContext> =
            StreamConsumer::from_config_and_context(&consumer_cfg, CustomContext {})
                .context("creating new consumer")?;
        let group_cfg = match cfg.consumers.get(topic) {
            None => &ConsumerConfig::default(),
            Some(c) => c,
        };
        let c = ConsumerInner {
            consumer,
            message_buffer: Vec::with_capacity(group_cfg.buffer_size),
            meta_buffer: Vec::with_capacity(group_cfg.buffer_size),
        };
        let handler = tokio::spawn(c.run()).abort_handle();

        Ok(Self {
            stop_dispatcher: handler,
        })
    }
}

struct ConsumerInner<'a> {
    cfg: ConsumerConfig,
    consumer: StreamConsumer<CustomContext>,
    message_buffer: Vec<BorrowedMessage<'a>>,
    meta_buffer: Vec<MessageMeta>, // XXXXXXXXXXXXXXXXXX mb wrap BorrowedMessage with metadata?

    msg_acks: mpsc::Receiver<usize>, // channel for ACKing message in buffers indexed by value
    msg_release: mpsc::Receiver<usize>, // channel for releasing claimed messages
    msg_claims: mpsc::Receiver<oneshot::Sender<grpc::Message>>, // channel for requesting message claims
}

impl<'a> ConsumerInner<'a> {
    async fn run(mut self) {
        loop {
            select! {
                msg = self.consumer.recv(), if self.message_buffer.len() < self.cfg.buffer_size => {
                    //new message from kafka, add to batch
                    // NB: consumer.recv() is supposed to be cancellation-safe
                    match msg {
                        Err(e) => {
                            error!("consuming message from kafka: {e}");
                        },
                        Ok(msg) => {
                            self.message_buffer.push(msg);
                            self.meta_buffer.push(MessageMeta { acked: false, claimed_at: None });
                        }
                    };
                }
            }
        }
    }
}

struct MessageMeta {
    acked: bool,
    claimed_at: Option<Instant>,
}

struct CustomContext;
impl ClientContext for CustomContext {}
impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}
