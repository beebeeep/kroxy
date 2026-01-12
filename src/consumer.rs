use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    config::{Config, ConsumerConfig, TopicConfig},
    grpc,
    util::Tracker,
};
use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, ClientContext, Message, TopicPartitionList,
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::AbortHandle,
    time,
};
use tracing::{error, info};

const DISPATCHER_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(300);

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
        let (acks_tx, acks_rx) = mpsc::channel(16);
        let (releases_tx, releases_rx) = mpsc::channel(16);
        let (claims_tx, claims_rx) = mpsc::channel(16);
        let c = ConsumerInner {
            consumer,
            message_buffer: Vec::with_capacity(group_cfg.buffer_size),
            next_message: None,
            cfg: group_cfg.clone(),
            msg_acks: acks_rx,
            msg_releases: releases_rx,
            msg_claims: claims_rx,
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
    message_buffer: Vec<MessageWithMeta<'a>>,
    next_message: Option<(grpc::Message, usize)>,

    msg_acks: mpsc::Receiver<usize>, // channel for ACKing message in buffers indexed by value
    msg_releases: mpsc::Receiver<usize>, // channel for releasing claimed messages
    msg_claims: mpsc::Receiver<oneshot::Sender<grpc::Message>>, // channel for requesting message claims
}

impl<'a> ConsumerInner<'_> {
    async fn run(mut self) {
        // let mut next_message: Option<(grpc::Message, usize)> = None;
        loop {
            self.get_next_message();
            self.process_events().await;
            self.commit_if_needed();
        }
    }

    async fn process_events(&mut self) {
        select! {
            msg = self.consumer.recv(), if self.message_buffer.len() < self.cfg.buffer_size => {
                //new message from kafka, add to batch
                // NB: consumer.recv() is supposed to be cancellation-safe
                match msg {
                    Err(e) => {
                        error!("consuming message from kafka: {e}");
                    },
                    Ok(msg) => {
                        // Vec::push(&'a mut self.message.buffer, msg)
                        self.message_buffer.push(MessageWithMeta{ inner: msg, acked: false, claimed_at: None });
                    }
                };
            }
            Some(out_msg) = self.msg_claims.recv(), if self.next_message.is_some() => {
                let (msg, idx) = self.next_message.take().unwrap();
                self.message_buffer[idx].claimed_at = Some(Instant::now());
                let _ = out_msg.send(msg);
            }
            Some(idx) = self.msg_acks.recv() => {
                self.message_buffer[idx].acked = true;
            }
            Some(idx) = self.msg_releases.recv() => {
                self.message_buffer[idx].claimed_at = None;
            }
            _ = time::sleep(DISPATCHER_TIMEOUT) => {
                // periodically check for new messages, some may have timed out claims
            }
        };
    }

    // get_new_message find next unclaimed message and returns grpc::Message by copying key and payload
    fn get_next_message(&mut self) {
        if self.next_message.is_some() {
            return;
        }
        for (idx, msg) in self.message_buffer.iter().enumerate() {
            if !msg.acked && msg.claimed_at.is_none() {
                self.next_message = Some((
                    grpc::Message {
                        key: msg.inner.key().map_or_else(Vec::new, |v| v.to_vec()),
                        value: msg.inner.payload().map_or_else(Vec::new, |v| v.to_vec()),
                        timestamp: match msg.inner.timestamp() {
                            rdkafka::Timestamp::NotAvailable => None,
                            rdkafka::Timestamp::CreateTime(ts) => Some(prost_types::Timestamp {
                                seconds: ts,
                                nanos: 0,
                            }),
                            rdkafka::Timestamp::LogAppendTime(ts) => Some(prost_types::Timestamp {
                                seconds: ts,
                                nanos: 0,
                            }),
                        },
                        headers: msg.inner.headers().map_or_else(HashMap::new, |v| {
                            HashMap::from_iter(v.iter().map(|h| {
                                (
                                    h.key.to_string(),
                                    h.value.map_or_else(Vec::new, |v| v.to_vec()),
                                )
                            }))
                        }),
                    },
                    idx,
                ));
            }
        }
        self.next_message = None
    }

    fn commit_if_needed(&mut self) {
        todo!();
    }
}

struct MessageWithMeta<'a> {
    inner: BorrowedMessage<'a>,
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
