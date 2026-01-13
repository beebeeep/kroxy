use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    config::{Config, ConsumerConfig},
    grpc,
    util::Tracker,
};
use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::{BorrowedMessage, Headers},
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::AbortHandle,
};
use tracing::{debug, error, info};

const DISPATCHER_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(300);
const CONSUMER_TTL: Duration = Duration::from_secs(5);
const CLEANUP_PERIOD: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct ConsumersManager {
    cfg: Config,
    consumers: Arc<Mutex<HashMap<String, Tracker<BatchedConsumer>>>>,
}
impl ConsumersManager {
    pub(crate) fn new(cfg: &Config) -> Self {
        let consumers = Arc::new(Mutex::new(HashMap::new()));
        let manager = Self {
            cfg: cfg.clone(),
            consumers: consumers.clone(),
        };

        info!("starting consumer manager");
        tokio::spawn(async move {
            let mut to_remove = Vec::with_capacity(1);
            loop {
                {
                    let mut cons = consumers.lock().expect("poisoned lock");
                    for (k, v) in cons.iter() {
                        if !v.is_used() && v.is_expired() {
                            to_remove.push(k.clone());
                        }
                    }
                    for k in to_remove.iter() {
                        debug!(topic = k, "deleting unused idle consumer");
                        cons.remove(k);
                    }
                }
                to_remove.truncate(0);
                tokio::time::sleep(CLEANUP_PERIOD).await;
            }
        });
        manager
    }

    pub(crate) fn get_consumer(
        &self,
        topic: &str,
        brokers: &[String],
        consumer_group: &str,
    ) -> Result<Arc<BatchedConsumer>> {
        let consumer_id = format!("{topic}@{consumer_group}");
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
        let consumer = {
            let mut consumers = self.consumers.lock().expect("poisoned lock");
            match consumers.get_mut(&consumer_id) {
                Some(c) => c.claim(),
                None => {
                    let group_cfg = self
                        .cfg
                        .topics
                        .get(topic)
                        .and_then(|c| c.consumers.get(consumer_group))
                        .map_or_else(ConsumerConfig::default, |v| v.clone());
                    let consumer = BatchedConsumer::new(topic, consumer_group, brokers, group_cfg)
                        .context("creating consumer")?;
                    consumers.insert(consumer_id.clone(), Tracker::new(consumer, CONSUMER_TTL));
                    consumers.get_mut(&consumer_id).unwrap().claim()
                }
            }
        };

        Ok(consumer)
    }
}

#[derive(Clone)]
pub(crate) struct BatchedConsumer {
    dispatcher_handle: AbortHandle,
    claims: mpsc::Sender<oneshot::Sender<MessageWithMeta>>,
    acks: mpsc::Sender<usize>,
    releases: mpsc::Sender<usize>,
}

impl BatchedConsumer {
    fn new(
        topic: &str,
        consumer_group: &str,
        brokers: &[String],
        cfg: ConsumerConfig,
    ) -> Result<Self> {
        let mut consumer_cfg = ClientConfig::new();
        consumer_cfg
            .set("group.id", consumer_group)
            .set("bootstrap.servers", brokers.join(","))
            .set("session.timeout.ms", "5000")
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "false")
            // .set("queued.max.messages.kbytes ", todo!()) // TODO: guess this defines how many BorrowedMessages we can store at the same time?
            .set_log_level(RDKafkaLogLevel::Debug);
        info!(config = format!("{:?}", consumer_cfg), "consumer");
        let (commit_cb_tx, commit_cb_rx) = mpsc::channel(1);
        let consumer: StreamConsumer<CustomContext> = StreamConsumer::from_config_and_context(
            &consumer_cfg,
            CustomContext {
                commit_cb: commit_cb_tx,
            },
        )
        .context("creating new consumer")?;
        let (acks_tx, acks_rx) = mpsc::channel(16);
        let (releases_tx, releases_rx) = mpsc::channel(16);
        let (claims_tx, claims_rx) = mpsc::channel(16);
        consumer
            .subscribe(&[topic])
            .context("subscribing to topic")?;
        let dispatcher = ConsumerDispatcher {
            consumer,
            topic: topic.to_string(),
            message_buffer: Vec::with_capacity(cfg.buffer_size),
            acks: 0,
            next_message: None,
            cfg: cfg,
            msg_acks: acks_rx,
            msg_releases: releases_rx,
            msg_claims: claims_rx,
            commit_cb: commit_cb_rx,
        };
        let handle = tokio::spawn(dispatcher.run()).abort_handle();

        Ok(Self {
            dispatcher_handle: handle,
            claims: claims_tx,
            acks: acks_tx,
            releases: releases_tx,
        })
    }

    pub(crate) async fn claim_message(&self) -> Result<MessageWithMeta> {
        let (msg_tx, msg_rx) = oneshot::channel();
        self.claims.send(msg_tx).await.context("consumer closed")?;

        Ok(msg_rx.await.context("claiming message from consumer")?)
    }

    pub(crate) async fn release_message(&self, id: usize) -> Result<()> {
        self.releases.send(id).await.context("consumer closed")
    }

    pub(crate) async fn ack_message(&self, id: usize) -> Result<()> {
        self.acks.send(id).await.context("consumer closed")
    }
}

impl Drop for BatchedConsumer {
    fn drop(&mut self) {
        self.dispatcher_handle.abort();
    }
}

struct ConsumerDispatcher {
    cfg: ConsumerConfig,
    topic: String,
    consumer: StreamConsumer<CustomContext>,
    message_buffer: Vec<MessageWithMeta>,
    next_message: Option<MessageWithMeta>, // message to be sent to next consumer
    acks: usize,                           // number of ACKed messages in buffer

    msg_acks: mpsc::Receiver<usize>, // channel for ACKing message in buffers indexed by value
    msg_releases: mpsc::Receiver<usize>, // channel for releasing claimed messages
    msg_claims: mpsc::Receiver<oneshot::Sender<MessageWithMeta>>, // channel for requesting message claims
    commit_cb: mpsc::Receiver<KafkaResult<()>>,                   // channel for commit results
}

impl ConsumerDispatcher {
    async fn run(mut self) {
        debug!(topic = self.topic, "starting consumer dispatcher");
        loop {
            self.find_next_message();
            self.process_events().await;
            self.commit_if_needed().await;
        }
    }

    async fn process_events(&mut self) {
        select! {
            msg = self.consumer.recv(), if self.message_buffer.len() < self.cfg.buffer_size => {
                debug!("got message");
                //new message from kafka, add to batch
                // NB: consumer.recv() is supposed to be cancellation-safe
                // NB: this implementation seem to be less efficient that it could have been:
                // message from kafka is copied 2 times:
                // 1st when we are getting it from consumer's internal buffer and putting into our buffer
                // 2nd when we prepare next_message that will go to subscribed grpc client
                // It is probably possible to avoid excessive data copying by keeping rdkafka::message::BorrowedMessage in our buffer,
                // but I failed miserably doing lifetimes properly :'(
                match msg {
                    Err(e) => {
                        error!("consuming message from kafka: {e}");
                    },
                    Ok(msg) => {
                        // we use message's index in buffer as its ID
                        // for consumers ID is opaque
                        self.message_buffer.push(MessageWithMeta::new(msg, self.message_buffer.len()));
                    }
                };
            }
            Some(out_msg) = self.msg_claims.recv(), if self.next_message.is_some() => {
                debug!("sent message");
                let msg = self.next_message.take().unwrap();
                self.message_buffer[msg.id].claim_expire = Some(Instant::now().checked_add(Duration::from_millis(self.cfg.message_timeout_ms)).unwrap());
                let _ = out_msg.send(msg);
            }
            Some(idx) = self.msg_acks.recv() => {
                debug!("ack");
                self.acks += 1;
                self.message_buffer[idx].acked = true;
            }
            Some(idx) = self.msg_releases.recv() => {
                debug!("release");
                self.message_buffer[idx].claim_expire = None;
            }
            _ = tokio::time::sleep(DISPATCHER_TIMEOUT) => {
                debug!("timeout!");
                // periodically check for new messages, some may have timed out claims
            }
        };
    }

    // find_next_message find next unclaimed message and returns its copy
    fn find_next_message(&mut self) {
        if self.next_message.is_some() {
            return;
        }
        for msg in &self.message_buffer {
            if !msg.acked && msg.claim_expire.map_or(true, |v| v < Instant::now()) {
                self.next_message = Some(msg.clone());
            }
        }
        self.next_message = None
    }

    async fn commit_if_needed(&mut self) {
        // TODO: partial commit on exit
        if self.acks == 0 || self.acks < self.message_buffer.len() {
            return;
        }

        let mut tpl = HashMap::new();
        for msg in &self.message_buffer {
            match tpl.get_mut(&msg.partition) {
                Some(Offset::Offset(v)) => {
                    if *v < msg.offset {
                        *v = msg.offset;
                    }
                }
                Some(_) => {
                    panic!("unexpected offset in TopicPartitionList")
                }
                None => {
                    tpl.insert(msg.partition, Offset::from_raw(msg.offset));
                }
            }
        }

        let tpl = TopicPartitionList::from_topic_map(&HashMap::from_iter(
            tpl.into_iter()
                .map(|(partition, offset)| ((self.topic.clone(), partition), offset)),
        ))
        .expect("generated incorrect TopicParititionList");

        if tpl.count() == 0 {
            // just in case, committing empty TPL never succeeds
            return;
        }

        debug!(partition_count = tpl.count(), "committing offsets");
        if let Err(e) = self
            .consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
        {
            error!("starting batch commit: {e}");
        }
        match self
            .commit_cb
            .recv()
            .await
            .expect("commit callback channel shall be open")
        {
            Ok(_) => {}
            Err(e) => {
                // we will keep retrying
                error!("committing batch to kafka: {e}");
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct MessageWithMeta {
    pub(crate) inner: grpc::Message,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) id: usize,

    acked: bool,
    claim_expire: Option<Instant>,
}

impl MessageWithMeta {
    fn new(msg: BorrowedMessage, id: usize) -> Self {
        Self {
            inner: grpc::Message {
                key: msg.key().map_or_else(Vec::new, |v| v.to_vec()),
                value: msg.payload().map_or_else(Vec::new, |v| v.to_vec()),
                timestamp: match msg.timestamp() {
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
                headers: msg.headers().map_or_else(HashMap::new, |v| {
                    HashMap::from_iter(v.iter().map(|h| {
                        (
                            h.key.to_string(),
                            h.value.map_or_else(Vec::new, |v| v.to_vec()),
                        )
                    }))
                }),
            },
            id,
            partition: msg.partition(),
            offset: msg.offset(),
            acked: false,
            claim_expire: None,
        }
    }
}

struct CustomContext {
    commit_cb: mpsc::Sender<KafkaResult<()>>,
}

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
        let _ = self.commit_cb.blocking_send(result);
    }
}
