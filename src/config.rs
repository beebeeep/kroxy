use std::{collections::HashMap, fs, time::Duration};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub bind_addr: String,
    pub topics: HashMap<String, TopicConfig>,
}

#[derive(Clone, Deserialize)]
pub struct TopicConfig {
    pub brokers: Vec<String>,
    pub consumers: HashMap<String, ConsumerConfig>,
}

#[derive(Clone, Deserialize)]
pub struct ConsumerConfig {
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    #[serde(default = "default_min_bytes")]
    pub min_bytes: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_timeout")]
    pub message_timeout_ms: u64,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let cfg: Self = toml::from_str(&fs::read_to_string(file).context("reading config file")?)
            .context("parsing config")?;
        Ok(cfg)
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            buffer_size: default_buffer_size(),
            min_bytes: default_min_bytes(),
            max_bytes: default_max_bytes(),
            message_timeout_ms: default_timeout(),
        }
    }
}

fn default_min_bytes() -> usize {
    1
}
fn default_max_bytes() -> usize {
    50 * 1024 * 1024
}
fn default_timeout() -> u64 {
    10000
}
fn default_buffer_size() -> usize {
    100
}
