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
    pub buffer_size: usize,
    pub min_bytes: usize,
    pub max_bytes: usize,
    pub message_timeout: Duration,
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
            buffer_size: 100,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            message_timeout: Duration::from_secs(30),
        }
    }
}
