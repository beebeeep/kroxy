use std::fs;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub bind_addr: String,
}

impl Config {
    pub fn new(file: &str) -> Result<Self> {
        let cfg = toml::from_str(&fs::read_to_string(file).context("reading config file")?)
            .context("parsing config")?;
        Ok(cfg)
    }
}
