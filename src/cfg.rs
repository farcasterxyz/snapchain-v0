use crate::connectors;
use clap::Parser;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub id: u32,
    pub log_format: String,
    pub fnames: connectors::fname::Config,
    pub onchain_events: connectors::onchain_events::Config,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 0,
            log_format: "text".to_string(),
            fnames: connectors::fname::Config::default(),
            onchain_events: connectors::onchain_events::Config::default(),
        }
    }
}

#[derive(Parser)]
pub struct CliArgs {
    #[arg(long, help = "Unique identifier for the node")]
    id: Option<u32>,

    #[arg(long, help = "Log format (text or json)")]
    log_format: Option<String>,

    #[arg(long, help = "Path to the config file")]
    config_path: Option<String>,
    // All new arguments that are to override values from config files or environment variables
    // should be probably be optional (`Option<T>`) and without a default. Setting a default
    // in this case will have the effect of automatically overriding all previous configuration
    // layers. Remember to add the override code below and a test case.
}

pub fn load_and_merge_config(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    let cli_args = CliArgs::parse_from(args);

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    if let Some(config_path) = cli_args.config_path {
        if !Path::new(&config_path).exists() {
            return Err(format!("config file not found: {}", config_path).into());
        }
        figment = figment.merge(Toml::file(config_path));
    }

    figment = figment.merge(Env::prefixed("SNAPCHAIN_").split("__"));

    let mut config: Config = figment.extract()?;

    if let Some(id) = cli_args.id {
        config.id = id;
    }
    if let Some(log_format) = cli_args.log_format {
        config.log_format = log_format;
    }

    Ok(config)
}
