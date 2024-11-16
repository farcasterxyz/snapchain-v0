use crate::{connectors, consensus, network};
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
    pub consensus: consensus::consensus::Config,
    pub gossip: network::gossip::Config,
    pub rpc_address: String,
    pub rocksdb_dir: String,
    pub clear_db: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 0,
            log_format: "text".to_string(),
            fnames: connectors::fname::Config::default(),
            onchain_events: connectors::onchain_events::Config::default(),
            consensus: consensus::consensus::Config::default(),
            gossip: network::gossip::Config::default(),
            rpc_address: "0.0.0.0:3383".to_string(),
            rocksdb_dir: ".rocks".to_string(),
            clear_db: false,
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

    #[arg(long, action, help = "Start the node with a clean database")]
    clear_db: bool,
    // All new arguments that are to override values from config files or environment variables
    // should be probably be optional (`Option<T>`) and without a default. Setting a default
    // in this case will have the effect of automatically overriding all previous configuration
    // layers. Remember to add the override code below and a test case.
}

pub fn load_and_merge_config(args: Vec<String>) -> Result<Config, Box<dyn Error>> {
    let cli_args = CliArgs::parse_from(args);

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    let config_path: String;
    if cli_args.id.is_some() && cli_args.config_path.is_none() {
        config_path = format!("nodes/{:}/snapchain.toml", cli_args.id.unwrap());
    } else if let Some(path) = &cli_args.config_path {
        config_path = path.to_string();
    } else {
        config_path = "snapchain.toml".to_string();
    }

    if Path::new(&config_path).exists() {
        figment = figment.merge(Toml::file(config_path));
    } else {
        // If the config path was explicitly set and we didn't find it, error
        if cli_args.config_path.is_some() {
            return Err(format!("config file not found: {}", config_path).into());
        }
    }

    figment = figment.merge(Env::prefixed("SNAPCHAIN_").split("__"));

    let mut config: Config = figment.extract()?;

    if let Some(id) = cli_args.id {
        config.id = id;
    }
    if let Some(log_format) = cli_args.log_format {
        config.log_format = log_format;
    }
    config.clear_db = cli_args.clear_db;

    Ok(config)
}
