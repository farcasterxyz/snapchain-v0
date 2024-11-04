use std::sync::Arc;
use tokio::time::{sleep, Duration};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn, error, span, Level, Span};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub start_from: u64, // for testing
    pub stop_at: u64, // for testing
    pub url: String,
    pub disable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            start_from: 0,
            stop_at: 200, // set this default to a small value for now, revisit later
            url: "https://fnames.farcaster.xyz/transfers".to_string(),
            disable: false,
        }
    }
}

#[derive(Deserialize, Debug)]
struct TransfersData {
    transfers: Vec<Transfer>,
}

#[derive(Deserialize, Debug)]
struct Transfer {
    id: u64,
    timestamp: u64,
    username: String,
    owner: String,
    from: u64,
    to: u64,
    user_signature: String,
    server_signature: String,
}


#[derive(Error, Debug)]
enum FetchError {
    #[error("non-sequential IDs found")]
    NonSequentialIds {
        position: u64,
        id: u64,
    },

    #[error("no new IDs found")]
    NoNewIDs,

    #[error("stop fetching")]
    Stop,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}


pub struct Fetcher {
    position: u64,
    transfers: Vec<Transfer>,
    cfg: Config,
}


impl Fetcher {
    pub fn new(cfg: Config) -> Self {
        Fetcher {
            position: cfg.start_from,
            transfers: vec![],
            cfg: cfg,
        }
    }

    async fn fetch(&mut self) -> Result<(), FetchError> {
        loop {
            let url = format!("{}?from_id={}", self.cfg.url, self.position);
            debug!(%url, "fetching transfers");

            let response = reqwest::get(&url)
                .await?
                .json::<TransfersData>()
                .await?;

            let count = response.transfers.len();

            if count == 0 {
                return Ok(());
            }

            info!(count, position=self.position, "found new transfers");

            for t in response.transfers {
                if t.id <= self.position {
                    return Err(FetchError::NonSequentialIds { id: t.id, position: self.position });
                }
                if t.id > self.cfg.stop_at {
                    return Err(FetchError::Stop);
                }
                self.position = t.id;
                self.transfers.push(t); // Just store these for now, we'll use them later
            }
        }
    }

    pub async fn run(&mut self) -> () {
        loop {
            let result = self.fetch().await;

            if let Err(e) = result {
                match e {
                    FetchError::NonSequentialIds { id, position } => {
                        error!(id, position, %e);
                    }
                    FetchError::Reqwest(request_error) => {
                        warn!(error = %request_error, "reqwest error fetching transfers");
                    }
                    FetchError::NoNewIDs => {} // just sleep and retry
                    FetchError::Stop => {
                        info!(position = self.position, "stopped fetching transfers");
                        return;
                    }
                }
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}