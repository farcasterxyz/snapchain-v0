use tokio::time::{sleep, Duration};
use serde::Deserialize;
use thiserror::Error;
use tracing::{debug, info, warn, span, Level, Span};


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
pub enum FnamesError {
    #[error("non-sequential IDs found")]
    NonSequentialIds,
}


#[derive(Error, Debug)]
enum FetchError {
    #[error("non-sequential IDs found")]
    NonSequentialIds,

    #[error("no new IDs found")]
    NoNewIDs,

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}


pub struct Fetcher {
    position: u64,
    transfers: Vec<Transfer>,
}


impl Fetcher {
    pub fn new(start_from: u64) -> Self {
        Fetcher {
            position: start_from,
            transfers: vec![],
        }
    }

    async fn fetch(&mut self) -> Result<u64, FetchError> {
        loop {
            let url = format!("https://fnames.farcaster.xyz/transfers?from_id={}", self.position);
            debug!(%url, "fetching transfers");

            let response = reqwest::get(&url)
                .await?
                .json::<TransfersData>()
                .await?;

            let count = response.transfers.len();

            if count == 0 {
                return Ok(self.position);
            }

            info!(count, position=self.position, "found new transfers");

            for t in response.transfers {
                if t.id <= self.position {
                    return Err(FetchError::NonSequentialIds);
                }
                self.position = t.id;
                self.transfers.push(t); // Just store these for now, we'll use them later
            }
        }
    }

    pub async fn run(&mut self) -> FnamesError {
        loop {
            match self.fetch().await {
                Ok(new_count) => self.position = new_count,
                Err(e) => match e {
                    FetchError::NonSequentialIds => {
                        return FnamesError::NonSequentialIds;
                    }
                    FetchError::Reqwest(request_error) => {
                        warn!(error = %request_error, "reqwest error fetching transfers");
                    }
                    FetchError::NoNewIDs => {} // just sleep and retry
                },
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}