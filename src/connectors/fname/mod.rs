use tokio::time::{sleep, Duration};
use serde::Deserialize;
use thiserror::Error;


// TODO: use actual logger


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
    count: u64,
    transfers: Vec<Transfer>,
}

impl Fetcher {
    pub fn new(initial_count: u64) -> Self {
        Fetcher {
            count: initial_count,
            transfers: vec![],
        }
    }

    async fn fetch(&mut self) -> Result<u64, FetchError> {
        let mut count = self.count;

        loop {
            let url = format!("https://fnames.farcaster.xyz/transfers?from_id={}", count);
            println!("fname: fetching url: {}", url);

            let response = reqwest::get(&url)
                .await?
                .json::<TransfersData>()
                .await?;

            let transfers_count = response.transfers.len();

            if transfers_count == 0 {
                return Ok(count);
            }

            println!("fname: found {} new transfers", transfers_count);

            for t in response.transfers {
                if t.id <= count {
                    return Err(FetchError::NonSequentialIds);
                }
                count = t.id;
                self.transfers.push(t); // Just store these for now, we'll use them later
            }
        }
    }

    pub async fn run(&mut self) -> FnamesError {
        loop {
            match self.fetch().await {
                Ok(new_count) => self.count = new_count,
                Err(e) => match e {
                    FetchError::NonSequentialIds => {
                        return FnamesError::NonSequentialIds;
                    }
                    FetchError::Reqwest(request_error) => {
                        println!("fname: reqwest error fetching: {}", request_error);
                    }
                    FetchError::NoNewIDs => {} // just sleep and retry
                },
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}