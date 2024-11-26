use crate::consensus::proposer::current_time;
use crate::proto::msg as message;
use crate::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use crate::proto::{rpc, snapchain::Block};
use crate::utils::factory::messages_factory;
use ed25519_dalek::SigningKey;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::time;
use tonic::transport::Channel;

const FETCH_SIZE: u64 = 100;

// compose_message is a proof-of-concept script, is not guaranteed to be correct,
// and clearly needs a lot of work. Use at your own risk.
pub async fn send_message(
    client: &mut SnapchainServiceClient<Channel>,
    msg: &message::Message,
) -> Result<message::Message, Box<dyn Error>> {
    let request = tonic::Request::new(msg.clone());
    let response = client.submit_message(request).await?;
    // println!("{}", serde_json::to_string(&response.get_ref()).unwrap());
    Ok(response.into_inner())
}

pub fn compose_message(
    fid: u32,
    text: &str,
    timestamp: Option<u32>,
    private_key: Option<SigningKey>,
) -> message::Message {
    messages_factory::casts::create_cast_add(fid, text, timestamp, private_key)
}

pub async fn follow_blocks(
    addr: String,
    block_tx: mpsc::Sender<Block>,
) -> Result<(), Box<dyn Error>> {
    let mut client = rpc::snapchain_service_client::SnapchainServiceClient::connect(addr).await?;

    let mut i = 1;

    loop {
        let msg = rpc::BlocksRequest {
            start_block_number: i,
            stop_block_number: Some(i + FETCH_SIZE),
            start_timestamp: Some(current_time()),
        };

        let request = tonic::Request::new(msg);
        let response = client.get_blocks(request).await?;

        let inner = response.into_inner();
        if inner.blocks.is_empty() {
            time::sleep(time::Duration::from_millis(10)).await;
            continue;
        }

        for block in &inner.blocks {
            block_tx.send(block.clone()).await.unwrap();
            i += 1;
        }
    }
}
