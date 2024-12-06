use crate::proto::admin_service_client::AdminServiceClient;
use crate::proto::hub_service_client::HubServiceClient;
use crate::proto::OnChainEvent;
use crate::proto::{self, Block};
use crate::utils::factory::messages_factory;
use ed25519_dalek::SigningKey;
use std::error::Error;
use tokio::sync::mpsc;
use tokio::time;
use tonic::transport::Channel;

use super::factory;

const FETCH_SIZE: u64 = 100;

// compose_message is a proof-of-concept script, is not guaranteed to be correct,
// and clearly needs a lot of work. Use at your own risk.
pub async fn send_message(
    client: &mut HubServiceClient<Channel>,
    msg: &proto::Message,
) -> Result<proto::Message, Box<dyn Error>> {
    let request = tonic::Request::new(msg.clone());
    let response = client.submit_message(request).await?;
    // println!("{}", serde_json::to_string(&response.get_ref()).unwrap());
    Ok(response.into_inner())
}

pub async fn send_on_chain_event(
    client: &mut AdminServiceClient<Channel>,
    onchain_event: &OnChainEvent,
) -> Result<OnChainEvent, Box<dyn Error>> {
    let request = tonic::Request::new(onchain_event.clone());
    let response = client.submit_on_chain_event(request).await?;
    Ok(response.into_inner())
}

pub fn compose_rent_event(fid: u32) -> OnChainEvent {
    factory::events_factory::create_rent_event(fid, None, Some(10), false)
}

pub fn compose_message(
    fid: u32,
    text: &str,
    timestamp: Option<u32>,
    private_key: Option<&SigningKey>,
) -> proto::Message {
    messages_factory::casts::create_cast_add(fid, text, timestamp, private_key)
}

pub async fn follow_blocks(
    addr: String,
    block_tx: mpsc::Sender<Block>,
) -> Result<(), Box<dyn Error>> {
    let mut client = proto::hub_service_client::HubServiceClient::connect(addr).await?;

    let mut i = 1;

    loop {
        let msg = proto::BlocksRequest {
            shard_id: 0,
            start_block_number: i,
            stop_block_number: Some(i + FETCH_SIZE),
        };

        let request = tonic::Request::new(msg);
        let mut response = client.get_blocks(request).await?.into_inner();
        while let Ok(Some(block)) = response.message().await {
            block_tx.send(block.clone()).await.unwrap();
            i += 1;
        }

        time::sleep(time::Duration::from_millis(10)).await;
    }
}
