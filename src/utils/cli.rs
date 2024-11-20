use crate::core::types::FARCASTER_EPOCH;
use crate::proto::message;
use crate::proto::rpc::snapchain_service_client::SnapchainServiceClient;
use crate::proto::{rpc, snapchain::Block};
use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use message::CastType::Cast;
use message::MessageType::CastAdd;
use message::{CastAddBody, FarcasterNetwork, MessageData};
use prost::Message;
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
    fid: u64,
    text: &str,
    timestamp: Option<u32>,
    private_key: Option<SigningKey>,
) -> message::Message {
    let key = private_key.unwrap_or_else(|| {
        SigningKey::from_bytes(
            &SecretKey::from_hex(
                "1000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        )
    });
    let network = FarcasterNetwork::Mainnet;

    let timestamp = timestamp.unwrap_or_else(|| {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - FARCASTER_EPOCH) as u32
    });

    let cast_add = CastAddBody {
        text: text.to_string(),
        embeds: vec![],
        embeds_deprecated: vec![],
        mentions: vec![],
        mentions_positions: vec![],
        parent: None,
        r#type: Cast as i32,
    };

    let msg_data = MessageData {
        fid,
        r#type: CastAdd as i32,
        timestamp,
        network: network as i32,
        body: Some(message::message_data::Body::CastAddBody(cast_add)),
    };

    let msg_data_bytes = msg_data.encode_to_vec();
    let hash = blake3::hash(&msg_data_bytes).as_bytes()[0..20].to_vec();

    let signature = key.sign(&hash).to_bytes();
    message::Message {
        data: Some(msg_data),
        hash_scheme: message::HashScheme::Blake3 as i32,
        hash: hash.clone(),
        signature_scheme: message::SignatureScheme::Ed25519 as i32,
        signature: signature.to_vec(),
        signer: key.verifying_key().to_bytes().to_vec(),
        data_bytes: None,
    }
}

pub async fn follow_blocks(
    addr: String,
    block_tx: mpsc::Sender<Block>,
) -> Result<(), Box<dyn Error>> {
    let mut client = rpc::snapchain_service_client::SnapchainServiceClient::connect(addr).await?;

    let mut i = 1;

    loop {
        let msg = rpc::BlocksRequest {
            shard_id: 0,
            start_block_number: i,
            stop_block_number: Some(i + FETCH_SIZE),
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
