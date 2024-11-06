mod rpc {
    tonic::include_proto!("rpc");
}

mod message {
    tonic::include_proto!("message");
}

mod username_proof {
    tonic::include_proto!("username_proof");
}

use std::error::Error;
use hex::{ToHex, FromHex};
use prost::Message;
use rpc::snapchain_service_server::{SnapchainService};
use message::{CastAddBody, FarcasterNetwork, MessageData};
use message::CastType::{Cast};
use message::MessageType::{CastAdd};
use ed25519_dalek::{Signer, SigningKey, SecretKey};
use rpc::snapchain_service_client::SnapchainServiceClient;

const FARCASTER_EPOCH: u64 = 1609459200; // January 1, 2021 UTC

async fn compose_message(private_key: SigningKey) -> Result<(), Box<dyn Error>> {
    let fid = 6833; // FID of the user submitting the message
    let network = FarcasterNetwork::Mainnet;

    let timestamp = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - FARCASTER_EPOCH) as u32;

    let cast_add = CastAddBody {
        text: "Welcome from Rust!".to_string(),
        embeds: vec![],
        embeds_deprecated: vec![],
        mentions: vec![],
        mentions_positions: vec![],
        parent: None,
        r#type: Cast as i32,
    };

    let mut msg_data = MessageData {
        fid,
        r#type: CastAdd as i32,
        timestamp: timestamp,
        network: network as i32,
        body: Some(message::message_data::Body::CastAddBody(cast_add)),
    };

    let msg_data_bytes = msg_data.encode_to_vec();
    let hash = blake3::hash(&msg_data_bytes).as_bytes()[0..20].to_vec();

    let mut msg = message::Message::default();
    msg.hash_scheme = message::HashScheme::Blake3 as i32;
    msg.hash = hash.clone();

    let signature = private_key.sign(&hash).to_bytes();

    msg.signature_scheme = message::SignatureScheme::Ed25519 as i32;
    msg.signature = signature.to_vec();
    msg.signer = private_key.verifying_key().to_bytes().to_vec();
    msg.data_bytes = Some(msg_data_bytes);

    let mut client = SnapchainServiceClient::connect("http://127.0.0.1:50061").await?;
    let request = tonic::Request::new(msg);
    let response = client.submit_message(request).await?;

    println!("{}", serde_json::to_string(&response.get_ref()).unwrap());

    Ok(())
}

#[tokio::main]
async fn main() {
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000").unwrap()
    );

    compose_message(private_key).await.unwrap();
}