use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use snapchain::{
    proto::rpc::snapchain_service_client::SnapchainServiceClient, utils::cli::compose_message,
};

#[tokio::main]
async fn main() {
    // feel free to specify your own key
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    );

    let mut client = SnapchainServiceClient::connect("http://127.0.0.1:3383".to_string())
        .await
        .unwrap();
    compose_message(&mut client, private_key, 6833, "Welcome from Rust!")
        .await
        .unwrap();
}
