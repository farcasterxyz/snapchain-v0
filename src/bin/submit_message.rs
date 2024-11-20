use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use snapchain::utils::cli::compose_message;
use snapchain::{
    proto::rpc::snapchain_service_client::SnapchainServiceClient, utils::cli::send_message,
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

    send_message(
        &mut client,
        &compose_message(6833, "Welcome from Rust!", None, Some(private_key)),
    )
    .await
    .unwrap();
}
