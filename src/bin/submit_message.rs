use clap::Parser;
use ed25519_dalek::{SecretKey, SigningKey};
use hex::FromHex;
use snapchain::utils::cli::compose_message;
use snapchain::{
    proto::rpc::snapchain_service_client::SnapchainServiceClient, utils::cli::send_message,
};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    addr: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // feel free to specify your own key
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    );

    let mut client = SnapchainServiceClient::connect(args.addr).await.unwrap();

    send_message(
        &mut client,
        &compose_message(6833, "Welcome from Rust!", None, Some(private_key)),
    )
    .await
    .unwrap();
}
