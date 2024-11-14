use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use snapchain::utils::cli::compose_message;

#[tokio::main]
async fn main() {
    // feel free to specify your own key
    let private_key = SigningKey::from_bytes(
        &SecretKey::from_hex("1000000000000000000000000000000000000000000000000000000000000000")
            .unwrap(),
    );

    compose_message(
        private_key,
        6833,
        "http://127.0.0.1:3383".to_string(),
        "Welcome from Rust!",
    )
    .await
    .unwrap();
}
