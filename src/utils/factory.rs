use crate::core::types::FARCASTER_EPOCH;
use crate::proto::msg as message;
use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use message::CastType::Cast;
use message::MessageType;
use message::{CastAddBody, FarcasterNetwork, MessageData};
use prost::Message;

pub mod messages_factory {
    use super::*;

    pub fn farcaster_time() -> u32 {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - FARCASTER_EPOCH) as u32
    }

    pub fn create_message_with_data(
        fid: u32,
        body: message::message_data::Body,
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

        let timestamp = timestamp.unwrap_or_else(|| farcaster_time());

        let msg_type = match &body {
            message::message_data::Body::CastAddBody(_) => MessageType::CastAdd,
            message::message_data::Body::CastRemoveBody(_) => MessageType::CastRemove,
            _ => panic!("Invalid message type"),
        };

        let msg_data = MessageData {
            fid: fid as u64,
            r#type: msg_type as i32,
            timestamp,
            network: network as i32,
            body: Some(body),
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

    pub mod casts {
        use super::*;
        use crate::proto::msg::CastRemoveBody;

        pub fn create_cast_add(
            fid: u32,
            text: &str,
            timestamp: Option<u32>,
            private_key: Option<SigningKey>,
        ) -> message::Message {
            let cast_add = CastAddBody {
                text: text.to_string(),
                embeds: vec![],
                embeds_deprecated: vec![],
                mentions: vec![],
                mentions_positions: vec![],
                parent: None,
                r#type: Cast as i32,
            };
            create_message_with_data(
                fid,
                message::message_data::Body::CastAddBody(cast_add),
                timestamp,
                private_key,
            )
        }

        pub fn create_cast_remove(
            fid: u32,
            target_hash: &Vec<u8>,
            timestamp: Option<u32>,
            private_key: Option<SigningKey>,
        ) -> crate::proto::msg::Message {
            let cast_remove = CastRemoveBody {
                target_hash: target_hash.clone(),
            };
            create_message_with_data(
                fid,
                message::message_data::Body::CastRemoveBody(cast_remove),
                timestamp,
                private_key,
            )
        }
    }
}
