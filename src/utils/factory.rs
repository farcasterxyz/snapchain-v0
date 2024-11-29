use crate::core::types::FARCASTER_EPOCH;
use crate::proto::msg as message;
use crate::proto::onchain_event::{OnChainEvent, OnChainEventType};
use ed25519_dalek::{SecretKey, Signer, SigningKey};
use hex::FromHex;
use message::CastType::Cast;
use message::MessageType;
use message::{CastAddBody, FarcasterNetwork, MessageData};
use prost::Message;

pub mod time {
    use super::*;

    pub fn farcaster_time() -> u32 {
        (current_timestamp() as u64 - FARCASTER_EPOCH) as u32
    }

    pub fn farcaster_time_with_offset(offset: i32) -> u32 {
        (farcaster_time() as i32 + offset) as u32
    }

    pub fn current_timestamp() -> u32 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32
    }

    pub fn current_timestamp_with_offset(offset: i32) -> u32 {
        (current_timestamp() as i32 + offset as i32) as u32
    }
}

pub mod events_factory {
    use super::*;
    use crate::proto::onchain_event;

    pub fn create_onchain_event(fid: u32) -> OnChainEvent {
        OnChainEvent {
            r#type: OnChainEventType::EventTypeIdRegister as i32,
            chain_id: 10,
            block_number: rand::random::<u32>(),
            block_hash: vec![],
            block_timestamp: 0,
            transaction_hash: rand::random::<[u8; 32]>().to_vec(),
            log_index: 0,
            fid: fid as u64,
            tx_index: 0,
            version: 1,
            body: None,
        }
    }

    pub fn create_rent_event(
        fid: u32,
        legacy_units: Option<u32>,
        units: Option<u32>,
        expired: bool,
    ) -> OnChainEvent {
        if legacy_units.is_some() && units.is_some() {
            panic!("Cannot have both legacy_units and units");
        }
        let one_year_in_seconds = 365 * 24 * 60 * 60;
        let rent_units;
        let mut timestamp = time::current_timestamp_with_offset(-10);
        if legacy_units.is_some() {
            rent_units = legacy_units.unwrap();
            if expired {
                timestamp = timestamp - one_year_in_seconds * 3;
            } else {
                timestamp = timestamp - one_year_in_seconds;
            }
        } else if units.is_some() {
            rent_units = units.unwrap();
            if expired {
                panic!("New units cannot be expired until 1 year from legacy cutoff");
            }
        } else {
            // random number between 1 and 10
            rent_units = rand::random::<u32>() % 10 + 1;
            if expired {
                panic!("New units cannot be expired until 1 year from legacy cutoff");
            }
        }

        let rent_event_body = onchain_event::StorageRentEventBody {
            expiry: 0, // This field is ignored, we use block_timestamp to calculate expiry
            units: rent_units,
            payer: rand::random::<[u8; 32]>().to_vec(),
        };
        OnChainEvent {
            r#type: OnChainEventType::EventTypeStorageRent as i32,
            chain_id: 10,
            block_number: rand::random::<u32>(),
            block_hash: vec![],
            block_timestamp: timestamp as u64,
            transaction_hash: rand::random::<[u8; 32]>().to_vec(),
            log_index: 0,
            fid: fid as u64,
            tx_index: 0,
            version: 1,
            body: Some(onchain_event::on_chain_event::Body::StorageRentEventBody(
                rent_event_body,
            )),
        }
    }

    pub fn create_signer_event(
        fid: u32,
        signer: Vec<u8>,
        event_type: onchain_event::SignerEventType,
    ) -> OnChainEvent {
        let signer_event_body = onchain_event::SignerEventBody {
            key: signer,
            event_type: event_type as i32,
            metadata: vec![],
            key_type: 1,
            metadata_type: 1,
        };
        OnChainEvent {
            r#type: OnChainEventType::EventTypeSigner as i32,
            chain_id: 10,
            block_number: rand::random::<u32>(),
            block_hash: vec![],
            block_timestamp: time::current_timestamp_with_offset(-10) as u64,
            transaction_hash: rand::random::<[u8; 32]>().to_vec(),
            log_index: 0,
            fid: fid as u64,
            tx_index: 0,
            version: 1,
            body: Some(onchain_event::on_chain_event::Body::SignerEventBody(
                signer_event_body,
            )),
        }
    }
}

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
        msg_type: MessageType,
        body: message::message_data::Body,
        timestamp: Option<u32>,
        private_key: Option<&SigningKey>,
    ) -> message::Message {
        let key = match private_key {
            Some(key) => key,
            None => &SigningKey::from_bytes(
                &SecretKey::from_hex(
                    "1000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap(),
            ),
        };
        let network = FarcasterNetwork::Mainnet;

        let timestamp = timestamp.unwrap_or_else(|| farcaster_time());

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
            private_key: Option<&SigningKey>,
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
                MessageType::CastAdd,
                message::message_data::Body::CastAddBody(cast_add),
                timestamp,
                private_key,
            )
        }

        pub fn create_cast_remove(
            fid: u32,
            target_hash: &Vec<u8>,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> crate::proto::msg::Message {
            let cast_remove = CastRemoveBody {
                target_hash: target_hash.clone(),
            };
            create_message_with_data(
                fid,
                MessageType::CastRemove,
                message::message_data::Body::CastRemoveBody(cast_remove),
                timestamp,
                private_key,
            )
        }
    }

    pub mod links {
        use message::{link_body::Target, LinkBody, LinkCompactStateBody};

        use super::*;

        pub fn create_link_add(
            fid: u32,
            link_type: String,
            target_fid: u32,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let link_body = LinkBody {
                r#type: link_type,
                display_timestamp: None,
                target: Some(Target::TargetFid(target_fid as u64)),
            };
            create_message_with_data(
                fid,
                MessageType::LinkAdd,
                message::message_data::Body::LinkBody(link_body),
                timestamp,
                private_key,
            )
        }

        pub fn create_link_remove(
            fid: u32,
            link_type: String,
            target_fid: u32,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> crate::proto::msg::Message {
            let link_body = LinkBody {
                r#type: link_type,
                display_timestamp: None,
                target: Some(Target::TargetFid(target_fid as u64)),
            };
            create_message_with_data(
                fid,
                MessageType::LinkRemove,
                message::message_data::Body::LinkBody(link_body),
                timestamp,
                private_key,
            )
        }

        pub fn create_link_compact_state(
            fid: u32,
            link_type: String,
            target_fid: u32,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> crate::proto::msg::Message {
            let link_compact_state_body = LinkCompactStateBody {
                r#type: link_type,
                target_fids: vec![target_fid as u64],
            };

            create_message_with_data(
                fid,
                MessageType::LinkCompactState,
                message::message_data::Body::LinkCompactStateBody(link_compact_state_body),
                timestamp,
                private_key,
            )
        }
    }

    pub mod reactions {
        use message::{reaction_body::Target, ReactionBody, ReactionType};

        use super::*;

        pub fn create_reaction_add(
            fid: u32,
            reaction_type: ReactionType,
            target_url: String,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let reaction_body = ReactionBody {
                r#type: reaction_type as i32,
                target: Some(Target::TargetUrl(target_url)),
            };
            create_message_with_data(
                fid,
                MessageType::ReactionAdd,
                message::message_data::Body::ReactionBody(reaction_body),
                timestamp,
                private_key,
            )
        }

        pub fn create_reaction_remove(
            fid: u32,
            reaction_type: ReactionType,
            target_url: String,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let reaction_body = ReactionBody {
                r#type: reaction_type as i32,
                target: Some(Target::TargetUrl(target_url)),
            };
            create_message_with_data(
                fid,
                MessageType::ReactionRemove,
                message::message_data::Body::ReactionBody(reaction_body),
                timestamp,
                private_key,
            )
        }
    }
    pub mod user_data {
        use message::{UserDataBody, UserDataType};

        use super::*;

        pub fn create_user_data_add(
            fid: u32,
            user_data_type: UserDataType,
            value: String,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let user_data_body = UserDataBody {
                r#type: user_data_type as i32,
                value,
            };
            create_message_with_data(
                fid,
                MessageType::UserDataAdd,
                message::message_data::Body::UserDataBody(user_data_body),
                timestamp,
                private_key,
            )
        }
    }

    pub mod verifications {
        use message::{VerificationAddAddressBody, VerificationRemoveBody};

        use super::*;

        pub fn create_verification_add(
            fid: u32,
            verification_type: u32,
            address: String,
            claim_signature: String,
            block_hash: String,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let body = VerificationAddAddressBody {
                address: address.encode_to_vec(),
                claim_signature: claim_signature.encode_to_vec(),
                block_hash: block_hash.encode_to_vec(),
                verification_type,
                chain_id: 0,
                protocol: 0,
            };
            create_message_with_data(
                fid,
                MessageType::VerificationAddEthAddress,
                message::message_data::Body::VerificationAddAddressBody(body),
                timestamp,
                private_key,
            )
        }

        pub fn create_verification_remove(
            fid: u32,
            address: String,
            timestamp: Option<u32>,
            private_key: Option<&SigningKey>,
        ) -> message::Message {
            let body = VerificationRemoveBody {
                address: address.encode_to_vec(),
                protocol: 0,
            };
            create_message_with_data(
                fid,
                MessageType::VerificationRemove,
                message::message_data::Body::VerificationRemoveBody(body),
                timestamp,
                private_key,
            )
        }
    }
}
