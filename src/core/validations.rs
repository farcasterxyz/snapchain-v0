use crate::proto;
use crate::storage::util::{blake3_20, bytes_compare};
use prost::Message;
use thiserror::Error;

const MAX_DATA_BYTES: usize = 2048;

#[derive(Error, Debug, Clone)]
pub enum ValidationError {
    #[error("Message data is missing")]
    MissingData,
    #[error("Invalid message hash")]
    InvalidHash,
    #[error("Unrecognized hash scheme")]
    InvalidHashScheme,
    #[error("Message data too large")]
    InvalidDataLength,
}

pub fn validate_message(message: &proto::Message) -> Result<(), ValidationError> {
    if message.data_bytes.is_some() {
        let data_bytes = message.data_bytes.as_ref().unwrap();
        if data_bytes.len() > MAX_DATA_BYTES {
            return Err(ValidationError::InvalidDataLength);
        }
        validate_message_hash(message.hash_scheme, data_bytes, &message.hash)?;
    } else {
        if message.data.is_none() {
            return Err(ValidationError::MissingData);
        }
        let data_bytes = message.data.as_ref().unwrap().encode_to_vec();
        validate_message_hash(message.hash_scheme, &data_bytes, &message.hash)?;
    }

    Ok(())
}

fn validate_message_hash(
    hash_scheme: i32,
    data_bytes: &Vec<u8>,
    hash: &Vec<u8>,
) -> Result<(), ValidationError> {
    if hash_scheme != proto::HashScheme::Blake3 as i32 {
        return Err(ValidationError::InvalidHashScheme);
    }

    if data_bytes.len() == 0 {
        return Err(ValidationError::MissingData);
    }

    let result = blake3_20(data_bytes);
    if bytes_compare(&result, hash) != 0 {
        return Err(ValidationError::InvalidHash);
    }
    Ok(())
}
