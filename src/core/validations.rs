use crate::proto;
use crate::storage::util::{blake3_20, bytes_compare};
use ed25519_dalek::{Signature, VerifyingKey};
use prost::Message;
use thiserror::Error;

const MAX_DATA_BYTES: usize = 2048;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum ValidationError {
    #[error("Message data is missing")]
    MissingData,
    #[error("Invalid message hash")]
    InvalidHash,
    #[error("Unrecognized hash scheme")]
    InvalidHashScheme,
    #[error("Message data too large")]
    InvalidDataLength,
    #[error("Unrecognized signature scheme")]
    InvalidSignatureScheme,
    #[error("Signer is empty or invalid")]
    MissingOrInvalidSigner,
    #[error("Signature is empty")]
    MissingSignature,
    #[error("Invalid message signature")]
    InvalidSignature,
}

pub fn validate_message(message: &proto::Message) -> Result<(), ValidationError> {
    let data_bytes;
    if message.data_bytes.is_some() {
        data_bytes = message.data_bytes.as_ref().unwrap().clone();
        if data_bytes.len() > MAX_DATA_BYTES {
            return Err(ValidationError::InvalidDataLength);
        }
    } else {
        if message.data.is_none() {
            return Err(ValidationError::MissingData);
        }
        data_bytes = message.data.as_ref().unwrap().encode_to_vec();
    }

    validate_message_hash(message.hash_scheme, &data_bytes, &message.hash)?;
    validate_signature(
        message.signature_scheme,
        &message.hash,
        &message.signature,
        &message.signer,
    )?;

    Ok(())
}

fn validate_signature(
    signature_scheme: i32,
    data_bytes: &Vec<u8>,
    signature: &Vec<u8>,
    signer: &Vec<u8>,
) -> Result<(), ValidationError> {
    if signature_scheme != proto::SignatureScheme::Ed25519 as i32 {
        return Err(ValidationError::InvalidSignatureScheme);
    }

    if signature.len() == 0 {
        return Err(ValidationError::MissingSignature);
    }

    let sig = Signature::from_slice(signature).map_err(|_| ValidationError::InvalidSignature)?;
    let public_key = VerifyingKey::try_from(signer.as_slice())
        .map_err(|_| ValidationError::MissingOrInvalidSigner)?;

    public_key
        .verify_strict(data_bytes.as_slice(), &sig)
        .map_err(|_| ValidationError::InvalidSignature)?;

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
