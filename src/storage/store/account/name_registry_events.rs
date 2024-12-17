use prost::Message;

use crate::{
    core::error::HubError,
    proto::UserNameProof,
    storage::{
        constants::RootPrefix,
        db::{RocksDB, RocksDbTransactionBatch},
    },
};

use super::{get_from_db_or_txn, make_fid_key};

pub fn make_fname_username_proof_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 32);
    key.push(RootPrefix::FNameUserNameProof as u8);
    key.extend_from_slice(name);
    key
}

pub fn make_fname_username_proof_by_fid_key(fid: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4);

    key.push(RootPrefix::FNameUserNameProofByFid as u8);
    key.extend_from_slice(&make_fid_key(fid));
    key
}

pub fn get_username_proof(
    db: &RocksDB,
    txn: &mut RocksDbTransactionBatch,
    name: &[u8],
) -> Result<Option<UserNameProof>, HubError> {
    let key = make_fname_username_proof_key(name);
    let buf = get_from_db_or_txn(db, txn, &key)?;
    if buf.is_none() {
        return Ok(None);
    }

    match UserNameProof::decode(buf.unwrap().as_slice()) {
        Ok(proof) => Ok(Some(proof)),
        Err(_) => Err(HubError {
            code: "internal_error".to_string(),
            message: "could not decode username proof".to_string(),
        }),
    }
}

pub fn get_fname_proof_by_fid(db: &RocksDB, fid: u64) -> Result<Option<UserNameProof>, HubError> {
    let secondary_key = make_fname_username_proof_by_fid_key(fid);
    let primary_key = db.get(&secondary_key)?;
    if primary_key.is_none() {
        return Ok(None);
    }

    let buf = db.get(primary_key.unwrap().as_slice())?;
    if buf.is_none() {
        return Ok(None);
    }

    match UserNameProof::decode(buf.unwrap().as_slice()) {
        Ok(proof) => Ok(Some(proof)),
        Err(_) => Err(HubError {
            code: "internal_error".to_string(),
            message: "could not decode username proof".to_string(),
        }),
    }
}

pub fn put_username_proof_transaction(
    txn: &mut RocksDbTransactionBatch,
    username_proof: &UserNameProof,
) {
    let buf = username_proof.encode_to_vec();

    let primary_key = make_fname_username_proof_key(&username_proof.name);
    txn.put(primary_key.clone(), buf);

    let secondary_key = make_fname_username_proof_by_fid_key(username_proof.fid);
    txn.put(secondary_key, primary_key);
}

pub fn delete_username_proof_transaction(
    txn: &mut RocksDbTransactionBatch,
    username_proof: &UserNameProof,
    existing_fid: Option<u64>,
) {
    let buf = username_proof.encode_to_vec();

    let primary_key = make_fname_username_proof_key(&username_proof.name);
    txn.put(primary_key.clone(), buf);

    if existing_fid.is_some() {
        let secondary_key = make_fname_username_proof_by_fid_key(existing_fid.unwrap());
        txn.delete(secondary_key);
    }
}
