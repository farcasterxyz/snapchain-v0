use core::fmt;
use libp2p::identity::ed25519::Keypair;
use malachite_common;
use malachite_common::{
    Extension, NilOrVal, Round, SignedProposal, SignedProposalPart, SignedVote, Validator,
    VoteType, VotingPower,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::sync::Arc;
use tracing::warn;

pub use crate::proto::snapchain as proto; // TODO: reconsider how this is imported

use crate::proto::snapchain::full_proposal::ProposedValue;
use crate::proto::snapchain::{Block, FullProposal, ShardChunk};
use proto::ShardHash;

pub trait ShardId
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    fn new(id: u32) -> Self;
    fn shard_id(&self) -> u32;
}

#[derive(Clone, Debug)]
pub struct SnapchainShard(u32);

impl ShardId for SnapchainShard {
    fn new(id: u32) -> Self {
        Self(id)
    }
    fn shard_id(&self) -> u32 {
        self.0
    }
}

pub trait ShardedContext {
    type ShardId: ShardId;
}

pub trait SnapchainContext: malachite_common::Context + ShardedContext {}

// TODO: Should validator keys be ECDSA?
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Address(pub [u8; 32]);

impl Address {
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&vec);
        Self(bytes)
    }

    pub fn prefix(&self) -> String {
        format!("0x{}", &self.to_hex()[0..4])
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Address({})", self)
    }
}

impl malachite_common::Address for Address {}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ed25519 {}

pub struct InvalidSignatureError();
impl fmt::Display for InvalidSignatureError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid signature")
    }
}

// Ed25519 signature
// Todo: Do we need the consensus-critical version? https://github.com/penumbra-zone/ed25519-consensus
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Signature(pub Vec<u8>);
pub type PublicKey = libp2p::identity::ed25519::PublicKey;
pub type PrivateKey = libp2p::identity::ed25519::SecretKey;

impl malachite_common::SigningScheme for Ed25519 {
    type DecodingError = InvalidSignatureError;
    type Signature = Signature;
    type PublicKey = PublicKey;
    type PrivateKey = PrivateKey;

    fn decode_signature(_bytes: &[u8]) -> Result<Self::Signature, Self::DecodingError> {
        todo!()
    }

    fn encode_signature(_signature: &Self::Signature) -> Vec<u8> {
        todo!()
    }
}

// Blake3 20-byte hashes (same as Message/sync trie)
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Hash([u8; 20]);

impl Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({})", hex::encode(&self.0))
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Height {
    pub shard_index: u32,
    pub block_number: u64,
}

impl Height {
    pub const fn new(shard_index: u32, block_number: u64) -> Self {
        Self {
            shard_index,
            block_number,
        }
    }

    pub fn to_proto(&self) -> proto::Height {
        proto::Height {
            shard_index: self.shard_index,
            block_number: self.block_number,
        }
    }

    pub(crate) fn from_proto(proto: proto::Height) -> Self {
        Self {
            block_number: proto.block_number,
            shard_index: proto.shard_index,
        }
    }

    pub const fn as_u64(&self) -> u64 {
        self.block_number
    }

    pub const fn increment(&self) -> Self {
        self.increment_by(1)
    }

    pub const fn increment_by(&self, n: u64) -> Self {
        Self {
            shard_index: self.shard_index,
            block_number: self.block_number + n,
        }
    }

    pub fn decrement(&self) -> Option<Self> {
        self.block_number.checked_sub(1).map(|block_number| Self {
            shard_index: self.shard_index,
            block_number,
        })
    }
}

impl fmt::Display for Height {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.shard_index, self.block_number)
    }
}

impl malachite_common::Height for Height {
    fn increment(&self) -> Self {
        self.increment()
    }

    fn as_u64(&self) -> u64 {
        self.block_number
    }
}

// #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
// pub struct ShardHash {
//     shard_index: u8,
//     hash: Hash,
// }

impl fmt::Display for ShardHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {:?}", self.shard_index, hex::encode(&self.hash))
    }
}

// impl ShardHash {
//     pub fn new(shard_id: u8, hash: Hash) -> Self {
//         Self { shard_id, hash }
//     }
// }

impl malachite_common::Value for ShardHash {
    type Id = ShardHash;

    fn id(&self) -> Self::Id {
        self.clone()
    }
}

impl FullProposal {
    pub fn shard_hash(&self) -> ShardHash {
        match &self.proposed_value {
            Some(ProposedValue::Block(block)) => ShardHash {
                shard_index: self.height().shard_index as u32,
                hash: block.hash.clone(),
            },
            Some(ProposedValue::Shard(shard_chunk)) => ShardHash {
                shard_index: self.height().shard_index as u32,
                hash: shard_chunk.hash.clone(),
            },
            _ => {
                panic!("Invalid proposal type");
            }
        }
    }

    pub fn block(&self) -> Option<Block> {
        match &self.proposed_value {
            Some(ProposedValue::Block(block)) => Some(block.clone()),
            _ => None,
        }
    }

    pub fn shard_chunk(&self) -> Option<ShardChunk> {
        match &self.proposed_value {
            Some(ProposedValue::Shard(chunk)) => Some(chunk.clone()),
            _ => None,
        }
    }

    pub fn proposer_address(&self) -> Address {
        Address::from_vec(self.proposer.clone())
    }

    pub fn height(&self) -> Height {
        Height::from_proto(self.height.clone().unwrap())
    }

    pub fn round(&self) -> Round {
        Round::new(self.round)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapchainValidator {
    pub shard_index: u32,
    pub address: Address,
    pub public_key: PublicKey,
    pub rpc_address: Option<String>,
}

impl SnapchainValidator {
    pub fn new(
        shard_index: SnapchainShard,
        public_key: PublicKey,
        rpc_address: Option<String>,
    ) -> Self {
        Self {
            shard_index: shard_index.shard_id(),
            address: Address(public_key.to_bytes()),
            public_key,
            rpc_address,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapchainValidatorSet {
    pub validators: Vec<SnapchainValidator>,
}

impl SnapchainValidatorSet {
    pub fn new(validators: Vec<SnapchainValidator>) -> Self {
        let mut set = Self { validators: vec![] };
        for validator in validators {
            set.add(validator);
        }
        set
    }

    pub fn add(&mut self, validator: SnapchainValidator) -> bool {
        if self.exists(&validator.address) {
            return false;
        }

        if self.validators.is_empty() || self.validators[0].shard_index == validator.shard_index {
            self.validators.push(validator);
            // Ensure validators are in the same order on all nodes
            self.validators.sort();
            true
        } else {
            // TODO: This should fail loudly
            false
        }
    }

    pub fn exists(&self, address: &Address) -> bool {
        self.validators.iter().any(|v| v.address == *address)
    }

    pub fn shard_id(&self) -> u32 {
        if self.validators.is_empty() {
            0
        } else {
            self.validators[0].shard_index
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Vote {
    pub vote_type: VoteType,
    pub height: Height,
    pub round: Round,
    pub shard_hash: NilOrVal<ShardHash>,
    pub voter: Address,
    pub extension: Option<Extension>,
}

impl Vote {
    pub fn new_prevote(
        height: Height,
        round: Round,
        block_hash: NilOrVal<ShardHash>,
        voter: Address,
    ) -> Self {
        Self {
            vote_type: VoteType::Prevote,
            height,
            round,
            shard_hash: block_hash,
            voter,
            extension: None,
        }
    }

    pub fn new_precommit(
        height: Height,
        round: Round,
        value: NilOrVal<ShardHash>,
        address: Address,
    ) -> Self {
        Self {
            vote_type: VoteType::Precommit,
            height,
            round,
            shard_hash: value,
            voter: address,
            extension: None,
        }
    }

    pub fn new_precommit_with_extension(
        height: Height,
        round: Round,
        value: NilOrVal<ShardHash>,
        address: Address,
        extension: Extension,
    ) -> Self {
        Self {
            vote_type: VoteType::Precommit,
            height,
            round,
            shard_hash: value,
            voter: address,
            extension: Some(extension),
        }
    }

    pub fn to_proto(&self) -> proto::Vote {
        let vote_type = match self.vote_type {
            VoteType::Prevote => proto::VoteType::Prevote,
            VoteType::Precommit => proto::VoteType::Precommit,
        };
        let shard_hash = match &self.shard_hash {
            NilOrVal::Nil => None,
            NilOrVal::Val(shard_hash) => Some(shard_hash.clone()),
        };
        proto::Vote {
            height: Some(self.height.to_proto()),
            round: self.round.as_i64(),
            voter: self.voter.to_vec(),
            r#type: vote_type as i32,
            value: shard_hash,
        }
    }

    pub fn from_proto(proto: proto::Vote) -> Self {
        let vote_type = match proto.r#type {
            0 => VoteType::Prevote,
            1 => VoteType::Precommit,
            _ => panic!("Invalid vote type"),
        };
        let shard_hash = match proto.value {
            None => NilOrVal::Nil,
            Some(value) => NilOrVal::Val(value),
        };
        Self {
            vote_type,
            height: Height::from_proto(proto.height.unwrap()),
            round: Round::new(proto.round),
            voter: Address::from_vec(proto.voter),
            shard_hash,
            extension: None,
        }
    }

    pub fn to_sign_bytes(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal {
    pub height: Height,
    pub round: Round,
    pub shard_hash: ShardHash,
    pub pol_round: Round,
    pub proposer: Address,
}

impl Proposal {
    pub fn to_proto(&self) -> proto::Proposal {
        proto::Proposal {
            height: Some(self.height.to_proto()),
            round: self.round.as_i64(),
            proposer: self.proposer.to_vec(),
            value: Some(self.shard_hash.clone()),
            pol_round: self.pol_round.as_i64(),
        }
    }

    pub fn from_proto(proto: proto::Proposal) -> Self {
        Self {
            height: Height::from_proto(proto.height.unwrap()),
            round: Round::new(proto.round),
            shard_hash: proto.value.unwrap(),
            pol_round: Round::new(proto.pol_round),
            proposer: Address::from_vec(proto.proposer),
        }
    }
    pub fn to_sign_bytes(&self) -> Vec<u8> {
        // TODO: Should we be signing the hash?
        self.to_proto().encode_to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinglePartProposal {
    pub height: Height,
    pub proposal_round: Round,
    pub proposer: Address,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalPart {
    FullProposal(SinglePartProposal),
}

#[derive(Clone, Debug)]
pub struct SnapchainValidatorContext {
    keypair: Arc<Keypair>,
}

impl SnapchainValidatorContext {
    pub fn new(keypair: Keypair) -> Self {
        Self {
            keypair: Arc::new(keypair),
        }
    }

    pub fn public_key(&self) -> PublicKey {
        self.keypair.public()
    }
}

impl ShardedContext for SnapchainValidatorContext {
    type ShardId = SnapchainShard;
}

impl malachite_common::Context for SnapchainValidatorContext {
    type Address = Address;
    type Height = Height;
    type ProposalPart = ProposalPart;
    type Proposal = Proposal;
    type Validator = SnapchainValidator;
    type ValidatorSet = SnapchainValidatorSet;
    type Value = ShardHash;
    type Vote = Vote;
    type SigningScheme = Ed25519;

    fn select_proposer<'a>(
        &self,
        validator_set: &'a Self::ValidatorSet,
        height: Self::Height,
        round: Round,
    ) -> &'a Self::Validator {
        assert!(validator_set.validators.len() > 0);
        assert!(round != Round::Nil && round.as_i64() >= 0);

        let proposer_index = {
            let height = height.as_u64() as usize;
            let round = round.as_i64() as usize;

            (height - 1 + round) % validator_set.validators.len()
        };

        validator_set
            .validators
            .get(proposer_index)
            .expect("proposer_index is valid")
    }

    fn sign_vote(&self, vote: Self::Vote) -> SignedVote<Self> {
        let signature = self.keypair.sign(&vote.to_sign_bytes());
        SignedVote::new(vote, Signature(signature))
    }

    fn verify_signed_vote(
        &self,
        vote: &Vote,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let valid = public_key.verify(&vote.to_sign_bytes(), &signature.0);
        if !valid {
            panic!("Invalid signature");
        }
        valid
    }

    fn sign_proposal(&self, proposal: Self::Proposal) -> SignedProposal<Self> {
        let signature = self.keypair.sign(&proposal.to_sign_bytes());
        SignedProposal::new(proposal, Signature(signature))
    }

    fn verify_signed_proposal(
        &self,
        proposal: &Proposal,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let valid = public_key.verify(&proposal.to_sign_bytes(), &signature.0);
        if !valid {
            panic!("Invalid signature");
        }
        valid
    }

    fn sign_proposal_part(&self, proposal_part: Self::ProposalPart) -> SignedProposalPart<Self> {
        SignedProposalPart::new(proposal_part, Signature(vec![]))
    }

    fn verify_signed_proposal_part(
        &self,
        _proposal_part: &ProposalPart,
        _signature: &Signature,
        _public_key: &PublicKey,
    ) -> bool {
        todo!()
    }

    fn new_proposal(
        height: Height,
        round: Round,
        shard_hash: ShardHash,
        pol_round: Round,
        address: Address,
    ) -> Proposal {
        Proposal {
            height,
            round,
            shard_hash,
            pol_round,
            proposer: address,
        }
    }

    fn new_prevote(
        height: Height,
        round: Round,
        value_id: NilOrVal<ShardHash>,
        address: Address,
    ) -> Vote {
        Vote::new_prevote(height, round, value_id, address)
    }

    fn new_precommit(
        height: Height,
        round: Round,
        value_id: NilOrVal<ShardHash>,
        address: Address,
    ) -> Vote {
        Vote::new_precommit(height, round, value_id, address)
    }
}

impl SnapchainContext for SnapchainValidatorContext {}

impl malachite_common::ProposalPart<SnapchainValidatorContext> for ProposalPart {
    fn is_first(&self) -> bool {
        // Only one part for now
        true
    }

    fn is_last(&self) -> bool {
        true
    }
}

impl malachite_common::Proposal<SnapchainValidatorContext> for Proposal {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &ShardHash {
        &self.shard_hash
    }

    fn take_value(self) -> ShardHash {
        self.shard_hash
    }

    fn pol_round(&self) -> Round {
        self.pol_round
    }

    fn validator_address(&self) -> &Address {
        &self.proposer
    }
}

impl malachite_common::Vote<SnapchainValidatorContext> for Vote {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &NilOrVal<ShardHash> {
        &self.shard_hash
    }

    fn take_value(self) -> NilOrVal<ShardHash> {
        self.shard_hash
    }

    fn vote_type(&self) -> VoteType {
        self.vote_type
    }

    fn validator_address(&self) -> &Address {
        &self.voter
    }

    fn extension(&self) -> Option<&Extension> {
        None
    }

    fn extend(self, extension: Extension) -> Self {
        Self {
            extension: Some(extension),
            ..self
        }
    }
}

impl malachite_common::ValidatorSet<SnapchainValidatorContext> for SnapchainValidatorSet {
    fn count(&self) -> usize {
        self.validators.len()
    }

    fn total_voting_power(&self) -> VotingPower {
        self.validators.iter().map(|v| v.voting_power()).sum()
    }

    fn get_by_address(&self, address: &Address) -> Option<&SnapchainValidator> {
        let option = self.validators.iter().find(|v| &v.address == address);
        if option.is_none() {
            warn!("Validator not found: {}", address);
        }
        option
    }

    fn get_by_index(&self, index: usize) -> Option<&SnapchainValidator> {
        self.validators.get(index)
    }
}

impl malachite_common::Validator<SnapchainValidatorContext> for SnapchainValidator {
    fn address(&self) -> &Address {
        &self.address
    }

    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    fn voting_power(&self) -> VotingPower {
        1
    }
}
