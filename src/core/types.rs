use core::fmt;
use libp2p::bytes::Bytes;
use malachite_common;
use malachite_common::{
    Extension, NilOrVal, Round, SignedProposal, SignedProposalPart, SignedVote, VoteType,
    VotingPower,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::sync::Arc;

pub mod snapchain {
    tonic::include_proto!("snapchain");
}

use snapchain::ShardHash;

pub trait ShardId
where
    Self: Sized + Clone + Send + Sync + 'static,
{
    fn new(id: u8) -> Self;
    fn shard_id(&self) -> u8;
}

#[derive(Clone, Debug)]
pub struct SnapchainShard(u8);

impl ShardId for SnapchainShard {
    fn new(id: u8) -> Self {
        Self(id)
    }
    fn shard_id(&self) -> u8 {
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
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
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
pub struct Signature([u8; 64]);
pub type PublicKey = libp2p::identity::ed25519::PublicKey;
pub type PrivateKey = libp2p::identity::ed25519::SecretKey;

impl malachite_common::SigningScheme for Ed25519 {
    type DecodingError = InvalidSignatureError;
    type Signature = Signature;
    type PublicKey = PublicKey;
    type PrivateKey = PrivateKey;

    fn decode_signature(bytes: &[u8]) -> Result<Self::Signature, Self::DecodingError> {
        todo!()
    }

    fn encode_signature(signature: &Self::Signature) -> Vec<u8> {
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
    pub shard_index: u8,
    pub block_number: u64,
}

impl Height {
    pub const fn new(shard_index: u8, block_number: u64) -> Self {
        Self {
            shard_index,
            block_number,
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
        write!(f, "[{}] {:?}", self.shard_index, &self.hash)
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SnapchainValidator {
    pub shard_index: u8,
    pub address: Address,
    pub public_key: PublicKey,
}

impl SnapchainValidator {
    pub fn new(shard_index: SnapchainShard, public_key: PublicKey) -> Self {
        Self {
            shard_index: shard_index.shard_id(),
            address: Address(public_key.to_bytes()),
            public_key,
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

    pub fn shard_id(&self) -> u8 {
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

    pub fn to_sign_bytes(&self) -> Bytes {
        todo!()
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
    private_key: Arc<PrivateKey>,
}

impl SnapchainValidatorContext {
    pub fn new(private_key: PrivateKey) -> Self {
        Self {
            private_key: Arc::new(private_key),
        }
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
        SignedVote::new(vote, Signature([0; 64]))
    }

    fn verify_signed_vote(
        &self,
        vote: &Vote,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        false
    }

    fn sign_proposal(&self, proposal: Self::Proposal) -> SignedProposal<Self> {
        SignedProposal::new(proposal, Signature([0; 64]))
    }

    fn verify_signed_proposal(
        &self,
        proposal: &Proposal,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        // TODO
        false
    }

    fn sign_proposal_part(&self, proposal_part: Self::ProposalPart) -> SignedProposalPart<Self> {
        SignedProposalPart::new(proposal_part, Signature([0; 64]))
    }

    fn verify_signed_proposal_part(
        &self,
        proposal_part: &ProposalPart,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        false
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
        1
    }

    fn get_by_address(&self, address: &Address) -> Option<&SnapchainValidator> {
        todo!()
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
