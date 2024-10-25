use malachite_common::{
    Context, Extension, NilOrVal, Round, SignedProposal, SignedProposalPart, SignedVote, VoteType,
    VotingPower,
};
use std::sync::Arc;

use malachite_starknet_p2p_types::{
    Address, BlockHash, Height, Proposal, ProposalPart, Validator, ValidatorSet, Vote,
};
use malachite_starknet_p2p_types::{PartType, PrivateKey, PublicKey, Signature, SigningScheme};
use starknet_core::utils::starknet_keccak;

#[derive(Clone, Debug)]
pub struct SnapchainContext {
    private_key: Arc<PrivateKey>,
}

impl SnapchainContext {
    pub fn new(private_key: PrivateKey) -> Self {
        Self {
            private_key: Arc::new(private_key),
        }
    }
}

impl Context for SnapchainContext {
    type Address = Address;
    type ProposalPart = ProposalPart;
    type Height = Height;
    type Proposal = Proposal;
    type ValidatorSet = ValidatorSet;
    type Validator = Validator;
    type Value = BlockHash;
    type Vote = Vote;
    type SigningScheme = SigningScheme;

    fn sign_vote(&self, vote: Self::Vote) -> SignedVote<Self> {
        let hash = starknet_keccak(&vote.to_sign_bytes());
        let signature = self.private_key.sign(&hash);
        SignedVote::new(vote, signature)
    }

    fn verify_signed_vote(
        &self,
        vote: &Vote,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let hash = starknet_keccak(&vote.to_sign_bytes());
        public_key.verify(&hash, signature)
    }

    fn sign_proposal(&self, proposal: Self::Proposal) -> SignedProposal<Self> {
        let hash = starknet_keccak(&proposal.to_sign_bytes());
        let signature = self.private_key.sign(&hash);
        SignedProposal::new(proposal, signature)
    }

    fn verify_signed_proposal(
        &self,
        proposal: &Proposal,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let hash = starknet_keccak(&proposal.to_sign_bytes());
        public_key.verify(&hash, signature)
    }

    fn sign_proposal_part(&self, proposal_part: Self::ProposalPart) -> SignedProposalPart<Self> {
        let hash = starknet_keccak(&proposal_part.to_sign_bytes());
        let signature = self.private_key.sign(&hash);
        SignedProposalPart::new(proposal_part, signature)
    }

    fn verify_signed_proposal_part(
        &self,
        proposal_part: &ProposalPart,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let hash = starknet_keccak(&proposal_part.to_sign_bytes());
        public_key.verify(&hash, signature)
    }

    fn new_proposal(
        height: Height,
        round: Round,
        block_hash: BlockHash,
        pol_round: Round,
        address: Address,
    ) -> Proposal {
        Proposal::new(height, round, block_hash, pol_round, address)
    }

    fn new_prevote(
        height: Height,
        round: Round,
        value_id: NilOrVal<BlockHash>,
        address: Address,
    ) -> Vote {
        Vote::new_prevote(height, round, value_id, address)
    }

    fn new_precommit(
        height: Height,
        round: Round,
        value_id: NilOrVal<BlockHash>,
        address: Address,
    ) -> Vote {
        Vote::new_precommit(height, round, value_id, address)
    }

    fn select_proposer<'a>(
        &self,
        validator_set: &'a Self::ValidatorSet,
        height: Self::Height,
        round: Round,
    ) -> &'a Self::Validator {
        todo!()
    }
}

impl malachite_common::ProposalPart<SnapchainContext> for ProposalPart {
    fn is_first(&self) -> bool {
        self.part_type() == PartType::Init
    }

    fn is_last(&self) -> bool {
        self.part_type() == PartType::Fin
    }
}

impl malachite_common::Proposal<SnapchainContext> for Proposal {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &BlockHash {
        &self.block_hash
    }

    fn take_value(self) -> BlockHash {
        self.block_hash
    }

    fn pol_round(&self) -> Round {
        self.pol_round
    }

    fn validator_address(&self) -> &Address {
        &self.proposer
    }
}

impl malachite_common::Vote<SnapchainContext> for Vote {
    fn height(&self) -> Height {
        self.height
    }

    fn round(&self) -> Round {
        self.round
    }

    fn value(&self) -> &NilOrVal<BlockHash> {
        &self.block_hash
    }

    fn take_value(self) -> NilOrVal<BlockHash> {
        self.block_hash
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

impl malachite_common::ValidatorSet<SnapchainContext> for ValidatorSet {
    fn count(&self) -> usize {
        self.validators.len()
    }

    fn total_voting_power(&self) -> VotingPower {
        self.total_voting_power()
    }

    fn get_by_address(&self, address: &Address) -> Option<&Validator> {
        self.get_by_address(address)
    }

    fn get_by_index(&self, index: usize) -> Option<&Validator> {
        self.validators.get(index)
    }
}

impl malachite_common::Validator<SnapchainContext> for Validator {
    fn address(&self) -> &Address {
        &self.address
    }

    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    fn voting_power(&self) -> VotingPower {
        self.voting_power
    }
}
