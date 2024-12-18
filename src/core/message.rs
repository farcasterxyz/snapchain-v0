use alloy_dyn_abi::TypedData;
use serde_json::json;

use crate::proto;
use crate::proto::MessageType;

use super::error::HubError;

impl proto::Message {
    pub fn is_type(&self, message_type: proto::MessageType) -> bool {
        self.data.is_some() && self.data.as_ref().unwrap().r#type == message_type as i32
    }

    pub fn fid(&self) -> u64 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().fid
        } else {
            0
        }
    }

    pub fn msg_type(&self) -> MessageType {
        if self.data.is_some() {
            MessageType::try_from(self.data.as_ref().unwrap().r#type).unwrap_or(MessageType::None)
        } else {
            MessageType::None
        }
    }

    pub fn hex_hash(&self) -> String {
        hex::encode(&self.hash)
    }
}

impl proto::ValidatorMessage {
    pub fn fid(&self) -> u64 {
        if let Some(fname) = &self.fname_transfer {
            if let Some(proof) = &fname.proof {
                return proof.fid;
            }
        }
        if let Some(event) = &self.on_chain_event {
            return event.fid;
        }
        0
    }
}

impl proto::FnameTransfer {
    pub fn verify_signature(&self) -> Result<bool, HubError> {
        let proof = self.proof.as_ref().unwrap();
        let username = std::str::from_utf8(&proof.name);
        if username.is_err() {
            return Err(HubError::validation_failure(
                "could not deserialize username",
            ));
        }

        let json = json!({
          "types": {
              "EIP712Domain": [
                  {
                      "name": "name",
                      "type": "string"
                  },
                  {
                      "name": "version",
                      "type": "string"
                  },
                  {
                      "name": "chainId",
                      "type": "uint256"
                  },
                  {
                      "name": "verifyingContract",
                      "type": "address"
                  }
              ],
              "UserNameProof": [
                { "name": "name", "type": "string" },
                { "name": "timestamp", "type": "uint256" },
                { "name": "owner", "type": "address" }
              ]
            },
            "primaryType": "UserNameProof",
            "domain": {
              "name": "Farcaster name verification",
              "version": "1",
              "chainId": 1,
              "verifyingContract": "0xe3be01d99baa8db9905b33a3ca391238234b79d1" // name registry contract, will be the farcaster ENS CCIP contract later
            },
            "message": {
                "name": username.unwrap(),
                "timestamp": proof.timestamp,
                "owner": hex::encode(proof.owner.clone())
            }
        });

        let typed_data = serde_json::from_value::<TypedData>(json);
        if typed_data.is_err() {
            return Err(HubError::validation_failure(
                "could not construct typed data",
            ));
        }

        let data = typed_data.unwrap();
        let prehash = data.eip712_signing_hash();
        if prehash.is_err() {
            return Err(HubError::validation_failure(
                "could not construct hash from typed data",
            ));
        }

        if proof.signature.len() != 65 {
            return Err(HubError::validation_failure("invalid signature length"));
        }

        let hash = prehash.unwrap();
        let fname_signer = alloy::primitives::address!("Bc5274eFc266311015793d89E9B591fa46294741");
        let signature = alloy::primitives::PrimitiveSignature::from_bytes_and_parity(
            &proof.signature[0..64],
            proof.signature[64] != 0x1b,
        );
        let recovered_address = signature.recover_address_from_prehash(&hash);
        if recovered_address.is_err() {
            return Err(HubError::validation_failure("could not recover address"));
        }
        let recovered = recovered_address.unwrap();
        return Ok(recovered == fname_signer);
    }
}

#[cfg(test)]
mod tests {
    use proto::{FnameTransfer, UserNameProof};

    use super::*;

    #[test]
    fn test_fname_transfer_verify_valid_signature() {
        let transfer = &FnameTransfer{
      id: 1,
      from_fid: 1,
      proof: Some(UserNameProof{
        timestamp: 1628882891,
        name: "farcaster".into(),
        owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
        signature: hex::decode("b7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
        fid: 1,
        r#type: 1,
      })
    };
        let result = transfer.verify_signature();
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_fname_transfer_verify_wrong_address_for_signature_fails() {
        let transfer = &FnameTransfer{
      id: 1,
      from_fid: 1,
      proof: Some(UserNameProof{
        timestamp: 1628882891,
        name: "farcaster".into(),
        owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
        signature: hex::decode("a7181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
        fid: 1,
        r#type: 1,
      })
    };
        let result = transfer.verify_signature();
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_fname_transfer_verify_invalid_signature_fails() {
        let transfer = &FnameTransfer{
      id: 1,
      from_fid: 1,
      proof: Some(UserNameProof{
        timestamp: 1628882891,
        name: "farcaster".into(),
        owner: hex::decode("8773442740c17c9d0f0b87022c722f9a136206ed").unwrap(),
        signature: hex::decode("181760f14eda0028e0b647ff15f45235526ced3b4ae07fcce06141b73d32960d3253776e62f761363fb8137087192047763f4af838950a96f3885f3c2289c41b").unwrap(),
        fid: 1,
        r#type: 1,
      })
    };
        let result = transfer.verify_signature();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().message, "invalid signature length");
    }
}
