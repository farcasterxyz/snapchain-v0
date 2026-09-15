mod tests {
    use serde::Deserialize;

    use crate::{
        core::validations::{self, error::ValidationError},
        proto::{reaction_body::Target, CastId as ProtoCastId, ReactionType},
    };

    #[derive(Deserialize)]
    struct Message {
        data: MessageData,
    }

    #[derive(Deserialize)]
    struct MessageData {
        #[serde(rename = "reactionBody")]
        reaction_body: ReactionBody,
    }

    #[derive(Deserialize)]
    struct ReactionBody {
        #[serde(rename = "targetCastId")]
        target_cast_id: Option<CastId>,
        // Reactions can target a URL instead of a cast (e.g. likes on frames/links). This must be
        // deserialized too: dropping it makes a valid URL-target reaction look like it has no target,
        // which then trips `validate_reaction_body`'s `TargetIsMissing` branch. See NEYN-12728.
        #[serde(rename = "targetUrl")]
        target_url: Option<String>,
        #[serde(rename = "type")]
        reaction_type: String,
    }

    #[derive(Deserialize)]
    struct CastId {
        fid: u64,
        hash: String,
    }

    #[derive(Deserialize)]
    struct PagedResponse {
        messages: Vec<Message>,
    }

    // Committed sample of a `/v1/reactionsByFid` response. Previously this test fetched a random fid
    // from live production every run, which made it non-deterministic and flaky: it depended on prod
    // uptime, CI egress, and whatever reaction data that random fid happened to have. The fixture
    // pins a representative mix (cast-target and URL-target, LIKE and RECAST) so the test exercises
    // the validator itself rather than the network. See NEYN-12728.
    const REACTIONS_FIXTURE: &str = include_str!("testdata/reactions_by_fid.json");

    #[test]
    fn test_reaction_validation() {
        let page = serde_json::from_str::<PagedResponse>(REACTIONS_FIXTURE).unwrap();
        assert!(
            !page.messages.is_empty(),
            "fixture should contain reactions to validate"
        );
        for msg in page.messages {
            let body = msg.data.reaction_body;
            let target = match (body.target_cast_id, body.target_url) {
                (Some(cast_id), _) => Some(crate::proto::reaction_body::Target::TargetCastId(
                    crate::proto::CastId {
                        fid: cast_id.fid,
                        hash: hex::decode(cast_id.hash.replace("0x", "")).unwrap(),
                    },
                )),
                (None, Some(url)) => Some(crate::proto::reaction_body::Target::TargetUrl(url)),
                (None, None) => None,
            };
            let reaction = crate::proto::ReactionBody {
                // Map the JSON reaction-type string to its real proto enum value
                // (NONE=0, LIKE=1, RECAST=2). An earlier version hardcoded `LIKE => 0, else => 1`,
                // which mislabeled every type (LIKE validated as NONE, RECAST as LIKE) and never fed
                // RECAST to the validator at all. See #982.
                r#type: match body.reaction_type.as_str() {
                    "REACTION_TYPE_NONE" => crate::proto::ReactionType::None as i32,
                    "REACTION_TYPE_LIKE" => crate::proto::ReactionType::Like as i32,
                    "REACTION_TYPE_RECAST" => crate::proto::ReactionType::Recast as i32,
                    other => panic!("unexpected reaction type in fixture: {other}"),
                },
                target,
            };
            let result = validations::reaction::validate_reaction_body(&reaction);
            assert!(
                result.is_ok(),
                "validate_reaction_body failed: {:?}",
                result.unwrap_err()
            )
        }
    }

    fn reaction(reaction_type: ReactionType, target: Option<Target>) -> crate::proto::ReactionBody {
        crate::proto::ReactionBody {
            r#type: reaction_type as i32,
            target,
        }
    }

    #[test]
    fn test_reaction_validation_shape_space() {
        let valid_cast = || {
            Target::TargetCastId(ProtoCastId {
                fid: 1,
                hash: vec![0; 20],
            })
        };

        for reaction_type in [ReactionType::None, ReactionType::Like, ReactionType::Recast] {
            assert_eq!(
                validations::reaction::validate_reaction_body(&reaction(
                    reaction_type,
                    Some(valid_cast()),
                )),
                Ok(())
            );
            assert_eq!(
                validations::reaction::validate_reaction_body(&reaction(
                    reaction_type,
                    Some(Target::TargetUrl("x".to_string())),
                )),
                Ok(())
            );
        }

        assert_eq!(
            validations::reaction::validate_reaction_body(&reaction(
                ReactionType::Like,
                Some(Target::TargetUrl("x".repeat(256))),
            )),
            Ok(())
        );

        let invalid_cases = [
            (
                crate::proto::ReactionBody {
                    r#type: 3,
                    target: Some(Target::TargetUrl("x".to_string())),
                },
                ValidationError::InvalidReactionType,
            ),
            (
                reaction(ReactionType::Like, None),
                ValidationError::TargetIsMissing,
            ),
            (
                reaction(ReactionType::Like, Some(Target::TargetUrl(String::new()))),
                ValidationError::UrlTooShort,
            ),
            (
                reaction(ReactionType::Like, Some(Target::TargetUrl("x".repeat(257)))),
                ValidationError::UrlTooLong,
            ),
            (
                reaction(
                    ReactionType::Like,
                    Some(Target::TargetCastId(ProtoCastId {
                        fid: 1,
                        hash: vec![0; 19],
                    })),
                ),
                ValidationError::HashIsMissing,
            ),
            (
                reaction(
                    ReactionType::Like,
                    Some(Target::TargetCastId(ProtoCastId {
                        fid: 0,
                        hash: vec![0; 20],
                    })),
                ),
                ValidationError::FidIsMissing,
            ),
        ];

        for (body, expected) in invalid_cases {
            assert_eq!(
                validations::reaction::validate_reaction_body(&body),
                Err(expected)
            );
        }
    }
}
