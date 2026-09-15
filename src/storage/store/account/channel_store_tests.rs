#[cfg(test)]
mod tests {
    use crate::proto::{
        hub_event, message_data::Body, CastingMode, ChannelMemberAction, ChannelMemberBody,
        ChannelModerateAction, ChannelModerateBody, ChannelPinBody, ChannelUpdateBody, HubEvent,
        MembershipMode, Message, MessageType,
    };
    use crate::storage::constants::{RootPrefix, UserPostfix};
    use crate::storage::db::{PageOptions, RocksDB, RocksDbTransactionBatch};
    use crate::storage::store::account::{
        make_message_primary_key, make_ts_hash, message_encode, ChannelMemberState,
        ChannelMemberStore, ChannelMemberStoreDef, ChannelModerateStore, ChannelModerateStoreDef,
        ChannelModerationState, ChannelPinStore, ChannelPinStoreDef, ChannelUpdateStore,
        ChannelUpdateStoreDef, DerivedIndexGate, Store, StoreEventHandler, StoreOptions,
        CHANNEL_MEMBER_SLOT_CAP, CHANNEL_MODERATE_SLOT_CAP,
    };
    use crate::storage::trie::merkle_trie::{Context, MerkleTrie, TrieKey};
    use crate::utils::factory::messages_factory;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestStores {
        update: Store<ChannelUpdateStoreDef>,
        member: Store<ChannelMemberStoreDef>,
        pin: Store<ChannelPinStoreDef>,
        moderate: Store<ChannelModerateStoreDef>,
        db: Arc<RocksDB>,
        _temp_dir: TempDir,
    }

    fn test_stores() -> TestStores {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(RocksDB::new(
            temp_dir.path().join("channel.db").to_str().unwrap(),
        ));
        db.open().unwrap();
        let event_handler = StoreEventHandler::new();
        TestStores {
            update: ChannelUpdateStore::new(db.clone(), event_handler.clone(), 100),
            member: ChannelMemberStore::new(db.clone(), event_handler.clone(), 100),
            pin: ChannelPinStore::new(db.clone(), event_handler.clone(), 100),
            moderate: ChannelModerateStore::new(db.clone(), event_handler, 100),
            db,
            _temp_dir: temp_dir,
        }
    }

    fn channel_id(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    fn cast_hash(index: u32) -> Vec<u8> {
        let mut hash = vec![0xCC; 20];
        hash[..4].copy_from_slice(&index.to_be_bytes());
        hash
    }

    fn replace_stored_message(db: &RocksDB, message: &Message, postfix: UserPostfix) {
        let data = message.data.as_ref().unwrap();
        let ts_hash = make_ts_hash(data.timestamp, &message.hash).unwrap();
        let key = make_message_primary_key(data.fid, postfix as u8, Some(&ts_hash));
        db.put(&key, &message_encode(message)).unwrap();
    }

    fn update_message(
        author: u64,
        channel_id: Vec<u8>,
        timestamp: u32,
        name: Option<&str>,
        description: Option<&str>,
        casting_mode: Option<CastingMode>,
        membership_mode: Option<MembershipMode>,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelUpdate,
            Body::ChannelUpdateBody(ChannelUpdateBody {
                channel_id,
                name: name.map(str::to_string),
                description: description.map(str::to_string),
                casting_mode: casting_mode.map(i32::from),
                membership_mode: membership_mode.map(i32::from),
                ..Default::default()
            }),
            Some(timestamp),
            None,
        )
    }

    fn member_message(
        author: u64,
        channel_id: Vec<u8>,
        target_fid: u64,
        action: ChannelMemberAction,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelMember,
            Body::ChannelMemberBody(ChannelMemberBody {
                channel_id,
                fid: target_fid,
                action: action as i32,
            }),
            Some(timestamp),
            None,
        )
    }

    fn pin_message(
        author: u64,
        channel_id: Vec<u8>,
        cast_hash: Vec<u8>,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelPin,
            Body::ChannelPinBody(ChannelPinBody {
                channel_id,
                cast_hash,
            }),
            Some(timestamp),
            None,
        )
    }

    fn moderate_message(
        author: u64,
        channel_id: Vec<u8>,
        cast_hash: Vec<u8>,
        action: ChannelModerateAction,
        timestamp: u32,
    ) -> Message {
        messages_factory::create_message_with_data(
            author,
            MessageType::ChannelModerate,
            Body::ChannelModerateBody(ChannelModerateBody {
                channel_id,
                cast_hash,
                action: action as i32,
            }),
            Some(timestamp),
            None,
        )
    }

    fn deleted_messages(event: &HubEvent) -> &[Message] {
        match event.body.as_ref().unwrap() {
            hub_event::Body::MergeMessageBody(body) => &body.deleted_messages,
            other => panic!("expected MergeMessage, got {other:?}"),
        }
    }

    fn expected_member_state(action: ChannelMemberAction) -> ChannelMemberState {
        match action {
            ChannelMemberAction::AddMember | ChannelMemberAction::RemoveModerator => {
                ChannelMemberState::Member
            }
            ChannelMemberAction::AddModerator => ChannelMemberState::Moderator,
            ChannelMemberAction::RemoveMember | ChannelMemberAction::Unban => {
                ChannelMemberState::Removed
            }
            ChannelMemberAction::Ban => ChannelMemberState::Banned,
            ChannelMemberAction::None => panic!("none is not a state transition"),
        }
    }

    #[test]
    fn channel_slot_key_layouts_are_pinned() {
        let channel = channel_id(0x44);
        let moderated_cast = cast_hash(7);
        let mut update = vec![RootPrefix::Channel as u8, 1];
        update.extend_from_slice(&channel);
        assert_eq!(ChannelUpdateStore::slot_key(&channel), update);

        let mut member = vec![RootPrefix::Channel as u8, 2];
        member.extend_from_slice(&channel);
        member.extend_from_slice(&123u32.to_be_bytes());
        assert_eq!(ChannelMemberStore::slot_key(&channel, 123).unwrap(), member);

        let mut pin = vec![RootPrefix::Channel as u8, 3];
        pin.extend_from_slice(&channel);
        assert_eq!(ChannelPinStore::slot_key(&channel), pin);

        let mut moderate = vec![RootPrefix::Channel as u8, 4];
        moderate.extend_from_slice(&channel);
        moderate.extend_from_slice(&moderated_cast);
        assert_eq!(
            ChannelModerateStore::slot_key(&channel, &moderated_cast),
            moderate
        );
    }

    #[test]
    fn member_by_fid_index_is_gated_and_shared_with_channel_reads() {
        let stores = test_stores();
        let target_fid = 77;
        let pre_gate_channel = channel_id(0x10);
        let active_channel = channel_id(0x20);

        let mut txn = RocksDbTransactionBatch::new();
        ChannelMemberStore::merge(
            &stores.member,
            &member_message(
                1,
                pre_gate_channel.clone(),
                target_fid,
                ChannelMemberAction::AddMember,
                1,
            ),
            &mut txn,
            DerivedIndexGate::Skip,
        )
        .unwrap();
        stores.db.commit(txn).unwrap();

        let pre_gate_index =
            ChannelMemberStoreDef::make_member_by_fid_key(target_fid, &pre_gate_channel).unwrap();
        assert!(stores.db.get(&pre_gate_index).unwrap().is_none());
        assert!(ChannelMemberStore::memberships_by_fid(
            &stores.member,
            target_fid,
            &PageOptions::default(),
        )
        .unwrap()
        .entries
        .is_empty());

        let mut txn = RocksDbTransactionBatch::new();
        ChannelMemberStore::merge(
            &stores.member,
            &member_message(
                2,
                active_channel.clone(),
                target_fid,
                ChannelMemberAction::AddModerator,
                2,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        stores.db.commit(txn).unwrap();

        let active_index =
            ChannelMemberStoreDef::make_member_by_fid_key(target_fid, &active_channel).unwrap();
        assert_eq!(stores.db.get(&active_index).unwrap(), Some(Vec::new()));
        assert_eq!(
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                target_fid,
                &PageOptions::default(),
            )
            .unwrap()
            .entries,
            vec![crate::storage::store::account::ChannelMembershipEntry {
                channel_id: active_channel.clone(),
                state: ChannelMemberState::Moderator,
            }]
        );
        assert_eq!(
            ChannelMemberStore::members_by_channel(
                &stores.member,
                &active_channel,
                Some(ChannelMemberState::Moderator),
                &PageOptions::default(),
            )
            .unwrap()
            .entries[0]
                .last_action_ts,
            2
        );
    }

    #[test]
    fn channel_read_pagination_round_trips_tokens_and_terminates_after_boundary_page() {
        let stores = test_stores();
        let members_channel = channel_id(0x30);
        let memberships_channels = [channel_id(0x40), channel_id(0x41)];
        let memberships_fid = 30;
        let moderated_hashes = [cast_hash(1), cast_hash(2)];
        let mut txn = RocksDbTransactionBatch::new();

        for (timestamp, fid) in [(1, 10), (2, 20)] {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    members_channel.clone(),
                    fid,
                    ChannelMemberAction::AddMember,
                    timestamp,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        for (index, channel) in memberships_channels.iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    channel.clone(),
                    memberships_fid,
                    ChannelMemberAction::AddMember,
                    index as u32 + 3,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        for (index, moderated_hash) in moderated_hashes.iter().enumerate() {
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    1,
                    members_channel.clone(),
                    moderated_hash.clone(),
                    ChannelModerateAction::Hide,
                    index as u32 + 5,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        stores.db.commit(txn).unwrap();

        let members_first = ChannelMemberStore::members_by_channel(
            &stores.member,
            &members_channel,
            None,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(members_first.entries.len(), 1);
        assert_eq!(members_first.entries[0].fid, 10);
        assert!(members_first.next_page_token.is_some());
        let members_second = ChannelMemberStore::members_by_channel(
            &stores.member,
            &members_channel,
            None,
            &PageOptions {
                page_size: Some(1),
                page_token: members_first.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(members_second.entries.len(), 1);
        assert_eq!(members_second.entries[0].fid, 20);
        assert!(members_second.next_page_token.is_some());
        let members_boundary = ChannelMemberStore::members_by_channel(
            &stores.member,
            &members_channel,
            None,
            &PageOptions {
                page_size: Some(1),
                page_token: members_second.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(members_boundary.entries.is_empty());
        assert_eq!(members_boundary.next_page_token, None);

        let moderations_first = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &members_channel,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(moderations_first.entries.len(), 1);
        assert_eq!(moderations_first.entries[0].cast_hash, moderated_hashes[0]);
        assert!(moderations_first.next_page_token.is_some());
        let moderations_second = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &members_channel,
            &PageOptions {
                page_size: Some(1),
                page_token: moderations_first.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(moderations_second.entries.len(), 1);
        assert_eq!(moderations_second.entries[0].cast_hash, moderated_hashes[1]);
        assert!(moderations_second.next_page_token.is_some());
        let moderations_boundary = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &members_channel,
            &PageOptions {
                page_size: Some(1),
                page_token: moderations_second.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(moderations_boundary.entries.is_empty());
        assert_eq!(moderations_boundary.next_page_token, None);

        let memberships_first = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            memberships_fid,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(memberships_first.entries.len(), 1);
        assert_eq!(
            memberships_first.entries[0].channel_id,
            memberships_channels[0]
        );
        assert!(memberships_first.next_page_token.is_some());
        let memberships_second = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            memberships_fid,
            &PageOptions {
                page_size: Some(1),
                page_token: memberships_first.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(memberships_second.entries.len(), 1);
        assert_eq!(
            memberships_second.entries[0].channel_id,
            memberships_channels[1]
        );
        assert!(memberships_second.next_page_token.is_some());
        let memberships_boundary = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            memberships_fid,
            &PageOptions {
                page_size: Some(1),
                page_token: memberships_second.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(memberships_boundary.entries.is_empty());
        assert_eq!(memberships_boundary.next_page_token, None);
    }

    #[test]
    fn production_slot_caps_are_the_ones_wired_into_the_stores() {
        // The cap BEHAVIOR is exercised at a shrunken cap via
        // `StoreOptions::channel_slot_cap_override`, because filling 8k/16k rows took
        // ~80s. That trade is sound — the `count >= cap` check is cap-value
        // independent — but it leaves nothing proving the production constants are
        // what `define_channel_store!` actually wires in. Asserting the constants
        // alone would not: the override expression is the only path under test.
        let stores = test_stores();
        assert_eq!(
            ChannelMemberStore::slot_cap(&stores.member),
            Some(CHANNEL_MEMBER_SLOT_CAP)
        );
        assert_eq!(
            ChannelModerateStore::slot_cap(&stores.moderate),
            Some(CHANNEL_MODERATE_SLOT_CAP)
        );
        // Update and pin are single-slot per channel and must stay uncapped, with or
        // without an override in play — `$default_slot_cap.map(..)` is what keeps an
        // override from inventing a cap where production has none.
        assert_eq!(ChannelUpdateStore::slot_cap(&stores.update), None);
        assert_eq!(ChannelPinStore::slot_cap(&stores.pin), None);

        let temp_dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(RocksDB::new(
            temp_dir.path().join("override.db").to_str().unwrap(),
        ));
        db.open().unwrap();
        let event_handler = StoreEventHandler::new();
        let opts = StoreOptions {
            channel_slot_cap_override: Some(24),
            ..Default::default()
        };
        assert_eq!(
            ChannelMemberStore::slot_cap(&ChannelMemberStore::new_with_opts(
                db.clone(),
                event_handler.clone(),
                100,
                opts.clone(),
            )),
            Some(24)
        );
        assert_eq!(
            ChannelModerateStore::slot_cap(&ChannelModerateStore::new_with_opts(
                db.clone(),
                event_handler.clone(),
                100,
                opts.clone(),
            )),
            Some(24)
        );
        assert_eq!(
            ChannelUpdateStore::slot_cap(&ChannelUpdateStore::new_with_opts(
                db.clone(),
                event_handler.clone(),
                100,
                opts.clone(),
            )),
            None,
            "an override must not introduce a cap on an uncapped store"
        );
        assert_eq!(
            ChannelPinStore::slot_cap(&ChannelPinStore::new_with_opts(
                db,
                event_handler,
                100,
                opts,
            )),
            None,
            "an override must not introduce a cap on an uncapped store"
        );
    }

    #[test]
    fn channel_read_paths_fail_loud_on_corrupt_index_state() {
        // These enumerators deliberately hard-error on malformed state instead of
        // skipping the row, so a corrupt replica is visible rather than quietly
        // under-reporting. Nothing exercised those branches, which means a
        // regression back to `continue` / `None` would be invisible — the reads
        // would simply return fewer rows and look healthy.
        let stores = test_stores();
        let channel = channel_id(0x80);
        let target_fid = 55;
        let mut txn = RocksDbTransactionBatch::new();
        ChannelMemberStore::merge(
            &stores.member,
            &member_message(
                1,
                channel.clone(),
                target_fid,
                ChannelMemberAction::AddMember,
                1,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        stores.db.commit(txn).unwrap();
        // Baseline: the healthy reads work, so the failures below are caused by the
        // injected corruption and not by an empty store.
        assert_eq!(
            ChannelMemberStore::members_by_channel(
                &stores.member,
                &channel,
                None,
                &PageOptions::default(),
            )
            .unwrap()
            .entries
            .len(),
            1
        );

        // A by-fid index entry whose member slot does not exist. This is the anomaly
        // the index can actually develop, since it is maintained separately from the
        // slot and lives outside the trie.
        let orphan_channel = channel_id(0x81);
        let dangling =
            ChannelMemberStoreDef::make_member_by_fid_key(target_fid, &orphan_channel).unwrap();
        stores.db.put(&dangling, &[]).unwrap();
        let error = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            target_fid,
            &PageOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, "invalid_internal_state");
        assert!(
            error.message.contains("missing slot"),
            "unexpected message: {}",
            error.message
        );
        stores.db.del(&dangling).unwrap();

        // Wrong-width keys inside each prefix. Truncating a real key by one byte keeps
        // it inside the scan range while breaking the length invariant the callback
        // relies on to slice out the fid / channel_id.
        let mut short_by_fid =
            ChannelMemberStoreDef::make_member_by_fid_key(target_fid, &channel).unwrap();
        short_by_fid.pop();
        stores.db.put(&short_by_fid, &[]).unwrap();
        assert_eq!(
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                target_fid,
                &PageOptions::default(),
            )
            .unwrap_err()
            .code,
            "invalid_internal_state"
        );
        stores.db.del(&short_by_fid).unwrap();

        let mut short_slot = ChannelMemberStore::slot_key(&channel, target_fid).unwrap();
        short_slot.pop();
        stores.db.put(&short_slot, &[]).unwrap();
        assert_eq!(
            ChannelMemberStore::members_by_channel(
                &stores.member,
                &channel,
                None,
                &PageOptions::default(),
            )
            .unwrap_err()
            .code,
            "invalid_internal_state"
        );
        stores.db.del(&short_slot).unwrap();

        let mut short_moderate = ChannelModerateStore::slot_key(&channel, &cast_hash(1));
        short_moderate.pop();
        stores.db.put(&short_moderate, &[]).unwrap();
        assert_eq!(
            ChannelModerateStore::moderations_by_channel(
                &stores.moderate,
                &channel,
                &PageOptions::default(),
            )
            .unwrap_err()
            .code,
            "invalid_internal_state"
        );
        stores.db.del(&short_moderate).unwrap();

        // With the corruption removed the reads recover, so the errors above are not
        // a permanently wedged store.
        assert_eq!(
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                target_fid,
                &PageOptions::default(),
            )
            .unwrap()
            .entries
            .len(),
            1
        );
    }

    #[test]
    fn channel_read_paths_tag_unparseable_stored_messages_as_internal_state() {
        let stores = test_stores();
        let channel = channel_id(0x82);
        let target_fid = 56;
        let moderated_cast = cast_hash(2);
        let member = member_message(
            1,
            channel.clone(),
            target_fid,
            ChannelMemberAction::AddMember,
            1,
        );
        let moderate = moderate_message(
            1,
            channel.clone(),
            moderated_cast.clone(),
            ChannelModerateAction::Hide,
            2,
        );
        let mut txn = RocksDbTransactionBatch::new();
        ChannelMemberStore::merge(&stores.member, &member, &mut txn, DerivedIndexGate::Write)
            .unwrap();
        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate,
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        stores.db.commit(txn).unwrap();

        let mut corrupt_member = member.clone();
        corrupt_member.data_bytes = None;
        match corrupt_member.data.as_mut().unwrap().body.as_mut().unwrap() {
            Body::ChannelMemberBody(body) => body.action = 9999,
            _ => unreachable!(),
        }
        replace_stored_message(
            &stores.db,
            &corrupt_member,
            UserPostfix::ChannelMemberMessage,
        );
        for error in [
            ChannelMemberStore::member_state(&stores.member, &channel, target_fid, None)
                .unwrap_err(),
            ChannelMemberStore::member(&stores.member, &channel, target_fid, None).unwrap_err(),
            ChannelMemberStore::members_by_channel(
                &stores.member,
                &channel,
                None,
                &PageOptions::default(),
            )
            .unwrap_err(),
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                target_fid,
                &PageOptions::default(),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.code, "invalid_internal_state");
            assert_eq!(error.message, "invalid channel member action");
        }

        let mut corrupt_moderation = moderate.clone();
        corrupt_moderation.data_bytes = None;
        match corrupt_moderation
            .data
            .as_mut()
            .unwrap()
            .body
            .as_mut()
            .unwrap()
        {
            Body::ChannelModerateBody(body) => body.action = 9999,
            _ => unreachable!(),
        }
        replace_stored_message(
            &stores.db,
            &corrupt_moderation,
            UserPostfix::ChannelModerateMessage,
        );
        for error in [
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                None,
            )
            .unwrap_err(),
            ChannelModerateStore::moderations_by_channel(
                &stores.moderate,
                &channel,
                &PageOptions::default(),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.code, "invalid_internal_state");
            assert_eq!(error.message, "invalid channel moderate action");
        }

        corrupt_moderation.data.as_mut().unwrap().body =
            Some(Body::ChannelPinBody(ChannelPinBody {
                channel_id: channel.clone(),
                cast_hash: moderated_cast.clone(),
            }));
        replace_stored_message(
            &stores.db,
            &corrupt_moderation,
            UserPostfix::ChannelModerateMessage,
        );
        for error in [
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                None,
            )
            .unwrap_err(),
            ChannelModerateStore::moderations_by_channel(
                &stores.moderate,
                &channel,
                &PageOptions::default(),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.code, "invalid_internal_state");
            assert_eq!(error.message, "invalid ChannelModerate body");
        }
    }

    #[test]
    fn channel_reads_page_in_reverse_with_the_same_token_contract() {
        // `reverse` is plumbed from the request through `channel_page_options` into
        // every one of these reads, and the two directions use ASYMMETRIC bounds in
        // `get_iterator_options`: forward sets `lower_bound = token ++ [0]`, reverse
        // sets `upper_bound = token` exactly. All three enumerators return the raw
        // last key as their token, which is correct for both — but only because of
        // that asymmetry, so it needs pinning rather than assuming.
        let stores = test_stores();
        let channel = channel_id(0x50);
        let membership_channels = [channel_id(0x70), channel_id(0x71)];
        let membership_fid = 90;
        let moderated = [cast_hash(11), cast_hash(12)];
        let mut txn = RocksDbTransactionBatch::new();

        for (index, fid) in [40u64, 41, 42].into_iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    channel.clone(),
                    fid,
                    ChannelMemberAction::AddMember,
                    index as u32 + 1,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        for (index, membership_channel) in membership_channels.iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    membership_channel.clone(),
                    membership_fid,
                    ChannelMemberAction::AddMember,
                    index as u32 + 10,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        for (index, moderated_hash) in moderated.iter().enumerate() {
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    1,
                    channel.clone(),
                    moderated_hash.clone(),
                    ChannelModerateAction::Hide,
                    index as u32 + 20,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        stores.db.commit(txn).unwrap();

        let reverse_page = |page_token: Option<Vec<u8>>| PageOptions {
            page_size: Some(1),
            page_token,
            reverse: true,
        };

        // Members descend by fid, and every row is visited exactly once.
        let mut seen_fids = Vec::new();
        let mut token = None;
        for _ in 0..3 {
            let page = ChannelMemberStore::members_by_channel(
                &stores.member,
                &channel,
                None,
                &reverse_page(token),
            )
            .unwrap();
            assert_eq!(page.entries.len(), 1);
            seen_fids.push(page.entries[0].fid);
            token = page.next_page_token;
        }
        assert_eq!(seen_fids, vec![42, 41, 40]);
        // Forward over the same rows is the exact mirror, so neither direction is
        // silently dropping or repeating the boundary row.
        let forward = ChannelMemberStore::members_by_channel(
            &stores.member,
            &channel,
            None,
            &PageOptions {
                page_size: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
        seen_fids.reverse();
        assert_eq!(
            forward
                .entries
                .iter()
                .map(|entry| entry.fid)
                .collect::<Vec<_>>(),
            seen_fids
        );

        let moderations = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &channel,
            &reverse_page(None),
        )
        .unwrap();
        assert_eq!(moderations.entries[0].cast_hash, moderated[1]);
        let moderations_next = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &channel,
            &reverse_page(moderations.next_page_token),
        )
        .unwrap();
        assert_eq!(moderations_next.entries[0].cast_hash, moderated[0]);

        let memberships = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            membership_fid,
            &reverse_page(None),
        )
        .unwrap();
        assert_eq!(memberships.entries[0].channel_id, membership_channels[1]);
        let memberships_next = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            membership_fid,
            &reverse_page(memberships.next_page_token),
        )
        .unwrap();
        assert_eq!(
            memberships_next.entries[0].channel_id,
            membership_channels[0]
        );
    }

    #[test]
    fn out_of_prefix_page_tokens_are_rejected_instead_of_widening_the_scan() {
        // A page token is a raw RocksDB key that REPLACES the prefix as the scan's
        // lower bound, and the enumerators identify a row by key length alone —
        // lengths every channel shares. So a token minted for a lower-sorting
        // channel would otherwise return that channel's rows under the requested
        // channel id. An empty token is the same defect from the front of the
        // keyspace. Both must fail as caller error, not as a store fault.
        let stores = test_stores();
        let lower_channel = channel_id(0x10);
        let higher_channel = channel_id(0x20);
        let mut txn = RocksDbTransactionBatch::new();

        for (channel, fid, timestamp) in [
            (&lower_channel, 10, 1),
            (&lower_channel, 20, 2),
            (&higher_channel, 30, 3),
        ] {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    channel.clone(),
                    fid,
                    ChannelMemberAction::AddMember,
                    timestamp,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        for (index, moderated_hash) in [cast_hash(1), cast_hash(2)].iter().enumerate() {
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    1,
                    lower_channel.clone(),
                    moderated_hash.clone(),
                    ChannelModerateAction::Hide,
                    index as u32 + 4,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        stores.db.commit(txn).unwrap();

        // Mint a real token inside the lower channel, then aim it at the higher one.
        let lower_first = ChannelMemberStore::members_by_channel(
            &stores.member,
            &lower_channel,
            None,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(lower_first.entries[0].fid, 10);
        let foreign_member_token = lower_first.next_page_token.clone().unwrap();

        let leaked = ChannelMemberStore::members_by_channel(
            &stores.member,
            &higher_channel,
            None,
            &PageOptions {
                page_size: Some(10),
                page_token: Some(foreign_member_token.clone()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(leaked.code, "bad_request.invalid_param");

        // Same token in reverse: there it lands on the upper bound, so an
        // unguarded scan runs past the requested channel instead of before it.
        let leaked_reversed = ChannelMemberStore::members_by_channel(
            &stores.member,
            &higher_channel,
            None,
            &PageOptions {
                page_size: Some(10),
                page_token: Some(foreign_member_token),
                reverse: true,
            },
        )
        .unwrap_err();
        assert_eq!(leaked_reversed.code, "bad_request.invalid_param");

        let lower_moderations = ChannelModerateStore::moderations_by_channel(
            &stores.moderate,
            &lower_channel,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        let foreign_moderate_token = lower_moderations.next_page_token.unwrap();
        assert_eq!(
            ChannelModerateStore::moderations_by_channel(
                &stores.moderate,
                &higher_channel,
                &PageOptions {
                    page_size: Some(10),
                    page_token: Some(foreign_moderate_token),
                    ..Default::default()
                },
            )
            .unwrap_err()
            .code,
            "bad_request.invalid_param"
        );

        // memberships_by_fid is keyed by fid, so the foreign cursor is another fid's.
        let fid_ten_page = ChannelMemberStore::memberships_by_fid(
            &stores.member,
            10,
            &PageOptions {
                page_size: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(fid_ten_page.entries[0].channel_id, lower_channel);
        assert_eq!(
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                30,
                &PageOptions {
                    page_size: Some(10),
                    page_token: fid_ten_page.next_page_token,
                    ..Default::default()
                },
            )
            .unwrap_err()
            .code,
            "bad_request.invalid_param"
        );

        // An empty token never reaches the store from the RPC layer, which maps it
        // to None. Reject it here too rather than scanning from key zero.
        for empty_token_result in [
            ChannelMemberStore::members_by_channel(
                &stores.member,
                &higher_channel,
                None,
                &PageOptions {
                    page_size: Some(10),
                    page_token: Some(vec![]),
                    ..Default::default()
                },
            )
            .map(|page| page.entries.len()),
            ChannelModerateStore::moderations_by_channel(
                &stores.moderate,
                &lower_channel,
                &PageOptions {
                    page_size: Some(10),
                    page_token: Some(vec![]),
                    ..Default::default()
                },
            )
            .map(|page| page.entries.len()),
            ChannelMemberStore::memberships_by_fid(
                &stores.member,
                30,
                &PageOptions {
                    page_size: Some(10),
                    page_token: Some(vec![]),
                    ..Default::default()
                },
            )
            .map(|page| page.entries.len()),
        ] {
            assert_eq!(
                empty_token_result.unwrap_err().code,
                "bad_request.invalid_param"
            );
        }

        // A token that does belong to the requested prefix still pages normally.
        let higher_page = ChannelMemberStore::members_by_channel(
            &stores.member,
            &lower_channel,
            None,
            &PageOptions {
                page_size: Some(10),
                page_token: lower_first.next_page_token,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            higher_page
                .entries
                .iter()
                .map(|entry| entry.fid)
                .collect::<Vec<_>>(),
            vec![20]
        );
    }

    #[test]
    fn cross_author_same_block_supersede_keeps_store_index_trie_and_event_in_agreement() {
        let stores = test_stores();
        let mut trie = MerkleTrie::new().unwrap();
        trie.initialize(&stores.db).unwrap();
        let ctx = Context::new();
        let channel = channel_id(1);
        let incumbent = update_message(
            100,
            channel.clone(),
            500,
            Some("incumbent"),
            None,
            None,
            None,
        );
        let replacement = update_message(
            200,
            channel.clone(),
            100,
            Some("replacement"),
            None,
            None,
            None,
        );

        let mut txn = RocksDbTransactionBatch::new();
        let incumbent_event = ChannelUpdateStore::merge(
            &stores.update,
            &incumbent,
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        trie.update_for_event(&ctx, &stores.db, &incumbent_event, &mut txn)
            .unwrap();
        let replacement_event = ChannelUpdateStore::merge(
            &stores.update,
            &replacement,
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        trie.update_for_event(&ctx, &stores.db, &replacement_event, &mut txn)
            .unwrap();
        assert_eq!(deleted_messages(&replacement_event), &[incumbent.clone()]);
        stores.db.commit(txn).unwrap();

        assert!(stores.update.get_add(&incumbent, None).unwrap().is_none());
        assert_eq!(
            stores.update.get_add(&replacement, None).unwrap(),
            Some(replacement.clone())
        );
        let slot_pointer = stores
            .db
            .get(&ChannelUpdateStore::slot_key(&channel))
            .unwrap()
            .unwrap();
        assert_eq!(slot_pointer.len(), 28);
        assert_eq!(
            &slot_pointer[..4],
            &(replacement.fid() as u32).to_be_bytes()
        );
        assert_eq!(
            ChannelUpdateStore::get_channel_update(&stores.update, &channel, None)
                .unwrap()
                .unwrap()
                .body
                .name,
            Some("replacement".to_string())
        );

        let incumbent_key = TrieKey::for_message(&incumbent).pop().unwrap();
        let replacement_key = TrieKey::for_message(&replacement).pop().unwrap();
        assert!(!trie.exists(&ctx, &stores.db, &incumbent_key).unwrap());
        assert!(trie.exists(&ctx, &stores.db, &replacement_key).unwrap());
    }

    #[test]
    fn every_slot_type_uses_merge_order_not_timestamp_and_supersedes_both_authors() {
        let stores = test_stores();
        let channel = channel_id(2);
        let mut txn = RocksDbTransactionBatch::new();

        let update_a = update_message(1, channel.clone(), 900, Some("a"), None, None, None);
        let update_b = update_message(2, channel.clone(), 100, Some("b"), None, None, None);
        let update_a_again =
            update_message(1, channel.clone(), 50, Some("a-again"), None, None, None);
        ChannelUpdateStore::merge(&stores.update, &update_a, &mut txn, DerivedIndexGate::Write)
            .unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelUpdateStore::merge(
                    &stores.update,
                    &update_b,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[update_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelUpdateStore::merge(
                    &stores.update,
                    &update_a_again,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[update_b]
        );

        let member_a = member_message(1, channel.clone(), 99, ChannelMemberAction::AddMember, 900);
        let member_b = member_message(2, channel.clone(), 99, ChannelMemberAction::Ban, 100);
        let member_a_again = member_message(1, channel.clone(), 99, ChannelMemberAction::Unban, 50);
        ChannelMemberStore::merge(&stores.member, &member_a, &mut txn, DerivedIndexGate::Write)
            .unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelMemberStore::merge(
                    &stores.member,
                    &member_b,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[member_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelMemberStore::merge(
                    &stores.member,
                    &member_a_again,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[member_b]
        );

        let pin_a = pin_message(1, channel.clone(), cast_hash(1), 900);
        let pin_b = pin_message(2, channel.clone(), cast_hash(2), 100);
        let pin_a_again = pin_message(1, channel.clone(), Vec::new(), 50);
        ChannelPinStore::merge(&stores.pin, &pin_a, &mut txn, DerivedIndexGate::Write).unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelPinStore::merge(&stores.pin, &pin_b, &mut txn, DerivedIndexGate::Write)
                    .unwrap()
            ),
            &[pin_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelPinStore::merge(
                    &stores.pin,
                    &pin_a_again,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[pin_b]
        );

        let moderated_cast = cast_hash(3);
        let moderate_a = moderate_message(
            1,
            channel.clone(),
            moderated_cast.clone(),
            ChannelModerateAction::Hide,
            900,
        );
        let moderate_b = moderate_message(
            2,
            channel.clone(),
            moderated_cast.clone(),
            ChannelModerateAction::Unhide,
            100,
        );
        let moderate_a_again =
            moderate_message(1, channel, moderated_cast, ChannelModerateAction::Hide, 50);
        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_a,
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        assert_eq!(
            deleted_messages(
                &ChannelModerateStore::merge(
                    &stores.moderate,
                    &moderate_b,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[moderate_a]
        );
        assert_eq!(
            deleted_messages(
                &ChannelModerateStore::merge(
                    &stores.moderate,
                    &moderate_a_again,
                    &mut txn,
                    DerivedIndexGate::Write
                )
                .unwrap()
            ),
            &[moderate_b]
        );
    }

    #[test]
    fn channel_update_is_whole_replace_and_unset_modes_fold_restrictively() {
        let stores = test_stores();
        let channel = channel_id(3);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelUpdateStore::merge(
            &stores.update,
            &update_message(
                1,
                channel.clone(),
                1,
                Some("first"),
                Some("must not survive"),
                Some(CastingMode::Everyone),
                Some(MembershipMode::Open),
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        ChannelUpdateStore::merge(
            &stores.update,
            &update_message(1, channel.clone(), 2, Some("second"), None, None, None),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();

        let state = ChannelUpdateStore::get_channel_update(&stores.update, &channel, Some(&txn))
            .unwrap()
            .unwrap();
        assert_eq!(state.body.name, Some("second".to_string()));
        assert_eq!(state.body.description, None);
        assert_eq!(state.body.casting_mode, None);
        assert_eq!(state.body.membership_mode, None);
        assert_eq!(state.casting_mode, CastingMode::MembersOnly);
        assert_eq!(state.membership_mode, MembershipMode::Approval);
    }

    #[test]
    fn member_state_machine_applies_all_six_actions_from_every_prior_state() {
        let stores = test_stores();
        let channel = channel_id(4);
        let actions = [
            ChannelMemberAction::AddMember,
            ChannelMemberAction::AddModerator,
            ChannelMemberAction::RemoveModerator,
            ChannelMemberAction::RemoveMember,
            ChannelMemberAction::Ban,
            ChannelMemberAction::Unban,
        ];
        let prior_actions = [
            None,
            Some(ChannelMemberAction::AddMember),
            Some(ChannelMemberAction::AddModerator),
            Some(ChannelMemberAction::RemoveMember),
            Some(ChannelMemberAction::Ban),
        ];
        let mut txn = RocksDbTransactionBatch::new();
        let mut target_fid = 1_000;
        let mut timestamp = 1;
        for prior_action in prior_actions {
            for action in actions {
                if let Some(prior_action) = prior_action {
                    ChannelMemberStore::merge(
                        &stores.member,
                        &member_message(1, channel.clone(), target_fid, prior_action, timestamp),
                        &mut txn,
                        DerivedIndexGate::Write,
                    )
                    .unwrap();
                    timestamp += 1;
                }
                ChannelMemberStore::merge(
                    &stores.member,
                    &member_message(1, channel.clone(), target_fid, action, timestamp),
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap();
                timestamp += 1;
                assert_eq!(
                    ChannelMemberStore::member_state(
                        &stores.member,
                        &channel,
                        target_fid,
                        Some(&txn),
                    )
                    .unwrap(),
                    Some(expected_member_state(action)),
                    "prior={prior_action:?}, action={action:?}"
                );
                target_fid += 1;
            }
        }
    }

    #[test]
    fn member_churn_is_row_neutral_and_live_moderator_count_tracks_transitions() {
        let stores = test_stores();
        let channel = channel_id(5);
        let target_fid = 55;
        let mut txn = RocksDbTransactionBatch::new();
        let sequence = [
            (ChannelMemberAction::AddMember, 0),
            (ChannelMemberAction::AddModerator, 1),
            (ChannelMemberAction::RemoveModerator, 0),
            (ChannelMemberAction::AddModerator, 1),
            (ChannelMemberAction::RemoveMember, 0),
            (ChannelMemberAction::AddMember, 0),
            (ChannelMemberAction::Ban, 0),
            (ChannelMemberAction::Unban, 0),
            (ChannelMemberAction::AddModerator, 1),
        ];
        for (index, (action, expected_moderators)) in sequence.into_iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(1, channel.clone(), target_fid, action, index as u32 + 1),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
            assert_eq!(
                ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
                1
            );
            assert_eq!(
                ChannelMemberStore::live_moderator_count(&stores.member, &channel, Some(&txn),)
                    .unwrap(),
                expected_moderators
            );
        }
    }

    #[test]
    fn pin_unpin_and_moderate_hide_unhide_fold_in_place() {
        let stores = test_stores();
        let channel = channel_id(6);
        let moderated_cast = cast_hash(9);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelPinStore::merge(
            &stores.pin,
            &pin_message(1, channel.clone(), cast_hash(8), 1),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        ChannelPinStore::merge(
            &stores.pin,
            &pin_message(1, channel.clone(), Vec::new(), 2),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        assert_eq!(
            ChannelPinStore::get_channel_pin(&stores.pin, &channel, Some(&txn))
                .unwrap()
                .unwrap()
                .cast_hash,
            Vec::<u8>::new()
        );

        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                channel.clone(),
                moderated_cast.clone(),
                ChannelModerateAction::Hide,
                3,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        assert_eq!(
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelModerationState::Hidden)
        );
        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                channel.clone(),
                moderated_cast.clone(),
                ChannelModerateAction::Unhide,
                4,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();
        assert_eq!(
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &channel,
                &moderated_cast,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelModerationState::Visible)
        );
        assert_eq!(
            ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
            1
        );
    }

    #[test]
    fn pin_slot_merge_enforces_the_cast_hash_width_itself() {
        // Snapshot bootstrap and replication replay reach merge_slot without running
        // the stateless body validators, so the store has to hold the same width rule
        // as `validate_channel_pin_body` rather than trusting its callers.
        let stores = test_stores();
        let channel = channel_id(0x66);

        for bad_hash in [vec![0xAB; 19], vec![0xAB; 21], vec![0xAB; 32]] {
            let mut txn = RocksDbTransactionBatch::new();
            let error = ChannelPinStore::merge(
                &stores.pin,
                &pin_message(1, channel.clone(), bad_hash.clone(), 1),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap_err();
            assert_eq!(error.code, "bad_request.validation_failure");
            assert!(
                txn.batch.is_empty(),
                "a rejected pin of width {} must stage no write",
                bad_hash.len()
            );
        }

        // 20 bytes pins; empty is an unpin, not a malformed hash.
        for good_hash in [cast_hash(3), Vec::new()] {
            let mut txn = RocksDbTransactionBatch::new();
            ChannelPinStore::merge(
                &stores.pin,
                &pin_message(1, channel.clone(), good_hash.clone(), 2),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
            assert_eq!(
                ChannelPinStore::get_channel_pin(&stores.pin, &channel, Some(&txn))
                    .unwrap()
                    .unwrap()
                    .cast_hash,
                good_hash
            );
        }
    }

    #[test]
    fn member_cap_boundary_checks_every_action_on_mint_and_overwrite() {
        let stores = test_stores();
        let channel = channel_id(7);
        let actions = [
            ChannelMemberAction::AddMember,
            ChannelMemberAction::AddModerator,
            ChannelMemberAction::RemoveModerator,
            ChannelMemberAction::RemoveMember,
            ChannelMemberAction::Ban,
            ChannelMemberAction::Unban,
        ];
        let mut txn = RocksDbTransactionBatch::new();
        for target_fid in 1..=u64::from(CHANNEL_MEMBER_SLOT_CAP) {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    1,
                    channel.clone(),
                    target_fid,
                    ChannelMemberAction::AddMember,
                    target_fid as u32,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        assert_eq!(
            ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
            CHANNEL_MEMBER_SLOT_CAP
        );

        for (index, action) in actions.into_iter().enumerate() {
            let before = txn.batch.clone();
            let mint = member_message(
                1,
                channel.clone(),
                u64::from(CHANNEL_MEMBER_SLOT_CAP) + index as u64 + 1,
                action,
                CHANNEL_MEMBER_SLOT_CAP + index as u32 + 1,
            );
            let error =
                ChannelMemberStore::merge(&stores.member, &mint, &mut txn, DerivedIndexGate::Write)
                    .unwrap_err();
            assert_eq!(error.message, "channel slot cap exceeded");
            assert_eq!(txn.batch, before, "failed {action:?} mint changed txn");
        }

        for (index, action) in actions.into_iter().enumerate() {
            ChannelMemberStore::merge(
                &stores.member,
                &member_message(
                    2,
                    channel.clone(),
                    1,
                    action,
                    CHANNEL_MEMBER_SLOT_CAP + 100 + index as u32,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
            assert_eq!(
                ChannelMemberStore::slot_count(&stores.member, &channel, Some(&txn)).unwrap(),
                CHANNEL_MEMBER_SLOT_CAP
            );
        }
    }

    #[test]
    fn moderate_cap_boundary_checks_both_actions_on_mint_and_overwrite() {
        let stores = test_stores();
        let channel = channel_id(8);
        let actions = [ChannelModerateAction::Hide, ChannelModerateAction::Unhide];
        let mut txn = RocksDbTransactionBatch::new();
        for index in 0..CHANNEL_MODERATE_SLOT_CAP {
            if index > 0 && index % 8_000 == 0 {
                stores
                    .moderate
                    .event_handler()
                    .set_current_height(u64::from(index / 8_000));
            }
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    1,
                    channel.clone(),
                    cast_hash(index),
                    ChannelModerateAction::Hide,
                    index + 1,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
        }
        assert_eq!(
            ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
            CHANNEL_MODERATE_SLOT_CAP
        );

        for (index, action) in actions.into_iter().enumerate() {
            let before = txn.batch.clone();
            let mint = moderate_message(
                1,
                channel.clone(),
                cast_hash(CHANNEL_MODERATE_SLOT_CAP + index as u32),
                action,
                CHANNEL_MODERATE_SLOT_CAP + index as u32 + 1,
            );
            let error = ChannelModerateStore::merge(
                &stores.moderate,
                &mint,
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap_err();
            assert_eq!(error.message, "channel slot cap exceeded");
            assert_eq!(txn.batch, before, "failed {action:?} mint changed txn");
        }

        for (index, action) in actions.into_iter().enumerate() {
            ChannelModerateStore::merge(
                &stores.moderate,
                &moderate_message(
                    2,
                    channel.clone(),
                    cast_hash(0),
                    action,
                    CHANNEL_MODERATE_SLOT_CAP + 100 + index as u32,
                ),
                &mut txn,
                DerivedIndexGate::Write,
            )
            .unwrap();
            assert_eq!(
                ChannelModerateStore::slot_count(&stores.moderate, &channel, Some(&txn)).unwrap(),
                CHANNEL_MODERATE_SLOT_CAP
            );
        }
    }

    /// The moderate slot key is `channel_id ++ cast_hash`. If channel_id were not fixed-width,
    /// these two messages would build the SAME slot key while charging different cap counters:
    /// the second would supersede the first's row and mint into channel `a`'s keyspace for free.
    #[test]
    fn variable_length_channel_id_cannot_collide_moderate_slots_or_launder_the_cap() {
        let stores = test_stores();
        let victim_channel = channel_id(0xAA);
        let victim_cast = cast_hash(1);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                victim_channel.clone(),
                victim_cast.clone(),
                ChannelModerateAction::Hide,
                1,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();

        // Same bytes, split differently between the two fields.
        let mut shifted_channel = victim_channel.clone();
        shifted_channel.push(victim_cast[0]);
        let shifted_cast = victim_cast[1..].to_vec();
        let error = ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                2,
                shifted_channel,
                shifted_cast,
                ChannelModerateAction::Unhide,
                2,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap_err();
        assert_eq!(error.message, "channel id must be 32 bytes");

        // The victim's slot is untouched and its cap counter still reflects exactly its own rows.
        assert_eq!(
            ChannelModerateStore::moderation_state(
                &stores.moderate,
                &victim_channel,
                &victim_cast,
                Some(&txn),
            )
            .unwrap(),
            Some(ChannelModerationState::Hidden)
        );
        assert_eq!(
            ChannelModerateStore::slot_count(&stores.moderate, &victim_channel, Some(&txn))
                .unwrap(),
            1
        );
    }

    #[test]
    fn wrong_width_channel_id_and_cast_hash_are_rejected_by_every_slot_store() {
        let stores = test_stores();
        let short = vec![0x01; 31];
        let long = vec![0x01; 33];
        let mut txn = RocksDbTransactionBatch::new();
        for bad in [short, long] {
            for error in [
                ChannelUpdateStore::merge(
                    &stores.update,
                    &update_message(1, bad.clone(), 1, Some("x"), None, None, None),
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap_err(),
                ChannelMemberStore::merge(
                    &stores.member,
                    &member_message(1, bad.clone(), 9, ChannelMemberAction::AddMember, 1),
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap_err(),
                ChannelPinStore::merge(
                    &stores.pin,
                    &pin_message(1, bad.clone(), cast_hash(1), 1),
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap_err(),
                ChannelModerateStore::merge(
                    &stores.moderate,
                    &moderate_message(1, bad.clone(), cast_hash(1), ChannelModerateAction::Hide, 1),
                    &mut txn,
                    DerivedIndexGate::Write,
                )
                .unwrap_err(),
            ] {
                assert_eq!(error.message, "channel id must be 32 bytes");
            }
        }
        assert!(txn.batch.is_empty(), "a rejected merge staged writes");

        let error = ChannelModerateStore::merge(
            &stores.moderate,
            &moderate_message(
                1,
                channel_id(0xBB),
                vec![0xCC; 19],
                ChannelModerateAction::Hide,
                1,
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap_err();
        assert_eq!(error.message, "channel moderate cast hash must be 20 bytes");
    }

    /// An explicit zero-valued mode means UNSPECIFIED on the wire and must fold to the same
    /// restrictive value as an absent field — otherwise `Some(0)` is a way around the D3 default.
    #[test]
    fn explicitly_zero_modes_fold_restrictively_like_absent_modes() {
        let stores = test_stores();
        let channel = channel_id(0x5A);
        let mut txn = RocksDbTransactionBatch::new();
        ChannelUpdateStore::merge(
            &stores.update,
            &update_message(
                1,
                channel.clone(),
                1,
                Some("zeroed"),
                None,
                Some(CastingMode::None),
                Some(MembershipMode::None),
            ),
            &mut txn,
            DerivedIndexGate::Write,
        )
        .unwrap();

        let state = ChannelUpdateStore::get_channel_update(&stores.update, &channel, Some(&txn))
            .unwrap()
            .unwrap();
        assert_eq!(state.membership_mode, MembershipMode::Approval);
        assert_eq!(state.casting_mode, CastingMode::MembersOnly);
    }

    /// An unparseable mode must be refused at merge, not accepted into the slot and then blow up
    /// on every read — that would be a one-message per-channel poison pill.
    #[test]
    fn unparseable_channel_update_modes_are_rejected_at_merge() {
        let stores = test_stores();
        let channel = channel_id(0x5B);
        let mut txn = RocksDbTransactionBatch::new();
        let mut poison = update_message(1, channel.clone(), 1, Some("poison"), None, None, None);
        match poison.data.as_mut().unwrap().body.as_mut().unwrap() {
            Body::ChannelUpdateBody(body) => body.casting_mode = Some(9999),
            _ => unreachable!(),
        }
        let error =
            ChannelUpdateStore::merge(&stores.update, &poison, &mut txn, DerivedIndexGate::Write)
                .unwrap_err();
        assert_eq!(error.message, "invalid channel casting mode");
        assert!(txn.batch.is_empty());
        assert!(
            ChannelUpdateStore::get_channel_update(&stores.update, &channel, Some(&txn))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn re_merging_the_current_slot_incumbent_is_a_duplicate() {
        let stores = test_stores();
        let channel = channel_id(0x6C);
        let mut txn = RocksDbTransactionBatch::new();
        let message = update_message(1, channel, 1, Some("only"), None, None, None);
        ChannelUpdateStore::merge(&stores.update, &message, &mut txn, DerivedIndexGate::Write)
            .unwrap();
        let before = txn.batch.clone();
        let error =
            ChannelUpdateStore::merge(&stores.update, &message, &mut txn, DerivedIndexGate::Write)
                .unwrap_err();
        assert_eq!(error.message, "message has already been merged");
        assert_eq!(txn.batch, before, "duplicate re-merge mutated the txn");
    }

    /// The slot index and counters are maintained only by `merge_slot`. Every generic mutating
    /// path would strand the slot pointer or desync the counters, so all of them must refuse.
    #[test]
    fn generic_mutating_store_paths_are_rejected_for_channel_slots() {
        let stores = test_stores();
        let message = update_message(1, channel_id(9), 1, Some("x"), None, None, None);
        let ts_hash = crate::storage::store::account::make_ts_hash(1, &message.hash).unwrap();
        let expected = "slot store requires consensus-order merge";

        let mut txn = RocksDbTransactionBatch::new();
        assert_eq!(
            stores
                .update
                .merge(
                    &message,
                    &mut txn,
                    &crate::storage::store::test_helper::default_merge_ctx(),
                )
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .merge_add(
                    &ts_hash,
                    &message,
                    &mut txn,
                    crate::storage::store::account::FollowIndexGate::no_follow_carrier(),
                )
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .merge_remove(&ts_hash, &message, &mut txn)
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .revoke(&message, &mut txn)
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .prune_message(&message, &mut txn)
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .prune_messages(1, 100, 0, &mut txn)
                .unwrap_err()
                .message,
            expected
        );
        assert_eq!(
            stores
                .update
                .revoke_messages_by_signer(1, &message.signer, &mut txn)
                .unwrap_err()
                .message,
            expected
        );
    }

    #[test]
    fn generic_timestamp_lww_merge_is_rejected_for_channel_slots() {
        let stores = test_stores();
        let message = update_message(1, channel_id(9), 1, Some("x"), None, None, None);
        let error = stores
            .update
            .merge(
                &message,
                &mut RocksDbTransactionBatch::new(),
                &crate::storage::store::test_helper::default_merge_ctx(),
            )
            .unwrap_err();
        assert_eq!(error.message, "slot store requires consensus-order merge");
    }
}
