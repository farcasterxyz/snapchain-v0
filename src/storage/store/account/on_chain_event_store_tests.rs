#[cfg(test)]
mod tests {
    use crate::storage::db;
    use crate::storage::db::RocksDbTransactionBatch;
    use crate::storage::store::account::{OnchainEventStore, StorageSlot, StoreEventHandler};
    use crate::utils::factory;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn store() -> (OnchainEventStore, TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("a.db");

        let db = db::RocksDB::new(db_path.to_str().unwrap());
        db.open().unwrap();

        (
            OnchainEventStore::new(Arc::new(db), StoreEventHandler::new(None, None, None)),
            dir,
        )
    }

    #[test]
    fn test_storage_slot_from_rent_event() {
        let one_year_in_seconds = 365 * 24 * 60 * 60;

        let expired_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(1), None, true);
        let slot = StorageSlot::from_event(&expired_legacy_rent_event).unwrap();
        assert_eq!(slot.is_active(), false);
        assert_eq!(slot.legacy_units, 1);
        assert_eq!(slot.units, 0);
        assert_eq!(
            slot.invalidate_at,
            expired_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 2
        );

        let valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(5), None, false);
        let slot = StorageSlot::from_event(&valid_legacy_rent_event).unwrap();
        assert_eq!(slot.is_active(), true);
        assert_eq!(slot.legacy_units, 5);
        assert_eq!(slot.units, 0);
        assert_eq!(
            slot.invalidate_at,
            valid_legacy_rent_event.block_timestamp as u32 + one_year_in_seconds * 2
        );

        let valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(9), false);
        let slot = StorageSlot::from_event(&valid_2024_rent_event).unwrap();
        assert_eq!(slot.is_active(), true);
        assert_eq!(slot.legacy_units, 0);
        assert_eq!(slot.units, 9);
        assert_eq!(
            slot.invalidate_at,
            valid_2024_rent_event.block_timestamp as u32 + one_year_in_seconds
        );
    }

    #[test]
    fn test_storage_slot_merge() {
        let current_time = factory::time::current_timestamp();
        // When merging two active slots, the units should be added together
        let active_slot = StorageSlot::new(1, 2, current_time + 1);
        let mut active_slot2 = StorageSlot::new(2, 1, current_time + 10);

        assert_eq!(active_slot.is_active(), true);
        assert_eq!(active_slot2.is_active(), true);

        assert_eq!(active_slot2.merge(&active_slot), true);

        assert_eq!(active_slot2.legacy_units, 3);
        assert_eq!(active_slot2.units, 3);
        assert_eq!(active_slot2.invalidate_at, current_time + 1); // min of both timestamps
        assert_eq!(active_slot2.is_active(), true);

        // When merging an active slot with an inactive slot, the inactive slot should be ignored
        let inactive_slot = StorageSlot::new(1, 2, current_time - 10);
        let mut active_slot3 = StorageSlot::new(2, 1, current_time + 10);

        assert_eq!(inactive_slot.is_active(), false);

        let mut inactive_slot_merged = inactive_slot.clone();

        // When merging an active slot into inactive slot, the inactive slot is replaced
        assert_eq!(inactive_slot_merged.merge(&active_slot3), true);
        assert_eq!(inactive_slot_merged.legacy_units, 2);
        assert_eq!(inactive_slot_merged.units, 1);
        assert_eq!(inactive_slot_merged.invalidate_at, current_time + 10);
        assert_eq!(inactive_slot_merged.is_active(), true);

        // When merging an inactive slot into active slot, the active slot is unchanged
        assert_eq!(active_slot3.merge(&inactive_slot), false);
        assert_eq!(active_slot3.legacy_units, 2);
        assert_eq!(active_slot3.units, 1);
        assert_eq!(active_slot3.invalidate_at, current_time + 10);
        assert_eq!(active_slot3.is_active(), true);
    }

    #[test]
    fn test_storage_slot_when_no_units() {
        let (store, _dir) = store();

        let storage_slot = store.get_storage_slot_for_fid(10).unwrap();
        assert_eq!(storage_slot.is_active(), false);
        assert_eq!(storage_slot.units, 0);
        assert_eq!(storage_slot.legacy_units, 0);
        assert_eq!(storage_slot.invalidate_at, 0);
    }

    #[test]
    fn test_storage_slot_with_mix_of_units() {
        let (store, _dir) = store();

        let expired_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(1), None, true);

        let valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(5), None, false);
        let another_valid_legacy_rent_event =
            factory::events_factory::create_rent_event(10, Some(7), None, false);
        let valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(9), false);
        let another_valid_2024_rent_event =
            factory::events_factory::create_rent_event(10, None, Some(11), false);

        let valid_rent_event_different_fid =
            factory::events_factory::create_rent_event(11, None, Some(13), false);

        let mut txn = RocksDbTransactionBatch::new();
        for event in vec![
            expired_legacy_rent_event,
            valid_legacy_rent_event,
            another_valid_legacy_rent_event,
            valid_2024_rent_event,
            another_valid_2024_rent_event,
            valid_rent_event_different_fid,
        ] {
            store.merge_onchain_event(event, &mut txn).unwrap();
        }
        store.db.commit(txn).unwrap();

        let storage_slot_different_fid = store.get_storage_slot_for_fid(11).unwrap();
        assert_eq!(storage_slot_different_fid.is_active(), true);
        assert_eq!(storage_slot_different_fid.legacy_units, 0);
        assert_eq!(storage_slot_different_fid.units, 13);

        let storage_slot = store.get_storage_slot_for_fid(10).unwrap();
        assert_eq!(storage_slot.is_active(), true);
        assert_eq!(storage_slot.legacy_units, 12); // 5 + 7
        assert_eq!(storage_slot.units, 20); // 9 + 11
    }
}
