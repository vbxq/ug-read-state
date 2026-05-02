use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use crate::{
    store::BoxFuture, ReadStateAckKind, ReadStateConfig, ReadStateEntry, ReadStateHandle,
    ReadStateMutation, ReadStateStore,
};

#[derive(Default)]
struct MockStoreState {
    entries: HashMap<(i64, i64), ReadStateEntry>,
    find_one_calls: Vec<(i64, i64)>,
    find_all_calls: Vec<i64>,
    upserts: Vec<(i64, i64, ReadStateEntry)>,
    deletes: Vec<(i64, i64)>,
    delete_failures: VecDeque<ug_error::UgError>,
}

#[derive(Default)]
struct MockReadStateStore {
    state: Mutex<MockStoreState>,
}

impl MockReadStateStore {
    async fn insert(&self, user_id: i64, channel_id: i64, entry: ReadStateEntry) {
        self.state
            .lock()
            .await
            .entries
            .insert((user_id, channel_id), entry);
    }

    async fn snapshot(&self, user_id: i64, channel_id: i64) -> Option<ReadStateEntry> {
        self.state
            .lock()
            .await
            .entries
            .get(&(user_id, channel_id))
            .cloned()
    }

    async fn upsert_count(&self) -> usize {
        self.state.lock().await.upserts.len()
    }

    async fn delete_count(&self) -> usize {
        self.state.lock().await.deletes.len()
    }

    async fn find_one_count(&self) -> usize {
        self.state.lock().await.find_one_calls.len()
    }

    async fn push_delete_failure(&self, error: ug_error::UgError) {
        self.state.lock().await.delete_failures.push_back(error);
    }
}

impl ReadStateStore for MockReadStateStore {
    fn find_one(
        &self,
        user_id: i64,
        channel_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Option<ReadStateEntry>>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.find_one_calls.push((user_id, channel_id));
            Ok(state.entries.get(&(user_id, channel_id)).cloned())
        })
    }

    fn find_all_for_user(
        &self,
        user_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Vec<(i64, ReadStateEntry)>>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.find_all_calls.push(user_id);
            let mut entries: Vec<(i64, ReadStateEntry)> = state
                .entries
                .iter()
                .filter_map(|(&(entry_user_id, channel_id), entry)| {
                    (entry_user_id == user_id).then_some((channel_id, entry.clone()))
                })
                .collect();
            entries.sort_unstable_by_key(|(channel_id, _)| *channel_id);
            Ok(entries)
        })
    }

    fn upsert<'a>(
        &'a self,
        user_id: i64,
        channel_id: i64,
        entry: &'a ReadStateEntry,
    ) -> BoxFuture<'a, ug_error::Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            let mut clean_entry = entry.clone();
            clean_entry.dirty = false;
            state
                .entries
                .insert((user_id, channel_id), clean_entry.clone());
            state.upserts.push((user_id, channel_id, clean_entry));
            Ok(())
        })
    }

    fn delete(&self, user_id: i64, channel_id: i64) -> BoxFuture<'_, ug_error::Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            if let Some(error) = state.delete_failures.pop_front() {
                return Err(error);
            }
            state.entries.remove(&(user_id, channel_id));
            state.deletes.push((user_id, channel_id));
            Ok(())
        })
    }
}

fn test_config() -> ReadStateConfig {
    ReadStateConfig {
        shard_count: 1,
        shard_channel_capacity: 32,
        flush_interval: Duration::from_secs(60),
        max_entries_per_shard: 8,
        shutdown_timeout: Duration::from_secs(1),
    }
}

async fn start_handle(store: Arc<MockReadStateStore>, config: ReadStateConfig) -> ReadStateHandle {
    ReadStateHandle::start_with_store(store, config)
}

#[tokio::test]
async fn increment_mention_three_times_results_in_three() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    for offset in 0..3_i64 {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 10,
                message_id: 100 + offset,
            })
            .expect("increment mutation should be queued");
    }

    let entry = handle
        .get(1, 10)
        .await
        .expect("read_state entry should exist after increments");
    assert_eq!(
        entry.mention_count, 3,
        "expected mention_count == 3 after three increments, got {}",
        entry.mention_count
    );
    assert_eq!(
        entry.last_read_message_id,
        Some(0),
        "expected missing read-state seed to keep last_read_message_id at 0, got {:?}",
        entry.last_read_message_id
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn increment_then_reset_mentions_results_in_zero() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    for offset in 0..3_i64 {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 11,
                message_id: 200 + offset,
            })
            .expect("increment mutation should be queued");
    }
    handle
        .mutate(ReadStateMutation::ResetMentions {
            user_id: 1,
            channel_id: 11,
        })
        .expect("reset mutation should be queued");

    let entry = handle
        .get(1, 11)
        .await
        .expect("read_state entry should exist after reset");
    assert_eq!(
        entry.mention_count, 0,
        "expected mention_count == 0 after reset, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_before_last_message_keeps_mentions_and_does_not_regress_last_message() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            1,
            12,
            ReadStateEntry {
                mention_count: 4,
                latest_channel_message_id: Some(99),
                last_read_message_id: Some(50),
                version: 0,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    handle
        .mutate(ReadStateMutation::Ack {
            user_id: 1,
            channel_id: 12,
            message_id: 70,
            version: 1,
        })
        .expect("ack mutation should be queued");

    let entry = handle
        .get(1, 12)
        .await
        .expect("read_state entry should exist after ack");
    assert_eq!(
        entry.mention_count, 4,
        "expected mention_count to stay at 4 after partial ack, got {}",
        entry.mention_count
    );
    assert_eq!(
        entry.latest_channel_message_id,
        Some(99),
        "expected latest_channel_message_id to stay at 99 after older ack, got {:?}",
        entry.latest_channel_message_id
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_at_or_past_last_message_resets_mentions() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            1,
            13,
            ReadStateEntry {
                mention_count: 5,
                latest_channel_message_id: Some(99),
                last_read_message_id: Some(90),
                version: 0,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    handle
        .mutate(ReadStateMutation::Ack {
            user_id: 1,
            channel_id: 13,
            message_id: 99,
            version: 1,
        })
        .expect("ack mutation should be queued");

    let entry = handle
        .get(1, 13)
        .await
        .expect("read_state entry should exist after ack");
    assert_eq!(
        entry.mention_count, 0,
        "expected mention_count == 0 after acking latest message, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn stale_ack_version_is_ignored_and_not_persisted() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            1,
            13,
            ReadStateEntry {
                mention_count: 5,
                latest_channel_message_id: Some(99),
                last_read_message_id: Some(90),
                version: 7,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    handle
        .mutate_and_persist(ReadStateMutation::Ack {
            user_id: 1,
            channel_id: 13,
            message_id: 120,
            version: 6,
        })
        .await
        .expect("stale ack should be ignored");

    let entry = handle
        .get(1, 13)
        .await
        .expect("read_state entry should still exist after stale ack");
    assert_eq!(entry.version, 7, "expected stale ACK version to be ignored");
    assert_eq!(
        entry.last_read_message_id,
        Some(90),
        "expected stale ACK to preserve last_read_message_id"
    );
    assert_eq!(
        store.upsert_count().await,
        0,
        "expected stale ACK to avoid a write-through flush"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn successful_flush_marks_entry_clean() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 14,
            message_id: 300,
        })
        .expect("increment mutation should be queued");
    handle.force_flush(1).await;

    let entry = handle
        .get(1, 14)
        .await
        .expect("read_state entry should exist after flush");
    assert!(
        !entry.dirty,
        "expected dirty == false after successful flush, got dirty == {}",
        entry.dirty
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn concurrent_increment_mutations_are_serialized() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    let handle_a = handle.clone();
    let handle_b = handle.clone();

    let task_a = tokio::spawn(async move {
        handle_a
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 15,
                message_id: 400,
            })
            .expect("first increment should be queued");
    });
    let task_b = tokio::spawn(async move {
        handle_b
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 15,
                message_id: 401,
            })
            .expect("second increment should be queued");
    });

    task_a.await.expect("first task should complete");
    task_b.await.expect("second task should complete");

    let entry = handle
        .get(1, 15)
        .await
        .expect("read_state entry should exist after concurrent increments");
    assert_eq!(
        entry.mention_count, 2,
        "expected mention_count == 2 after two concurrent increments, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn get_loads_from_store_on_cache_miss() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            1,
            16,
            ReadStateEntry {
                mention_count: 7,
                latest_channel_message_id: Some(123),
                last_read_message_id: Some(120),
                version: 0,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 3,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    let entry = handle
        .get(1, 16)
        .await
        .expect("cache miss should be loaded from store");

    assert_eq!(
        entry.mention_count, 7,
        "expected mention_count == 7 from store-backed get, got {}",
        entry.mention_count
    );
    assert_eq!(
        store.find_one_count().await,
        1,
        "expected exactly one store find_one call on cache miss, got {}",
        store.find_one_count().await
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn dirty_entry_is_flushed_before_eviction() {
    let store = Arc::new(MockReadStateStore::default());
    let mut config = test_config();
    config.max_entries_per_shard = 1;

    let handle = start_handle(store.clone(), config).await;
    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 20,
            message_id: 500,
        })
        .expect("first increment should be queued");
    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 21,
            message_id: 501,
        })
        .expect("second increment should be queued");

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        store.upsert_count().await >= 1,
        "expected at least one flush before eviction, got {} upserts",
        store.upsert_count().await
    );
    assert!(
        store.snapshot(1, 20).await.is_some(),
        "expected evicted dirty entry to be flushed to store before removal"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn mutate_buffers_when_soft_limit_is_exceeded() {
    let store = Arc::new(MockReadStateStore::default());
    let config = ReadStateConfig {
        shard_count: 1,
        shard_channel_capacity: 1,
        flush_interval: Duration::from_secs(60),
        max_entries_per_shard: 8,
        shutdown_timeout: Duration::from_secs(1),
    };
    let handle = start_handle(store.clone(), config).await;

    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 30,
            message_id: 600,
        })
        .expect("first mutation should be accepted");

    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 31,
            message_id: 601,
        })
        .expect("second mutation should be buffered instead of dropped");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let first = handle
        .get(1, 30)
        .await
        .expect("first buffered mutation should be applied");
    let second = handle
        .get(1, 31)
        .await
        .expect("second buffered mutation should be applied");

    assert_eq!(
        first.mention_count, 1,
        "expected first buffered mutation to produce mention_count == 1, got {}",
        first.mention_count
    );
    assert_eq!(
        second.mention_count, 1,
        "expected second buffered mutation to produce mention_count == 1, got {}",
        second.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn mutate_returns_shard_closed_after_shutdown() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    handle.shutdown().await;

    let result = handle.mutate(ReadStateMutation::IncrementMention {
        user_id: 1,
        channel_id: 31,
        message_id: 601,
    });
    assert!(
        matches!(
            result,
            Err(crate::ReadStateMutationError::ShardClosed { .. })
        ),
        "expected mutate after shutdown to fail with ShardClosed, got {:?}",
        result
    );
}

#[tokio::test]
async fn increment_mention_is_idempotent_for_same_message_id() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    // Send the same IncrementMention twice (simulates JetStream redelivery)
    for _ in 0..2 {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 50,
                message_id: 9999,
            })
            .expect("increment mutation should be queued");
    }

    let entry = handle
        .get(1, 50)
        .await
        .expect("read_state entry should exist after increments");
    assert_eq!(
        entry.mention_count, 1,
        "expected mention_count == 1 after duplicate IncrementMention with same message_id, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn increment_mention_counts_distinct_message_ids() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    for msg_id in [1000, 1001, 1002] {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 51,
                message_id: msg_id,
            })
            .expect("increment mutation should be queued");
    }

    let entry = handle
        .get(1, 51)
        .await
        .expect("read_state entry should exist after increments");
    assert_eq!(
        entry.mention_count, 3,
        "expected mention_count == 3 after three distinct message_ids, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn increment_mention_skips_older_message_id_after_newer() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    // First, send a newer message_id
    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 52,
            message_id: 5000,
        })
        .expect("increment mutation should be queued");

    // Then an older one (out-of-order redelivery)
    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 52,
            message_id: 4000,
        })
        .expect("increment mutation should be queued");

    let entry = handle
        .get(1, 52)
        .await
        .expect("read_state entry should exist after increments");
    assert_eq!(
        entry.mention_count, 1,
        "expected mention_count == 1 after newer then older message_id, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn increment_mention_triple_redelivery_still_counts_once() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    // Simulate triple redelivery of two distinct messages
    for msg_id in [1000, 1000, 1001, 1001, 1001] {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 53,
                message_id: msg_id,
            })
            .expect("increment mutation should be queued");
    }

    let entry = handle
        .get(1, 53)
        .await
        .expect("read_state entry should exist after increments");
    assert_eq!(
        entry.mention_count, 2,
        "expected mention_count == 2 after redelivered pair of distinct messages, got {}",
        entry.mention_count
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn bulk_ack_persisted_entry_skips_redelivered_increment_after_restart() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    for message_id in [1000, 1001] {
        handle
            .mutate(ReadStateMutation::IncrementMention {
                user_id: 1,
                channel_id: 54,
                message_id,
            })
            .expect("increment mutation should be queued");
    }

    handle
        .ack_and_persist(1, 54, 1001, ReadStateAckKind::BulkAck, None)
        .await
        .expect("bulk ack should persist");
    handle.shutdown().await;

    let restarted = start_handle(store.clone(), test_config()).await;
    restarted
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 1,
            channel_id: 54,
            message_id: 1000,
        })
        .expect("redelivered increment should be queued");

    let entry = restarted
        .get(1, 54)
        .await
        .expect("read_state entry should exist after restart");
    assert_eq!(
        entry.mention_count, 0,
        "expected redelivered IncrementMention <= bulk-acked message_id to stay deduped after restart"
    );
    assert_eq!(
        entry.last_read_message_id,
        Some(1001),
        "expected persisted ack position to survive restart"
    );

    restarted.shutdown().await;
}

#[tokio::test]
async fn ack_and_persist_marks_only_the_first_ack_as_first_ack() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    let first = handle
        .ack_and_persist(5, 55, 1000, ReadStateAckKind::Ack, None)
        .await
        .expect("first ack should persist");
    let second = handle
        .ack_and_persist(5, 55, 1001, ReadStateAckKind::Ack, None)
        .await
        .expect("second ack should persist");

    assert!(first.first_ack, "first ack must be marked as first_ack");
    assert!(
        !second.first_ack,
        "subsequent ack on the same channel must clear first_ack"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_with_last_viewed_persists_client_supplied_value() {
    // CLAUDE.md rule 15.5 + 2026-04-19 DM capture: the real Discord client
    // POSTs the ack with `{"token": null, "last_viewed": 4121}`, where
    // `last_viewed` is a Discord-epoch-day integer. Persistence must echo
    // that exact value into Scylla so READY and MESSAGE_ACK emit the
    // client's own value on first-ack (instead of 0 / NULL).
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    let record = handle
        .ack_and_persist(10, 100, 5000, ReadStateAckKind::Ack, Some(4121))
        .await
        .expect("ack with last_viewed should persist");
    assert_eq!(
        record.last_viewed,
        Some(4121),
        "ack record must carry the client-supplied last_viewed"
    );
    assert!(
        record.first_ack,
        "first ack for (10, 100) should be flagged"
    );

    let persisted = store
        .snapshot(10, 100)
        .await
        .expect("scylla row should exist after ack");
    assert_eq!(
        persisted.last_viewed,
        Some(4121),
        "scylla row must carry the client-supplied last_viewed"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_with_none_last_viewed_preserves_prior_value() {
    // Clients that POST ack with no body (or omit last_viewed) must not
    // wipe an already-persisted last_viewed -- the server preserves what
    // was there. Verified by acking first with Some(4121), then again
    // with None, and asserting the stored value is still 4121.
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    handle
        .ack_and_persist(11, 101, 2000, ReadStateAckKind::Ack, Some(4121))
        .await
        .expect("first ack should persist");
    handle
        .ack_and_persist(11, 101, 2001, ReadStateAckKind::Ack, None)
        .await
        .expect("second ack without body should persist");

    let persisted = store
        .snapshot(11, 101)
        .await
        .expect("scylla row should exist after second ack");
    assert_eq!(
        persisted.last_viewed,
        Some(4121),
        "ack with None last_viewed must preserve prior stored value"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_fence_keeps_the_higher_increment_message_id() {
    let store = Arc::new(MockReadStateStore::default());
    let handle = start_handle(store.clone(), test_config()).await;

    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 6,
            channel_id: 56,
            message_id: 2000,
        })
        .expect("increment mutation should be queued");

    handle
        .ack_and_persist(6, 56, 1500, ReadStateAckKind::Ack, None)
        .await
        .expect("partial ack should persist");

    handle
        .mutate(ReadStateMutation::IncrementMention {
            user_id: 6,
            channel_id: 56,
            message_id: 1900,
        })
        .expect("older redelivery should be queued");

    let entry = handle
        .get(6, 56)
        .await
        .expect("read_state entry should exist after ack fence test");
    assert_eq!(
        entry.mention_count, 1,
        "ack fence must preserve the higher increment message_id and skip older redeliveries"
    );
    assert_eq!(
        entry.last_read_message_id,
        Some(1500),
        "partial ack must still advance the durable read position"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn delete_flushes_before_cache_removal() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            1,
            40,
            ReadStateEntry {
                mention_count: 3,
                latest_channel_message_id: Some(3),
                last_read_message_id: Some(1),
                version: 0,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;
    store
        .push_delete_failure(ug_error::UgError::internal("database: delete failed"))
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    handle
        .mutate(ReadStateMutation::Delete {
            user_id: 1,
            channel_id: 40,
        })
        .expect("delete mutation should be queued");
    handle.force_flush(1).await;

    assert!(
        handle.get(1, 40).await.is_none(),
        "expected pending-delete entry to be hidden from cache reads after delete mutation"
    );
    assert_eq!(
        store.delete_count().await,
        0,
        "expected failed delete flush to avoid removing store entry, got {} delete calls",
        store.delete_count().await
    );

    handle.force_flush(1).await;
    assert_eq!(
        store.delete_count().await,
        1,
        "expected delete to be retried and flushed once failure cleared, got {} delete calls",
        store.delete_count().await
    );
    assert!(
        handle.get(1, 40).await.is_none(),
        "expected deleted entry to stay absent from cache after successful flush"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_and_persist_allocates_versions_monotonically_across_channels() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            42,
            100,
            ReadStateEntry {
                mention_count: 0,
                latest_channel_message_id: Some(10),
                last_read_message_id: Some(10),
                version: 7,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;
    store
        .insert(
            42,
            200,
            ReadStateEntry {
                mention_count: 1,
                latest_channel_message_id: Some(12),
                last_read_message_id: Some(11),
                version: 9,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 3,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    let first = handle
        .ack_and_persist(42, 100, 15, ReadStateAckKind::Ack, None)
        .await
        .expect("first ack should persist");
    let second = handle
        .ack_and_persist(42, 200, 20, ReadStateAckKind::BulkAck, None)
        .await
        .expect("second ack should persist");

    assert_eq!(
        first.version, 10,
        "expected first version to continue from max durable version 9"
    );
    assert_eq!(
        second.version, 11,
        "expected second version to remain globally monotonic"
    );
    assert_eq!(
        store.upsert_count().await,
        2,
        "expected each ack to flush immediately"
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn ack_and_persist_reuses_in_memory_version_head_without_reloading_store() {
    let store = Arc::new(MockReadStateStore::default());
    store
        .insert(
            77,
            300,
            ReadStateEntry {
                mention_count: 0,
                latest_channel_message_id: Some(1),
                last_read_message_id: Some(1),
                version: 4,
                last_viewed: Some(0),
                last_pin_timestamp: None,
                flags: 0,
                dirty: false,
            },
        )
        .await;

    let handle = start_handle(store.clone(), test_config()).await;
    handle
        .ack_and_persist(77, 300, 2, ReadStateAckKind::Ack, None)
        .await
        .expect("first ack should seed version head");
    let find_all_after_first = { store.state.lock().await.find_all_calls.len() };

    handle
        .ack_and_persist(77, 300, 3, ReadStateAckKind::Ack, None)
        .await
        .expect("second ack should reuse version head");

    assert_eq!(
        find_all_after_first, 1,
        "expected exactly one full-user load while seeding version head"
    );
    assert_eq!(
        { store.state.lock().await.find_all_calls.len() },
        1,
        "expected second ack to avoid another full-user load"
    );

    handle.shutdown().await;
}
