use std::sync::Arc;

use scylla::value::CqlTimestamp;
use ug_db::scylla::pool::ScyllaPool;
use ug_db::scylla::read_state_repo::{self, ReadStateStatements};

use crate::store::{BoxFuture, ReadStateStore};
use crate::ReadStateEntry;

pub(crate) struct ScyllaReadStateStore {
    scylla: ScyllaPool,
    stmts: Arc<ReadStateStatements>,
}

impl ScyllaReadStateStore {
    pub(crate) fn new(scylla: ScyllaPool, stmts: ReadStateStatements) -> Self {
        Self {
            scylla,
            stmts: Arc::new(stmts),
        }
    }
}

impl ReadStateStore for ScyllaReadStateStore {
    fn find_one(
        &self,
        user_id: i64,
        channel_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Option<ReadStateEntry>>> {
        Box::pin(async move {
            let row =
                read_state_repo::find_one(&self.scylla, &self.stmts, user_id, channel_id).await?;
            Ok(row.as_ref().map(row_to_entry))
        })
    }

    fn find_all_for_user(
        &self,
        user_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Vec<(i64, ReadStateEntry)>>> {
        Box::pin(async move {
            let rows = read_state_repo::find_by_user_id(&self.scylla, &self.stmts, user_id).await?;
            Ok(rows
                .into_iter()
                .map(|row| (row.channel_id, row_to_entry(&row)))
                .collect())
        })
    }

    fn upsert<'a>(
        &'a self,
        user_id: i64,
        channel_id: i64,
        entry: &'a ReadStateEntry,
    ) -> BoxFuture<'a, ug_error::Result<()>> {
        Box::pin(async move {
            read_state_repo::upsert_with_mentions(
                &self.scylla,
                &self.stmts,
                user_id,
                channel_id,
                entry.latest_channel_message_id,
                entry.version,
                entry.mention_count,
                entry.last_read_message_id,
                entry.last_viewed,
                entry.last_pin_timestamp.map(CqlTimestamp),
                entry.flags,
            )
            .await
            .map_err(ug_error::UgError::from)
        })
    }

    fn delete(&self, user_id: i64, channel_id: i64) -> BoxFuture<'_, ug_error::Result<()>> {
        Box::pin(async move {
            read_state_repo::delete_one(&self.scylla, &self.stmts, user_id, channel_id)
                .await
                .map_err(ug_error::UgError::from)
        })
    }
}

fn row_to_entry(row: &read_state_repo::ReadStateRow) -> ReadStateEntry {
    ReadStateEntry {
        mention_count: row.mention_count.unwrap_or(0),
        latest_channel_message_id: row.last_message_id,
        last_read_message_id: row.last_acked_id,
        version: row.version.unwrap_or(0),
        last_viewed: row.last_viewed,
        last_pin_timestamp: row.last_pin_timestamp.map(|timestamp| timestamp.0),
        flags: row.flags.unwrap_or(0),
        dirty: false,
    }
}
