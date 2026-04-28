use std::time::Duration;

use serde::{Deserialize, Serialize};

pub(crate) type ReadStateKey = (i64, i64);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadStateEntry {
    pub mention_count: i32,
    pub latest_channel_message_id: Option<i64>,
    pub last_read_message_id: Option<i64>,
    pub version: i64,
    pub last_viewed: Option<i32>,
    pub last_pin_timestamp: Option<i64>,
    pub flags: i32,
    pub dirty: bool,
}

impl ReadStateEntry {
    pub(crate) fn unread_seed() -> Self {
        Self {
            last_read_message_id: Some(0),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadStateAckKind {
    Ack,
    BulkAck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadStateAckRecord {
    pub channel_id: i64,
    pub message_id: i64,
    pub version: i64,
    pub last_viewed: Option<i32>,
    pub flags: i32,
    pub ack_kind: ReadStateAckKind,
    /// True when this ack was the first ever for (user, channel).
    /// Drives whether the MESSAGE_ACK gateway event carries `flags` and `last_viewed` (only sent on first ack per capture)
    #[serde(default)]
    pub first_ack: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadStateMutation {
    /// Caller responsibility:
    /// - do not call for the message author
    /// - do not call when the message should be ignored because it is already
    ///   covered by the recipient's latest ACK/read position
    ///
    /// This crate serializes counters, it does not re-check business rules
    IncrementMention {
        user_id: i64,
        channel_id: i64,
        message_id: i64,
    },
    ResetMentions {
        user_id: i64,
        channel_id: i64,
    },
    SetMentionCount {
        user_id: i64,
        channel_id: i64,
        mention_count: i32,
    },
    Ack {
        user_id: i64,
        channel_id: i64,
        message_id: i64,
        version: i64,
    },
    BulkAck {
        user_id: i64,
        channel_id: i64,
        message_id: i64,
        version: i64,
    },
    Delete {
        user_id: i64,
        channel_id: i64,
    },
}

impl ReadStateMutation {
    pub fn user_id(&self) -> i64 {
        match self {
            Self::IncrementMention { user_id, .. }
            | Self::ResetMentions { user_id, .. }
            | Self::SetMentionCount { user_id, .. }
            | Self::Ack { user_id, .. }
            | Self::BulkAck { user_id, .. }
            | Self::Delete { user_id, .. } => *user_id,
        }
    }

    pub fn channel_id(&self) -> i64 {
        match self {
            Self::IncrementMention { channel_id, .. }
            | Self::ResetMentions { channel_id, .. }
            | Self::SetMentionCount { channel_id, .. }
            | Self::Ack { channel_id, .. }
            | Self::BulkAck { channel_id, .. }
            | Self::Delete { channel_id, .. } => *channel_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadStateMutationError {
    ShardClosed { shard: usize },
}

#[derive(Debug, Clone, Copy)]
pub struct ReadStateConfig {
    pub shard_count: usize,
    pub shard_channel_capacity: usize,
    pub flush_interval: Duration,
    pub max_entries_per_shard: usize,
    pub shutdown_timeout: Duration,
}

impl Default for ReadStateConfig {
    fn default() -> Self {
        Self {
            shard_count: 16,
            shard_channel_capacity: 4096,
            flush_interval: Duration::from_secs(30),
            max_entries_per_shard: 50_000,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

pub(crate) fn apply_ack(entry: &mut ReadStateEntry, acked_message_id: i64, version: i64) -> bool {
    if version <= entry.version {
        return false;
    }

    if acked_message_id >= entry.latest_channel_message_id.unwrap_or(0) {
        entry.mention_count = 0;
    }

    entry.latest_channel_message_id = Some(
        entry
            .latest_channel_message_id
            .map_or(acked_message_id, |current| current.max(acked_message_id)),
    );
    entry.last_read_message_id = Some(
        entry
            .last_read_message_id
            .map_or(acked_message_id, |current| current.max(acked_message_id)),
    );
    entry.version = version;
    entry.dirty = true;
    true
}
