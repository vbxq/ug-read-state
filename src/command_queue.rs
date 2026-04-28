use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use tokio::sync::{oneshot, Notify};

use crate::{ReadStateAckKind, ReadStateAckRecord, ReadStateEntry, ReadStateMutation};

pub(crate) enum ShardCommand {
    Mutate(ReadStateMutation),
    MutateAndPersist {
        mutation: ReadStateMutation,
        response: oneshot::Sender<ug_error::Result<()>>,
    },
    AckAndPersist {
        user_id: i64,
        channel_id: i64,
        message_id: i64,
        ack_kind: ReadStateAckKind,
        /// Discord-epoch-day integer sent by the real client in the ack
        /// request body (`{"token": null, "last_viewed": 4121}`).
        /// `None` means "don't touch `entry.last_viewed`"; `Some(v)` overwrites.
        last_viewed: Option<i32>,
        response: oneshot::Sender<ug_error::Result<ReadStateAckRecord>>,
    },
    Get {
        user_id: i64,
        channel_id: i64,
        response: oneshot::Sender<Option<ReadStateEntry>>,
    },
    GetAllForUser {
        user_id: i64,
        response: oneshot::Sender<Vec<(i64, ReadStateEntry)>>,
    },
    #[cfg(test)]
    ForceFlush {
        response: oneshot::Sender<()>,
    },
}

pub(crate) struct ShardCommandQueue {
    shard_id: usize,
    soft_limit: usize,
    commands: Mutex<VecDeque<ShardCommand>>,
    notify: Notify,
    over_soft_limit: AtomicBool,
    closed: AtomicBool,
}

impl ShardCommandQueue {
    pub(crate) fn new(shard_id: usize, soft_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            shard_id,
            soft_limit,
            commands: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            over_soft_limit: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        })
    }

    pub(crate) fn push(&self, command: ShardCommand) -> Result<(), ()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(());
        }

        let len = {
            let mut commands = self.commands.lock().expect("command queue lock poisoned");
            if self.closed.load(Ordering::SeqCst) {
                return Err(());
            }
            commands.push_back(command);
            commands.len()
        };

        if len > self.soft_limit && !self.over_soft_limit.swap(true, Ordering::SeqCst) {
            tracing::warn!(
                shard = self.shard_id,
                queued_commands = len,
                soft_limit = self.soft_limit,
                "read_state shard queue exceeded soft limit; buffering in-memory instead of dropping mutations"
            );
        }

        self.notify.notify_one();
        Ok(())
    }

    pub(crate) fn pop(&self) -> Option<ShardCommand> {
        let (command, len_after_pop) = {
            let mut commands = self.commands.lock().expect("command queue lock poisoned");
            let command = commands.pop_front();
            let len_after_pop = commands.len();
            (command, len_after_pop)
        };

        if len_after_pop <= self.soft_limit {
            self.over_soft_limit.store(false, Ordering::SeqCst);
        }

        command
    }

    pub(crate) async fn wait_for_signal(&self) {
        self.notify.notified().await;
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}
