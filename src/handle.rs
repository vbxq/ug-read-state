use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use ug_db::scylla::pool::ScyllaPool;
use ug_db::scylla::read_state_repo::ReadStateStatements;

use crate::command_queue::{ShardCommand, ShardCommandQueue};
use crate::scylla_store::ScyllaReadStateStore;
use crate::shard::ReadStateShard;
use crate::{
    ReadStateAckKind, ReadStateAckRecord, ReadStateConfig, ReadStateEntry, ReadStateMutation,
    ReadStateMutationError, ReadStateStore,
};

#[derive(Clone)]
pub struct ReadStateHandle {
    queues: Arc<Vec<Arc<ShardCommandQueue>>>,
    store: Arc<dyn ReadStateStore>,
    shutdown_tx: watch::Sender<bool>,
    join_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    is_shutdown: Arc<AtomicBool>,
}

impl ReadStateHandle {
    #[allow(clippy::unused_async)]
    pub async fn start(
        scylla: ScyllaPool,
        stmts: ReadStateStatements,
        config: ReadStateConfig,
    ) -> Self {
        let store = Arc::new(ScyllaReadStateStore::new(scylla, stmts)) as Arc<dyn ReadStateStore>;
        Self::start_with_store(store, config)
    }

    pub fn mutate(&self, mutation: ReadStateMutation) -> Result<(), ReadStateMutationError> {
        let shard = self.shard_for_user(mutation.user_id());
        if self.is_shutdown.load(Ordering::SeqCst) || self.queues[shard].is_closed() {
            return Err(ReadStateMutationError::ShardClosed { shard });
        }

        self.queues[shard]
            .push(ShardCommand::Mutate(mutation))
            .map_err(|()| ReadStateMutationError::ShardClosed { shard })
    }

    pub async fn mutate_and_persist(&self, mutation: ReadStateMutation) -> ug_error::Result<()> {
        let shard = self.shard_for_user(mutation.user_id());
        if self.is_shutdown.load(Ordering::SeqCst) || self.queues[shard].is_closed() {
            return Err(ug_error::UgError::internal(format!(
                "read_state shard {shard} closed"
            )));
        }

        let (tx, rx) = oneshot::channel();
        if self.queues[shard]
            .push(ShardCommand::MutateAndPersist {
                mutation,
                response: tx,
            })
            .is_err()
        {
            return Err(ug_error::UgError::internal(format!(
                "read_state shard {shard} closed"
            )));
        }

        rx.await.map_err(|error| {
            ug_error::UgError::internal(format!(
                "read_state shard {shard} response channel dropped: {error}"
            ))
        })?
    }

    pub async fn ack_and_persist(
        &self,
        user_id: i64,
        channel_id: i64,
        message_id: i64,
        ack_kind: ReadStateAckKind,
        last_viewed: Option<i32>,
    ) -> ug_error::Result<ReadStateAckRecord> {
        let shard = self.shard_for_user(user_id);
        if self.is_shutdown.load(Ordering::SeqCst) || self.queues[shard].is_closed() {
            return Err(ug_error::UgError::internal(format!(
                "read_state shard {shard} closed"
            )));
        }

        let (tx, rx) = oneshot::channel();
        if self.queues[shard]
            .push(ShardCommand::AckAndPersist {
                user_id,
                channel_id,
                message_id,
                ack_kind,
                last_viewed,
                response: tx,
            })
            .is_err()
        {
            return Err(ug_error::UgError::internal(format!(
                "read_state shard {shard} closed"
            )));
        }

        rx.await.map_err(|error| {
            ug_error::UgError::internal(format!(
                "read_state shard {shard} ack response channel dropped: {error}"
            ))
        })?
    }

    pub async fn get(&self, user_id: i64, channel_id: i64) -> Option<ReadStateEntry> {
        let shard = self.shard_for_user(user_id);
        if self.is_shutdown.load(Ordering::SeqCst) || self.queues[shard].is_closed() {
            tracing::error!(
                shard,
                user_id,
                channel_id,
                error_kind = "shard_closed",
                "read_state shard closed during get; falling back to store"
            );
            return self.direct_get(user_id, channel_id).await;
        }

        let (tx, rx) = oneshot::channel();
        if self.queues[shard]
            .push(ShardCommand::Get {
                user_id,
                channel_id,
                response: tx,
            })
            .is_err()
        {
            tracing::error!(
                shard,
                user_id,
                channel_id,
                error_kind = "shard_closed",
                "read_state shard closed during get enqueue; falling back to store"
            );
            return self.direct_get(user_id, channel_id).await;
        }

        match rx.await {
            Ok(result) => result,
            Err(error) => {
                tracing::error!(
                    shard,
                    user_id,
                    channel_id,
                    error = %error,
                    error_kind = "library_error",
                    "read_state get response channel dropped; falling back to store"
                );
                self.direct_get(user_id, channel_id).await
            }
        }
    }

    pub async fn get_all_for_user(&self, user_id: i64) -> Vec<(i64, ReadStateEntry)> {
        let shard = self.shard_for_user(user_id);
        if self.is_shutdown.load(Ordering::SeqCst) || self.queues[shard].is_closed() {
            tracing::error!(
                shard,
                user_id,
                error_kind = "shard_closed",
                "read_state shard closed during get_all_for_user; falling back to store"
            );
            return self.direct_get_all_for_user(user_id).await;
        }

        let (tx, rx) = oneshot::channel();
        if self.queues[shard]
            .push(ShardCommand::GetAllForUser {
                user_id,
                response: tx,
            })
            .is_err()
        {
            tracing::error!(
                shard,
                user_id,
                error_kind = "shard_closed",
                "read_state shard closed during get_all_for_user enqueue; falling back to store"
            );
            return self.direct_get_all_for_user(user_id).await;
        }

        match rx.await {
            Ok(result) => result,
            Err(error) => {
                tracing::error!(
                    shard,
                    user_id,
                    error = %error,
                    error_kind = "library_error",
                    "read_state get_all_for_user response channel dropped; falling back to store"
                );
                self.direct_get_all_for_user(user_id).await
            }
        }
    }

    pub async fn shutdown(&self) {
        if self.is_shutdown.swap(true, Ordering::SeqCst) {
            return;
        }

        if let Err(error) = self.shutdown_tx.send(true) {
            tracing::warn!(error = %error, "failed to broadcast read_state shutdown");
        }

        let mut join_handles = self.join_handles.lock().await;
        while let Some(handle) = join_handles.pop() {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "read_state shard task failed during shutdown");
            }
        }
    }

    pub(crate) fn start_with_store(
        store: Arc<dyn ReadStateStore>,
        config: ReadStateConfig,
    ) -> Self {
        assert!(
            config.shard_count > 0,
            "ReadStateConfig.shard_count must be > 0"
        );
        assert!(
            config.shard_channel_capacity > 0,
            "ReadStateConfig.shard_channel_capacity must be > 0"
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut queues = Vec::with_capacity(config.shard_count);
        let mut join_handles = Vec::with_capacity(config.shard_count);

        for shard_id in 0..config.shard_count {
            let queue = ShardCommandQueue::new(shard_id, config.shard_channel_capacity);
            let shard_store = Arc::clone(&store);
            let shard_config = config;
            let shard_shutdown = shutdown_rx.clone();
            let shard_queue = Arc::clone(&queue);

            join_handles.push(tokio::spawn(async move {
                let mut shard =
                    ReadStateShard::new(shard_id, shard_queue, shard_store, shard_config);
                shard.run(shard_shutdown).await;
            }));

            queues.push(queue);
        }

        Self {
            queues: Arc::new(queues),
            store,
            shutdown_tx,
            join_handles: Arc::new(Mutex::new(join_handles)),
            is_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) async fn force_flush(&self, user_id: i64) {
        let shard = self.shard_for_user(user_id);
        let (tx, rx) = oneshot::channel();
        self.queues[shard]
            .push(ShardCommand::ForceFlush { response: tx })
            .expect("force flush command should be accepted in tests");
        rx.await
            .expect("force flush response should arrive in tests");
    }

    fn shard_for_user(&self, user_id: i64) -> usize {
        let shard_count = i64::try_from(self.queues.len()).expect("queue count must fit in i64");
        let shard = user_id.rem_euclid(shard_count);
        usize::try_from(shard).expect("shard index must fit in usize")
    }

    async fn direct_get(&self, user_id: i64, channel_id: i64) -> Option<ReadStateEntry> {
        match self.store.find_one(user_id, channel_id).await {
            Ok(entry) => entry,
            Err(error) => {
                tracing::error!(
                    user_id,
                    channel_id,
                    error = %error,
                    error_kind = error.kind_str(),
                    "direct read_state get fallback failed"
                );
                None
            }
        }
    }

    async fn direct_get_all_for_user(&self, user_id: i64) -> Vec<(i64, ReadStateEntry)> {
        match self.store.find_all_for_user(user_id).await {
            Ok(entries) => entries,
            Err(error) => {
                tracing::error!(
                    user_id,
                    error = %error,
                    error_kind = error.kind_str(),
                    "direct read_state get_all_for_user fallback failed"
                );
                Vec::new()
            }
        }
    }
}
