use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::watch;

use crate::command_queue::{ShardCommand, ShardCommandQueue};
use crate::store::ReadStateStore;
use crate::{
    apply_ack, ReadStateAckKind, ReadStateAckRecord, ReadStateConfig, ReadStateEntry, ReadStateKey,
    ReadStateMutation,
};

struct CachedReadState {
    entry: ReadStateEntry,
    last_access: u64,
    pending_delete: bool,
    /// Highest message_id already counted by IncrementMention or fenced by ACK.
    /// Snowflake IDs are monotonic, replayed increments with the same or older message_id are skipped. 
    /// Seed from the persisted entry so historical JetStream redeliveries after restart do not re-increment mention_count that has already been flushed to Scylla.
    last_increment_message_id: i64,
}

impl CachedReadState {
    fn from_entry(entry: ReadStateEntry, last_access: u64) -> Self {
        let last_increment_message_id = entry
            .latest_channel_message_id
            .unwrap_or(0)
            .max(entry.last_read_message_id.unwrap_or(0));
        Self {
            entry,
            last_access,
            pending_delete: false,
            last_increment_message_id,
        }
    }

    fn default(last_access: u64) -> Self {
        Self::from_entry(ReadStateEntry::unread_seed(), last_access)
    }

    fn is_dirty(&self) -> bool {
        self.pending_delete || self.entry.dirty
    }
}

pub(crate) struct ReadStateShard {
    shard_id: usize,
    queue: Arc<ShardCommandQueue>,
    store: Arc<dyn ReadStateStore>,
    config: ReadStateConfig,
    entries: HashMap<ReadStateKey, CachedReadState>,
    user_channels: HashMap<i64, BTreeSet<i64>>,
    user_version_heads: HashMap<i64, i64>,
    lru_order: BTreeSet<(u64, i64, i64)>,
    fully_loaded_users: HashSet<i64>,
    access_clock: u64,
}

impl ReadStateShard {
    pub(crate) fn new(
        shard_id: usize,
        queue: Arc<ShardCommandQueue>,
        store: Arc<dyn ReadStateStore>,
        config: ReadStateConfig,
    ) -> Self {
        Self {
            shard_id,
            queue,
            store,
            config,
            entries: HashMap::new(),
            user_channels: HashMap::new(),
            user_version_heads: HashMap::new(),
            lru_order: BTreeSet::new(),
            fully_loaded_users: HashSet::new(),
            access_clock: 0,
        }
    }

    pub(crate) async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut flush_interval = tokio::time::interval(self.config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if let Some(command) = self.queue.pop() {
                self.handle_command(command).await;
                continue;
            }

            tokio::select! {
                _ = flush_interval.tick() => {
                    self.flush_dirty_entries().await;
                }
                result = shutdown_rx.changed() => {
                    match result {
                        Ok(()) if *shutdown_rx.borrow() => {
                            self.flush_on_shutdown().await;
                            break;
                        }
                        Ok(()) => {}
                        Err(_) => {
                            self.flush_on_shutdown().await;
                            break;
                        }
                    }
                }
                _ = self.queue.wait_for_signal() => {}
            }
        }

        self.queue.close();
    }

    async fn handle_command(&mut self, command: ShardCommand) {
        match command {
            ShardCommand::Mutate(mutation) => {
                self.apply_mutation(mutation).await;
                self.evict_if_needed().await;
            }
            ShardCommand::MutateAndPersist { mutation, response } => {
                let result = self.apply_mutation_and_persist(mutation).await;
                if response.send(result).is_err() {
                    tracing::warn!(
                        shard = self.shard_id,
                        "read_state mutate_and_persist receiver dropped before response"
                    );
                }
            }
            ShardCommand::AckAndPersist {
                user_id,
                channel_id,
                message_id,
                ack_kind,
                last_viewed,
                response,
            } => {
                let result = self
                    .apply_ack_and_persist(
                        user_id,
                        channel_id,
                        message_id,
                        ack_kind,
                        last_viewed,
                    )
                    .await;
                if response.send(result).is_err() {
                    tracing::warn!(
                        shard = self.shard_id,
                        user_id,
                        channel_id,
                        "read_state ack_and_persist receiver dropped before response"
                    );
                }
            }
            ShardCommand::Get {
                user_id,
                channel_id,
                response,
            } => {
                let result = self.get_entry(user_id, channel_id).await;
                if response.send(result).is_err() {
                    tracing::warn!(
                        shard = self.shard_id,
                        user_id,
                        channel_id,
                        "read_state get receiver dropped before response"
                    );
                }
            }
            ShardCommand::GetAllForUser { user_id, response } => {
                let result = self.get_all_for_user(user_id).await;
                if response.send(result).is_err() {
                    tracing::warn!(
                        shard = self.shard_id,
                        user_id,
                        "read_state get_all_for_user receiver dropped before response"
                    );
                }
            }
            #[cfg(test)]
            ShardCommand::ForceFlush { response } => {
                self.flush_dirty_entries().await;
                if response.send(()).is_err() {
                    tracing::warn!(
                        shard = self.shard_id,
                        "read_state force_flush receiver dropped before response"
                    );
                }
            }
        }
    }

    async fn apply_mutation(&mut self, mutation: ReadStateMutation) -> bool {
        match mutation {
            ReadStateMutation::IncrementMention {
                user_id,
                channel_id,
                message_id,
            } => {
                let Some(cached) = self.load_or_init(user_id, channel_id).await else {
                    return false;
                };
                if message_id <= cached.last_increment_message_id {
                    let last = cached.last_increment_message_id;
                    tracing::debug!(
                        shard = self.shard_id,
                        user_id,
                        channel_id,
                        message_id,
                        last,
                        "skipping duplicate IncrementMention (redelivery)"
                    );
                    return false;
                }
                cached.last_increment_message_id = message_id;
                cached.pending_delete = false;
                cached.entry.mention_count = cached.entry.mention_count.saturating_add(1);
                cached.entry.latest_channel_message_id = Some(
                    cached
                        .entry
                        .latest_channel_message_id
                        .map_or(message_id, |current| current.max(message_id)),
                );
                cached.entry.dirty = true;
                true
            }
            ReadStateMutation::ResetMentions {
                user_id,
                channel_id,
            } => {
                let Some(cached) = self.load_or_init(user_id, channel_id).await else {
                    return false;
                };
                cached.pending_delete = false;
                cached.entry.mention_count = 0;
                cached.entry.dirty = true;
                true
            }
            ReadStateMutation::SetMentionCount {
                user_id,
                channel_id,
                mention_count,
            } => {
                let Some(cached) = self.load_or_init(user_id, channel_id).await else {
                    return false;
                };
                cached.pending_delete = false;
                cached.entry.mention_count = mention_count;
                cached.entry.dirty = true;
                true
            }
            ReadStateMutation::Ack {
                user_id,
                channel_id,
                message_id,
                version,
            }
            | ReadStateMutation::BulkAck {
                user_id,
                channel_id,
                message_id,
                version,
            } => {
                let Some(cached) = self.load_or_init(user_id, channel_id).await else {
                    return false;
                };
                cached.pending_delete = false;
                let applied = apply_ack(&mut cached.entry, message_id, version);
                if applied {
                    // Advance the dedup fence to the acked position so
                    // historical IncrementMention redeliveries for
                    // messages <= acked_message_id cannot re-increment
                    // mention_count after a reconnect or restart.
                    cached.last_increment_message_id =
                        cached.last_increment_message_id.max(message_id);
                }
                applied
            }
            ReadStateMutation::Delete {
                user_id,
                channel_id,
            } => {
                let key = (user_id, channel_id);
                if !self.entries.contains_key(&key) {
                    let access = self.next_access();
                    self.insert_cached(key, CachedReadState::default(access));
                }

                self.touch_key(key);
                if let Some(cached) = self.entries.get_mut(&key) {
                    cached.pending_delete = true;
                    cached.entry.dirty = true;
                }
                true
            }
        }
    }

    async fn apply_mutation_and_persist(
        &mut self,
        mutation: ReadStateMutation,
    ) -> ug_error::Result<()> {
        let key = (mutation.user_id(), mutation.channel_id());
        if !self.apply_mutation(mutation).await {
            return Ok(());
        }
        self.persist_key(key, "immediate").await?;
        Ok(())
    }

    async fn apply_ack_and_persist(
        &mut self,
        user_id: i64,
        channel_id: i64,
        message_id: i64,
        ack_kind: ReadStateAckKind,
        last_viewed: Option<i32>,
    ) -> ug_error::Result<ReadStateAckRecord> {
        let version = self.next_version_for_user(user_id).await?;
        let mutation = match ack_kind {
            ReadStateAckKind::Ack => ReadStateMutation::Ack {
                user_id,
                channel_id,
                message_id,
                version,
            },
            ReadStateAckKind::BulkAck => ReadStateMutation::BulkAck {
                user_id,
                channel_id,
                message_id,
                version,
            },
        };
        // Detect the first ack to (user, channel) by reading the entry's prior version before the mutation applies.
        // load_or_init hydrates from the Scylla store on a cold cache; a brand-new entry hasversion = 0,
        // which is our "first ack" signal. 
        // The &mut borrow ends with the block so the follow-up apply_mutation call is free to take &mut self again.
        let prior_version: i64 = {
            match self.load_or_init(user_id, channel_id).await {
                Some(cached) => cached.entry.version,
                None => 0,
            }
        };
        let first_ack = prior_version == 0;
        if !self.apply_mutation(mutation).await {
            return Err(ug_error::UgError::internal(format!(
                "read_state ack mutation was not applied for user {user_id} channel {channel_id}"
            )));
        }
        // Persist the client-supplied last_viewed (Discord-epoch-day integer) if the request body carried one.
        // None preserves any prior value.
        // Setting dirty=true is defensive; apply_ack already marks dirty on a successful version advance, but this branch runs only when
        // the mutation applied, so the entry is already dirty. 
        // The explicit set guards future refactors where that invariant might change.
        if let Some(v) = last_viewed {
            if let Some(cached) = self.entries.get_mut(&(user_id, channel_id)) {
                cached.entry.last_viewed = Some(v);
                cached.entry.dirty = true;
            }
        }
        self.persist_key((user_id, channel_id), "immediate").await?;
        let entry = self.entries.get(&(user_id, channel_id)).ok_or_else(|| {
            ug_error::UgError::internal(format!(
                "read_state entry missing after ack persist for user {user_id} channel {channel_id}"
            ))
        })?;
        Ok(ReadStateAckRecord {
            channel_id,
            message_id,
            version,
            last_viewed: entry.entry.last_viewed,
            flags: entry.entry.flags,
            ack_kind,
            first_ack,
        })
    }

    async fn get_entry(&mut self, user_id: i64, channel_id: i64) -> Option<ReadStateEntry> {
        let key = (user_id, channel_id);
        if let Some(cached) = self.entries.get(&key) {
            if cached.pending_delete {
                self.touch_key(key);
                return None;
            }

            self.touch_key(key);
            return self.entries.get(&key).map(|entry| entry.entry.clone());
        }

        if self.fully_loaded_users.contains(&user_id) {
            return None;
        }

        match self.store.find_one(user_id, channel_id).await {
            Ok(Some(entry)) => {
                let access = self.next_access();
                self.insert_cached(key, CachedReadState::from_entry(entry.clone(), access));
                Some(entry)
            }
            Ok(None) => None,
            Err(error) => {
                tracing::error!(
                    shard = self.shard_id,
                    user_id,
                    channel_id,
                    error = %error,
                    error_kind = error.kind_str(),
                    "read_state find_one failed during cache miss load"
                );
                None
            }
        }
    }

    async fn get_all_for_user(&mut self, user_id: i64) -> Vec<(i64, ReadStateEntry)> {
        if !self.fully_loaded_users.contains(&user_id) {
            self.load_all_for_user(user_id).await;
        }

        let channel_ids: Vec<i64> = self
            .user_channels
            .get(&user_id)
            .map(|channels| channels.iter().copied().collect())
            .unwrap_or_default();

        let mut entries = Vec::with_capacity(channel_ids.len());
        for channel_id in channel_ids {
            let key = (user_id, channel_id);
            self.touch_key(key);
            if let Some(cached) = self.entries.get(&key) {
                if cached.pending_delete {
                    continue;
                }
                entries.push((channel_id, cached.entry.clone()));
            }
        }

        entries
    }

    async fn load_all_for_user(&mut self, user_id: i64) {
        match self.store.find_all_for_user(user_id).await {
            Ok(entries) => {
                for (channel_id, entry) in entries {
                    let key = (user_id, channel_id);
                    if self
                        .entries
                        .get(&key)
                        .is_some_and(|cached| cached.pending_delete || cached.entry.dirty)
                    {
                        continue;
                    }

                    let access = self.next_access();
                    self.insert_or_replace_cached(key, CachedReadState::from_entry(entry, access));
                }
                self.fully_loaded_users.insert(user_id);
                self.refresh_user_version_head(user_id);
            }
            Err(error) => {
                tracing::error!(
                    shard = self.shard_id,
                    user_id,
                    error = %error,
                    error_kind = error.kind_str(),
                    "read_state find_all_for_user failed during cache miss load"
                );
            }
        }
    }

    async fn next_version_for_user(&mut self, user_id: i64) -> ug_error::Result<i64> {
        if !self.user_version_heads.contains_key(&user_id) {
            self.seed_user_version_head(user_id).await?;
        }

        let next = self.user_version_heads.get_mut(&user_id).ok_or_else(|| {
            ug_error::UgError::internal(format!(
                "read_state version head missing for user {user_id}"
            ))
        })?;
        *next = next.saturating_add(1);
        Ok(*next)
    }

    fn refresh_user_version_head(&mut self, user_id: i64) {
        let max_version = self.user_max_cached_version(user_id);
        self.user_version_heads
            .entry(user_id)
            .and_modify(|head| {
                if *head < max_version {
                    *head = max_version;
                }
            })
            .or_insert(max_version);
    }

    fn user_max_cached_version(&self, user_id: i64) -> i64 {
        self.user_channels
            .get(&user_id)
            .into_iter()
            .flat_map(|channels| channels.iter())
            .filter_map(|channel_id| self.entries.get(&(user_id, *channel_id)))
            .map(|cached| cached.entry.version)
            .max()
            .unwrap_or(0)
    }

    async fn seed_user_version_head(&mut self, user_id: i64) -> ug_error::Result<()> {
        let entries = self.store.find_all_for_user(user_id).await?;
        for (channel_id, entry) in entries {
            let key = (user_id, channel_id);
            if self
                .entries
                .get(&key)
                .is_some_and(|cached| cached.pending_delete || cached.entry.dirty)
            {
                continue;
            }

            let access = self.next_access();
            self.insert_or_replace_cached(key, CachedReadState::from_entry(entry, access));
        }
        self.fully_loaded_users.insert(user_id);
        self.refresh_user_version_head(user_id);
        Ok(())
    }

    async fn load_or_init(
        &mut self,
        user_id: i64,
        channel_id: i64,
    ) -> Option<&mut CachedReadState> {
        let key = (user_id, channel_id);
        if !self.entries.contains_key(&key) {
            let access = self.next_access();
            let initial = match self.store.find_one(user_id, channel_id).await {
                Ok(Some(entry)) => CachedReadState::from_entry(entry, access),
                Ok(None) => CachedReadState::default(access),
                Err(error) => {
                    tracing::error!(
                        shard = self.shard_id,
                        user_id,
                        channel_id,
                        error = %error,
                        error_kind = error.kind_str(),
                        "read_state find_one failed during mutation load"
                    );
                    return None;
                }
            };

            self.insert_cached(key, initial);
        }

        self.touch_key(key);
        self.entries.get_mut(&key)
    }

    async fn flush_dirty_entries(&mut self) {
        let dirty_keys: Vec<ReadStateKey> = self
            .entries
            .iter()
            .filter_map(|(&key, cached)| cached.is_dirty().then_some(key))
            .collect();

        for key in dirty_keys {
            self.flush_key(key, "periodic").await;
        }
    }

    async fn flush_on_shutdown(&mut self) {
        let timeout = self.config.shutdown_timeout;
        if tokio::time::timeout(timeout, self.flush_dirty_entries())
            .await
            .is_err()
        {
            tracing::error!(
                shard = self.shard_id,
                timeout_ms = timeout.as_millis(),
                error_kind = "shutdown_timeout",
                "read_state shutdown flush timed out"
            );
        }
    }

    async fn flush_key(&mut self, key: ReadStateKey, reason: &str) -> bool {
        match self.persist_key(key, reason).await {
            Ok(should_remove) => should_remove,
            Err(error) => {
                tracing::error!(
                    shard = self.shard_id,
                    user_id = key.0,
                    channel_id = key.1,
                    reason,
                    error = %error,
                    error_kind = error.kind_str(),
                    "read_state flush failed"
                );
                false
            }
        }
    }

    async fn persist_key(&mut self, key: ReadStateKey, reason: &str) -> ug_error::Result<bool> {
        let Some(mut cached) = self.remove_cached(key) else {
            return Ok(false);
        };

        let should_remove = match self.flush_cached_entry(key, &mut cached, reason).await {
            Ok(should_remove) => should_remove,
            Err(error) => {
                self.insert_cached(key, cached);
                return Err(error);
            }
        };

        if !should_remove {
            self.insert_cached(key, cached);
        }

        Ok(should_remove)
    }

    async fn flush_cached_entry(
        &self,
        key: ReadStateKey,
        cached: &mut CachedReadState,
        reason: &str,
    ) -> ug_error::Result<bool> {
        if cached.pending_delete {
            self.store.delete(key.0, key.1).await?;
            tracing::debug!(
                shard = self.shard_id,
                user_id = key.0,
                channel_id = key.1,
                reason,
                "read_state delete flushed"
            );
            return Ok(true);
        }

        if !cached.entry.dirty {
            return Ok(false);
        }

        self.store.upsert(key.0, key.1, &cached.entry).await?;
        cached.entry.dirty = false;
        tracing::debug!(
            shard = self.shard_id,
            user_id = key.0,
            channel_id = key.1,
            reason,
            "read_state upsert flushed"
        );
        Ok(false)
    }

    async fn evict_if_needed(&mut self) {
        if self.entries.len() <= self.config.max_entries_per_shard {
            return;
        }

        let mut skipped = HashSet::new();
        for _attempt in 0..3 {
            let candidate = self.lru_order.iter().find_map(|&(_, user_id, channel_id)| {
                let key = (user_id, channel_id);
                (!skipped.contains(&key)).then_some(key)
            });

            let Some(candidate) = candidate else {
                break;
            };

            if self.entries[&candidate].is_dirty() {
                if self.flush_key(candidate, "eviction").await {
                    return;
                }

                tracing::warn!(
                    shard = self.shard_id,
                    user_id = candidate.0,
                    channel_id = candidate.1,
                    "read_state eviction skipped dirty entry after flush failure"
                );
                skipped.insert(candidate);
                continue;
            }

            self.remove_cached(candidate);
            return;
        }

        tracing::error!(
            shard = self.shard_id,
            entries = self.entries.len(),
            limit = self.config.max_entries_per_shard,
            error_kind = "shard_overflow",
            "read_state shard exceeded max_entries_per_shard; no entry could be safely evicted"
        );
    }

    fn insert_cached(&mut self, key: ReadStateKey, cached: CachedReadState) {
        self.user_channels.entry(key.0).or_default().insert(key.1);
        self.lru_order.insert((cached.last_access, key.0, key.1));
        self.entries.insert(key, cached);
    }

    fn insert_or_replace_cached(&mut self, key: ReadStateKey, cached: CachedReadState) {
        if let Some(previous) = self.entries.remove(&key) {
            self.lru_order.remove(&(previous.last_access, key.0, key.1));
        }
        self.insert_cached(key, cached);
    }

    fn remove_cached(&mut self, key: ReadStateKey) -> Option<CachedReadState> {
        let cached = self.entries.remove(&key)?;
        self.lru_order.remove(&(cached.last_access, key.0, key.1));
        if let Some(channels) = self.user_channels.get_mut(&key.0) {
            channels.remove(&key.1);
            if channels.is_empty() {
                self.user_channels.remove(&key.0);
            }
        }
        Some(cached)
    }

    fn touch_key(&mut self, key: ReadStateKey) {
        let access = self.next_access();
        if let Some(cached) = self.entries.get_mut(&key) {
            self.lru_order.remove(&(cached.last_access, key.0, key.1));
            cached.last_access = access;
            self.lru_order.insert((access, key.0, key.1));
        }
    }

    fn next_access(&mut self) -> u64 {
        self.access_clock = self.access_clock.wrapping_add(1);
        self.access_clock
    }
}
