use std::future::Future;
use std::pin::Pin;

use crate::ReadStateEntry;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ReadStateStore: Send + Sync {
    fn find_one(
        &self,
        user_id: i64,
        channel_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Option<ReadStateEntry>>>;
    fn find_all_for_user(
        &self,
        user_id: i64,
    ) -> BoxFuture<'_, ug_error::Result<Vec<(i64, ReadStateEntry)>>>;
    fn upsert<'a>(
        &'a self,
        user_id: i64,
        channel_id: i64,
        entry: &'a ReadStateEntry,
    ) -> BoxFuture<'a, ug_error::Result<()>>;
    fn delete(&self, user_id: i64, channel_id: i64) -> BoxFuture<'_, ug_error::Result<()>>;
}
