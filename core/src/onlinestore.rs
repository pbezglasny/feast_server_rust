mod sqlite_onlinestore;

use crate::feast::types::EntityKey;
use anyhow::Result;
use async_trait::async_trait;
use std::time::SystemTime;

#[derive(Debug)]
pub struct OnlineStoreRow {
    pub entity_key: Vec<u8>,
    pub feature_name: String,
    pub value: Vec<u8>,
    pub event_ts: SystemTime,
    pub created_ts: SystemTime,
}

#[async_trait]
pub trait OnlineStore {
    async fn get_feature_values(
        &self,
        feature_view: &str,
        keys: &[EntityKey],
        requested_feature_names: &[&str],
    ) -> Result<Vec<OnlineStoreRow>>;
}
