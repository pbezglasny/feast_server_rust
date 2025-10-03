pub mod sqlite_onlinestore;

use crate::config::OnlineStoreConfig;
use crate::feast::types::EntityKey;
use crate::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug)]
pub struct OnlineStoreRow {
    pub feature_view_name: String,
    pub entity_key: Vec<u8>,
    pub feature_name: String,
    pub value: Vec<u8>,
    pub event_ts: SystemTime,
    pub created_ts: SystemTime,
}

#[async_trait]
pub trait OnlineStore: Send + Sync {
    async fn get_feature_values(
        &self,
        feature_view: &str,
        keys: &[EntityKey],
        requested_feature_names: &[&str],
    ) -> Result<Vec<OnlineStoreRow>>;
}

pub async fn get_online_store(
    online_store_config: &OnlineStoreConfig,
    project: &str,
    cwd: Option<&str>,
) -> Result<Arc<dyn OnlineStore>> {
    match online_store_config {
        OnlineStoreConfig::Sqlite { path } => {
            let full_path = cwd
                .map(|prefix| format!("{}/{}", prefix, path))
                .unwrap_or_else(|| path.to_string());
            SqliteOnlineStore::from_options(
                &full_path,
                project.to_owned(),
                ConnectionOptions::default(),
            )
            .await
            .map(|sqlite| Arc::new(sqlite) as Arc<dyn OnlineStore>)
        }
        other => Err(anyhow!("Unsupported online store type: {:?}", other)),
    }
}
