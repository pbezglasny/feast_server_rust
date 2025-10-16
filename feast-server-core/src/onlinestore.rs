mod redis;
pub mod sqlite_onlinestore;

use crate::config::OnlineStoreConfig;
use crate::feast::types::{EntityKey, Value};
use crate::model::{Feature, HashEntityKey};
use crate::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct OnlineStoreRow {
    pub feature_view_name: String,
    pub entity_key: HashEntityKey,
    pub feature_name: String,
    pub value: Value,
    pub event_ts: DateTime<Utc>,
    pub created_ts: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait OnlineStore: Send + Sync + 'static {
    async fn get_feature_values(
        &self,
        features: HashMap<HashEntityKey, Vec<Feature>>,
    ) -> Result<Vec<OnlineStoreRow>>;
}

pub async fn get_online_store(
    online_store_config: &OnlineStoreConfig,
    project: &str,
    cwd: Option<&str>,
) -> Result<Arc<dyn OnlineStore>> {
    match online_store_config {
        OnlineStoreConfig::Sqlite { path } => {
            debug!("Create SQLite online store with path: {}", path);
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
        OnlineStoreConfig::Redis { connection_string } => {
            debug!("Create Redis online store");
            redis::RedisOnlineStore::from_connection_string(
                project.to_owned(),
                connection_string.clone(),
            )
            .await
            .map(|redis| Arc::new(redis) as Arc<dyn OnlineStore>)
        }
        other => Err(anyhow!("Unsupported online store type: {:?}", other)),
    }
}
