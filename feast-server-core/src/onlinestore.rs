//! Online store interface and implementations for different backends.
//! Contains logic for retrieving feature values from online stores.

mod redis;
pub mod sqlite_onlinestore;

use crate::config::OnlineStoreConfig;
use crate::feast::types::{EntityKey, Value};
use crate::model::{Feature, HashEntityKey};
use crate::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use lasso::{Spur, ThreadedRodeo};
use rustc_hash::FxHashMap as HashMap;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug)]
pub struct OnlineStoreRow {
    pub feature_view_name: Spur,
    pub entity_key: HashEntityKey,
    pub feature_name: Spur,
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
    rodeo: Arc<ThreadedRodeo>,
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
                rodeo,
            )
            .await
            .map(|sqlite| Arc::new(sqlite) as Arc<dyn OnlineStore>)
        }
        conf @ OnlineStoreConfig::Redis { .. } => {
            debug!("Create Redis online store");
            redis::from_config(project.to_string(), conf.clone(), rodeo).await
        }
        other => Err(anyhow!("Unsupported online store type: {:?}", other)),
    }
}
