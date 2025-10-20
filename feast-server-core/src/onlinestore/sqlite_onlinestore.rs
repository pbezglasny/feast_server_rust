use crate::config::EntityKeySerializationVersion;
use crate::feast::types::{EntityKey, Value};
use crate::key_serialization::deserialize_key;
use crate::key_serialization::serialize_key;
use crate::model::{Feature, HashEntityKey};
use crate::onlinestore::{OnlineStore, OnlineStoreRow};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use prost::Message;
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::{FromRow, Pool, Row, Sqlite};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::JoinSet;

pub struct ConnectionOptions {
    max_connections: u32,
    min_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Duration,
    test_before_acquire: bool,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            max_connections: 5,
            min_connections: 1,
            acquire_timeout: Duration::seconds(5),
            idle_timeout: Duration::seconds(600),
            test_before_acquire: true,
        }
    }
}

#[derive(Debug)]
pub struct SqliteStoreRow {
    pub entity_key: Vec<u8>,
    pub feature_name: String,
    pub value: Vec<u8>,
    pub event_ts: DateTime<Utc>,
    pub created_ts: DateTime<Utc>,
}

impl SqliteStoreRow {
    fn try_into_online_store_row(self, feature_view_name: &str) -> Result<OnlineStoreRow> {
        let Self {
            entity_key,
            feature_name,
            value,
            event_ts,
            created_ts,
        } = self;
        let decoded_value = Value::decode(value.as_slice()).with_context(|| {
            format!(
                "Failed to decode value for feature {}:{}",
                feature_view_name, feature_name
            )
        })?;
        let entity_key =
            deserialize_key(entity_key, EntityKeySerializationVersion::V3).map_err(|e| {
                anyhow!(
                    "Failed to deserialize entity key for feature view {}: {:?}",
                    feature_view_name,
                    e
                )
            })?;
        Ok(OnlineStoreRow {
            feature_view_name: feature_view_name.to_owned(),
            entity_key: HashEntityKey(Arc::new(entity_key)),
            feature_name,
            value: decoded_value,
            event_ts,
            created_ts: Some(created_ts),
        })
    }
}

impl FromRow<'_, SqliteRow> for SqliteStoreRow {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let entity_key: Vec<u8> = row.try_get("entity_key")?;
        let feature_name: String = row.try_get("feature_name")?;
        let value: Vec<u8> = row.try_get("value")?;
        let event_ts: DateTime<Utc> = row.try_get("event_ts")?;
        let created_ts: DateTime<Utc> = row.try_get("created_ts")?;
        Ok(Self {
            entity_key,
            feature_name,
            value,
            event_ts,
            created_ts,
        })
    }
}

pub struct SqliteOnlineStore {
    project: String,
    connection_pool: Pool<Sqlite>,
}

#[async_trait]
impl OnlineStore for SqliteOnlineStore {
    async fn get_feature_values(
        &self,
        features: HashMap<HashEntityKey, Vec<Arc<Feature>>>,
    ) -> Result<Vec<OnlineStoreRow>> {
        let mut view_to_keys: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();
        let mut view_features: HashMap<String, HashSet<String>> = HashMap::new();

        for (entity_key, feature_list) in features {
            let serialized_key = serialize_key(&entity_key.0, EntityKeySerializationVersion::V3)?;
            for feature in feature_list {
                let fv_name = feature.feature_view_name.clone();
                view_features
                    .entry(fv_name.clone())
                    .or_default()
                    .insert(feature.feature_name.clone());

                view_to_keys
                    .entry(fv_name)
                    .or_default()
                    .insert(serialized_key.clone());
            }
        }

        let mut join_set: JoinSet<Result<Vec<OnlineStoreRow>>> = JoinSet::new();
        for (view_name, serialized_keys) in view_to_keys {
            let features = view_features.remove(&view_name).unwrap_or_default();
            if serialized_keys.is_empty() || features.is_empty() {
                continue;
            }

            let mut connection = self.connection_pool.acquire().await?;
            let table_name = format!("{}_{}", self.project, view_name);

            join_set.spawn(async move {
                let entity_keys_parameters =
                    format!("?{}", ", ?".repeat(serialized_keys.len() - 1));
                let feature_parameters = format!("?{}", ", ?".repeat(features.len() - 1));
                let query = format!(
                    "SELECT entity_key, feature_name, value, event_ts, created_ts \
             FROM {} where entity_key in ({}) AND feature_name in ({})",
                    table_name, entity_keys_parameters, feature_parameters
                );
                let mut sqlx_query = sqlx::query_as(&query);
                for key in &serialized_keys {
                    sqlx_query = sqlx_query.bind(key);
                }
                for feature_name in features {
                    sqlx_query = sqlx_query.bind(feature_name);
                }
                match sqlx_query.fetch_all(&mut *connection).await {
                    Ok(rows) => rows
                        .into_iter()
                        .map(|r: SqliteStoreRow| r.try_into_online_store_row(&view_name))
                        .collect::<Result<Vec<_>>>(),
                    Err(sqlx::Error::Database(db_err))
                        if db_err.message().contains("no such table") =>
                    {
                        Ok(Vec::new())
                    }
                    Err(err) => Err(err.into()),
                }
            });
        }

        let mut feature_rows = Vec::new();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(val) => feature_rows.push(val),
                Err(e) => return Err(anyhow!("Error joining online feature task: {:?}", e)),
            }
        }
        let mut errors = vec![];
        let clean_data: Vec<OnlineStoreRow> = feature_rows
            .into_iter()
            .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
            .flatten()
            .collect();
        if !errors.is_empty() {
            return Err(anyhow!(
                "error while getting online data, errors: {:?}",
                errors
            ));
        }
        Ok(clean_data)
    }
}

impl SqliteOnlineStore {
    pub async fn from_options(
        path: &str,
        project: String,
        connection_options: ConnectionOptions,
    ) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(connection_options.max_connections)
            .min_connections(connection_options.min_connections)
            .acquire_timeout(
                connection_options
                    .acquire_timeout
                    .to_std()
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0)),
            )
            .idle_timeout(
                connection_options
                    .idle_timeout
                    .to_std()
                    .unwrap_or_else(|_| std::time::Duration::from_secs(0)),
            )
            .test_before_acquire(connection_options.test_before_acquire)
            .connect(path)
            .await?;
        Ok(Self {
            project,
            connection_pool: pool,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::feast::types::Value;
    use crate::feast::types::value::Val;

    #[tokio::test]
    async fn read_sqlite_trait() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let sqlite_path = format!("{}/test_data/online_store.db", project_dir);

        let entity_key = Arc::new(EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1005)),
            }],
        });

        let arg: HashMap<HashEntityKey, Vec<Arc<Feature>>> = HashMap::from([(
            HashEntityKey(entity_key),
            vec![Arc::new(Feature::new(
                "driver_hourly_stats".to_string(),
                "conv_rate".to_string(),
            ))],
        )]);

        let sqlite_store = SqliteOnlineStore::from_options(
            &sqlite_path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        let online_store: Box<dyn OnlineStore> = Box::new(sqlite_store);
        let result = online_store.get_feature_values(arg).await?;
        println!("{:?}", result);
        assert_eq!(result.len(), 1);
        Ok(())
    }
}
