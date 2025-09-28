use crate::config::EntityKeySerializationVersion;
use crate::feast::types::EntityKey;
use crate::key_serialization::serialize_key;
use crate::onlinestore::{OnlineStore, OnlineStoreRow};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::{FromRow, Pool, Row, Sqlite};
use std::time::{Duration, SystemTime};

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
            acquire_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(600),
            test_before_acquire: true,
        }
    }
}

#[derive(Debug)]
pub struct SqliteStoreRow {
    pub entity_key: Vec<u8>,
    pub feature_name: String,
    pub value: Vec<u8>,
    pub event_ts: SystemTime,
    pub created_ts: SystemTime,
}

impl SqliteStoreRow {
    fn converto_to_online_store_row(self, feature_view_name: &str) -> OnlineStoreRow {
        OnlineStoreRow {
            feature_view_name: feature_view_name.to_owned(),
            entity_key: self.entity_key,
            feature_name: self.feature_name,
            value: self.value,
            event_ts: self.event_ts,
            created_ts: self.created_ts,
        }
    }
}

impl FromRow<'_, SqliteRow> for SqliteStoreRow {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let entity_key: Vec<u8> = row.try_get("entity_key")?;
        let feature_name: String = row.try_get("feature_name")?;
        let value: Vec<u8> = row.try_get("value")?;
        let event_ts: DateTime<Utc> = row.try_get("event_ts")?;
        let created_ts: DateTime<Utc> = row.try_get("event_ts")?;
        Ok(Self {
            entity_key,
            feature_name,
            value,
            event_ts: event_ts.into(),
            created_ts: created_ts.into(),
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
        feature_view: &str,
        keys: &[EntityKey],
        requested_feature_names: &[&str],
    ) -> Result<Vec<OnlineStoreRow>> {
        let mut connection = self.connection_pool.acquire().await?;
        let table_name = format!("{}_{}", self.project, feature_view);

        let serialized_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|key| serialize_key(key, EntityKeySerializationVersion::V3))
            .collect::<Result<Vec<_>>>()?;
        let entity_keys_parameters = format!("?{}", ", ?".repeat(serialized_keys.len() - 1));
        let feature_parameters = format!("?{}", ", ?".repeat(requested_feature_names.len() - 1));
        let query = format!(
            "SELECT * FROM {} where entity_key in ({}) AND feature_name in ({})",
            table_name, entity_keys_parameters, feature_parameters
        );
        let mut sqlx_query = sqlx::query_as(&query);
        for key in &serialized_keys {
            sqlx_query = sqlx_query.bind(key);
        }
        for feature_name in requested_feature_names {
            sqlx_query = sqlx_query.bind(feature_name);
        }
        let result: Vec<SqliteStoreRow> = sqlx_query.fetch_all(&mut *connection).await?;
        Ok(result
            .into_iter()
            .map(|r| r.converto_to_online_store_row(feature_view))
            .collect())
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
            .acquire_timeout(connection_options.acquire_timeout)
            .idle_timeout(connection_options.idle_timeout)
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
        let path =
            "/Users/pavel/work/rust/feast_rust/dev/golden_hornet/feature_repo/data/online_store.db";

        let entity_key = EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1005)),
            }],
        };
        let keys = vec![entity_key];
        let features = vec!["conv_rate"];

        let sqlite_store = SqliteOnlineStore::from_options(
            path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        let online_store: Box<dyn OnlineStore> = Box::new(sqlite_store);
        let result = online_store
            .get_feature_values("driver_hourly_stats", &keys, &features)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
}
