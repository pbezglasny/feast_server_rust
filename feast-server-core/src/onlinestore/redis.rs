use crate::config::OnlineStoreConfig;
use crate::feast::types::Value as FeastValue;
use crate::model::{Feature, HashEntityKey};
use crate::onlinestore::{OnlineStore, OnlineStoreRow};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use prost_types::Timestamp;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, FromRedisValue};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

fn feature_redis_key(feature: &Feature) -> Result<Vec<u8>> {
    let mut key_bytes = feature.feature_view_name.as_bytes().to_vec();
    key_bytes.push(b':');
    key_bytes.extend_from_slice(feature.feature_name.as_bytes());
    let hashed_key = murmur3::murmur3_32(&mut std::io::Cursor::new(&key_bytes), 0)?;
    Ok(hashed_key.to_le_bytes().to_vec())
}

pub struct RedisOnlineStore {
    project: String,
    connection_pool: ConnectionManager,
}

fn add_redis_prefix_to_connection_string(connection_string: &str) -> String {
    if connection_string.starts_with("redis://") || connection_string.starts_with("rediss://") {
        connection_string.to_string()
    } else {
        format!("redis://{}", connection_string)
    }
}

impl RedisOnlineStore {
    pub async fn from_config(project: String, config: OnlineStoreConfig) -> Result<Self> {
        match config {
            OnlineStoreConfig::Redis { connection_string } => {
                Self::from_connection_string(project, &connection_string).await
            }
            _ => Err(anyhow!("Invalid config for RedisOnlineStore")),
        }
    }

    pub async fn from_connection_string(project: String, connection_string: &str) -> Result<Self> {
        let connection_info = add_redis_prefix_to_connection_string(connection_string);
        let client = redis::Client::open(connection_info.as_str())
            .map_err(|e| anyhow!("Failed to create Redis client: {}", e))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .with_context(|| anyhow!("Cannot establish redis connection"))?;
        let ping_response: String = redis::cmd("PING").query_async(&mut conn).await?;
        if ping_response.to_uppercase() != "PONG" {
            return Err(anyhow!(
                "Failed to connect to Redis online store, unexpected PING response: {}",
                ping_response
            ));
        }
        let connection_pool = ConnectionManager::new(client).await?;

        Ok(Self {
            project,
            connection_pool,
        })
    }
}

enum RedisRequest<'a> {
    FeatureRow {
        feature_view_name: &'a str,
        entity_key: &'a HashEntityKey,
        feature_name: &'a str,
    },
    TimestampRow {
        entity_key: &'a HashEntityKey,
        feature_view_name: &'a str,
    },
}

#[async_trait]
impl OnlineStore for RedisOnlineStore {
    async fn get_feature_values(
        &self,
        features: HashMap<HashEntityKey, Vec<Arc<Feature>>>,
    ) -> Result<Vec<OnlineStoreRow>> {
        let mut entities: Vec<RedisRequest> = vec![];

        let mut pipeline = redis::pipe();

        for (key, feature_vec) in features.iter() {
            let mut seen_views: HashSet<&str> = HashSet::new();
            let mut feature_keys: Vec<Vec<u8>> = vec![];
            let mut hset_entity_key = crate::key_serialization::serialize_key(
                &key.0,
                crate::config::EntityKeySerializationVersion::V3,
            )?;
            hset_entity_key.extend_from_slice(self.project.as_bytes());
            for feature in feature_vec {
                if !seen_views.contains(&feature.feature_view_name.as_ref()) {
                    seen_views.insert(feature.feature_view_name.as_ref());
                    feature_keys.push(
                        ["_ts:", feature.feature_view_name.as_ref()]
                            .concat()
                            .as_bytes()
                            .to_vec(),
                    );
                    entities.push(RedisRequest::TimestampRow {
                        entity_key: key,
                        feature_view_name: &feature.feature_view_name,
                    });
                }
                feature_keys.push(feature_redis_key(feature)?);
                entities.push(RedisRequest::FeatureRow {
                    feature_view_name: &feature.feature_view_name,
                    entity_key: key,
                    feature_name: &feature.feature_name,
                });
            }

            pipeline.cmd("HMGET").arg(hset_entity_key).arg(feature_keys);
        }

        let mut connection = self.connection_pool.clone();

        let results: Vec<Vec<Option<Vec<u8>>>> = pipeline.query_async(&mut connection).await?;
        let result_count: usize = results.iter().map(|v| v.len()).sum();
        if result_count != entities.len() {
            return Err(anyhow!(
                "Mismatched number of results: expected {}, got {}",
                entities.len(),
                result_count
            ));
        }
        let mut result_rows: Vec<OnlineStoreRow> = vec![];
        let mut timestamp_map: HashMap<(&str, &HashEntityKey), Option<DateTime<Utc>>> =
            HashMap::new();
        for (request, value) in entities.into_iter().zip(results.into_iter().flatten()) {
            match request {
                RedisRequest::FeatureRow {
                    feature_view_name,
                    entity_key,
                    feature_name,
                } => {
                    let ts = timestamp_map
                        .get(&(feature_view_name, entity_key))
                        .cloned()
                        .flatten()
                        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                    let decoded_value = match value {
                        Some(bytes) => FeastValue::decode(bytes.as_slice()).with_context(|| {
                            format!(
                                "Failed to decode value for feature {}:{} from bytes: {:?}",
                                feature_view_name, feature_name, bytes
                            )
                        })?,
                        None => FeastValue::default(),
                    };
                    result_rows.push(OnlineStoreRow {
                        feature_view_name: feature_view_name.to_string(),
                        entity_key: entity_key.clone(),
                        feature_name: feature_name.to_string(),
                        value: decoded_value,
                        event_ts: ts,
                        created_ts: None,
                    });
                }
                RedisRequest::TimestampRow {
                    entity_key,
                    feature_view_name,
                } => {
                    let ts = match value {
                        Some(bytes) => {
                            let timestamp_proto = Timestamp::decode(bytes.as_slice())
                                .with_context(|| {
                                    format!(
                                        "Failed to decode timestamp for feature view {}",
                                        feature_view_name
                                    )
                                })?;
                            DateTime::<Utc>::from_timestamp(
                                timestamp_proto.seconds,
                                timestamp_proto.nanos.max(0) as u32,
                            )
                        }
                        None => None,
                    };
                    timestamp_map.insert((feature_view_name, entity_key), ts);
                }
            }
        }

        Ok(result_rows)
    }
}
#[cfg(test)]
mod tests {
    use crate::feast::types::value::Val;
    use crate::feast::types::{EntityKey, Value};
    use crate::model::{Feature, HashEntityKey};
    use crate::onlinestore::OnlineStore;
    use anyhow::Result;
    use redis::aio::ConnectionManager;
    use std::collections::HashMap;
    use std::sync::Arc;

    impl super::RedisOnlineStore {
        async fn new(project: String, connection_pool: ConnectionManager) -> Result<Self> {
            Ok(Self {
                project,
                connection_pool,
            })
        }
    }

    #[tokio::test]
    #[ignore]
    async fn trait_test() -> Result<()> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let con = client.get_connection_manager().await?;
        let redis_store = super::RedisOnlineStore::new("careful_tomcat".to_string(), con).await?;
        let arg = HashMap::from([(
            HashEntityKey(Arc::new(EntityKey {
                join_keys: vec!["driver_id".to_string()],
                entity_values: vec![Value {
                    val: Some(Val::Int64Val(1005)),
                }],
            })),
            vec![
                Arc::new(Feature::new(
                    "driver_hourly_stats".to_string(),
                    "conv_rate".to_string(),
                )),
                Arc::new(Feature::new(
                    "driver_hourly_stats".to_string(),
                    "acc_rate".to_string(),
                )),
            ],
        )]);
        let result = redis_store.get_feature_values(arg).await?;
        println!("result: {:?}", result);
        Ok(())
    }
}
