use crate::config::{OnlineStoreConfig, RedisType};
use crate::feast::types::Value as FeastValue;
use crate::model::{Feature, HashEntityKey};
use crate::onlinestore::{OnlineStore, OnlineStoreRow};
use crate::util::read_file_to_bytes;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prost::Message;
use prost_types::Timestamp;
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::cluster::{ClusterClient, ClusterClientBuilder};
use redis::cluster_async::ClusterConnection;
use redis::{
    AsyncCommands, Client, ClientTlsConfig, ConnectionAddr, ConnectionInfo, FromRedisValue,
    IntoConnectionInfo, RedisConnectionInfo, RedisResult, TlsCertificates,
};
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

fn parse_redis_connection_string(connection_string: &str) -> Result<RedisConnectionOption> {
    let mut result = RedisConnectionOption::default();
    let mut common_options = CommonConnectionOptions::default();
    for (i, part) in connection_string.split(',').enumerate() {
        if part.contains(':') && part.matches(':').count() == 1 {
            if let Some((host, port_str)) = part.split_once(":") {
                let port = port_str
                    .parse::<u16>()
                    .with_context(|| format!("Failed to parse port '{}'", port_str))?;
                result.hosts.push((host.to_string(), port));
            } else {
                return Err(anyhow!("Invalid connection URL of host at index {}", i));
            }
        } else if part.contains('=') && part.matches('=').count() == 1 {
            if let Some((key, value)) = part.split_once("=") {
                parse_common_options(&mut common_options, i, key, value)?;
            } else {
                return Err(anyhow!("Invalid connection option at index {}", i));
            }
        } else {
            return Err(anyhow!(
                "Invalid connection URL part at index {}: {}",
                i,
                part
            ));
        }
    }
    result.common_options = common_options;
    Ok(result)
}

trait GetConnection {
    fn get_connection(&self) -> impl ConnectionLike + Send + Sync;
}

trait GetProject {
    fn get_project(&self) -> String;
}

pub(crate) struct RedisSingleNodeOnlineStore {
    project: String,
    connection_pool: ConnectionManager,
}

impl GetConnection for RedisSingleNodeOnlineStore {
    fn get_connection(&self) -> impl ConnectionLike + Send + Sync {
        self.connection_pool.clone()
    }
}

impl GetProject for RedisSingleNodeOnlineStore {
    fn get_project(&self) -> String {
        self.project.clone()
    }
}

pub(crate) struct RedisClusterOnlineStore {
    project: String,
    connection_pool: ClusterConnection,
}

impl GetConnection for RedisClusterOnlineStore {
    fn get_connection(&self) -> impl ConnectionLike + Send + Sync {
        self.connection_pool.clone()
    }
}

impl GetProject for RedisClusterOnlineStore {
    fn get_project(&self) -> String {
        self.project.clone()
    }
}

#[derive(Debug, Default, Clone)]
struct CommonConnectionOptions {
    password: Option<String>,
    username: Option<String>,
    ssl: Option<bool>,
    db: Option<i64>,
    ssl_certfile: Option<String>,
    ssl_keyfile: Option<String>,
    ssl_ca_certs: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct RedisConnectionOption {
    hosts: Vec<(String, u16)>,
    common_options: CommonConnectionOptions,
}

#[derive(Debug, Default, Clone)]
struct SingleNodeConnectionOption {
    host: String,
    port: u16,
    common_options: CommonConnectionOptions,
}

impl TryFrom<&CommonConnectionOptions> for TlsCertificates {
    type Error = anyhow::Error;

    fn try_from(value: &CommonConnectionOptions) -> Result<Self> {
        match (value.ssl_keyfile.as_ref(), value.ssl_certfile.as_ref()) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(anyhow!(
                    "Both ssl_keyfile and ssl_certfile must be provided together or neither"
                ));
            }
            _ => {}
        }
        let client_tls: Option<ClientTlsConfig> = if let (Some(cert), Some(key)) =
            (value.ssl_certfile.clone(), value.ssl_keyfile.clone())
        {
            Some(ClientTlsConfig {
                client_cert: read_file_to_bytes(&cert)?,
                client_key: read_file_to_bytes(&key)?,
            })
        } else {
            None
        };
        Ok(TlsCertificates {
            client_tls,
            root_cert: value
                .ssl_ca_certs
                .clone()
                .map(|cert_path| read_file_to_bytes(&cert_path))
                .transpose()?,
        })
    }
}

impl TryFrom<RedisConnectionOption> for SingleNodeConnectionOption {
    type Error = anyhow::Error;

    fn try_from(value: RedisConnectionOption) -> Result<Self, Self::Error> {
        if value.hosts.len() != 1 {
            return Err(anyhow!(
                "Expected single host for SingleNodeConnectionOption, got {}",
                value.hosts.len()
            ));
        }
        let (host, port) = &value.hosts[0];
        Ok(SingleNodeConnectionOption {
            host: host.clone(),
            port: *port,
            common_options: value.common_options,
        })
    }
}

impl IntoConnectionInfo for SingleNodeConnectionOption {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        let mut redis = RedisConnectionInfo::default();
        redis.username = self.common_options.username;
        redis.password = self.common_options.password;
        if let Some(db) = self.common_options.db {
            redis.db = db;
        }
        let addr: ConnectionAddr = ConnectionAddr::Tcp(self.host, self.port);
        Ok(ConnectionInfo { addr, redis })
    }
}

struct RedisClusterHost {
    host: String,
    port: u16,
    db: Option<i64>,
}

impl From<RedisConnectionOption> for Vec<RedisClusterHost> {
    fn from(value: RedisConnectionOption) -> Self {
        let db = value.common_options.db;
        value
            .hosts
            .into_iter()
            .map(|(host, port)| RedisClusterHost { host, port, db })
            .collect()
    }
}

impl IntoConnectionInfo for RedisClusterHost {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        let conn_address = ConnectionAddr::Tcp(self.host, self.port);
        let mut redis_info = RedisConnectionInfo::default();
        if let Some(db) = self.db {
            redis_info.db = db;
        }
        Ok(ConnectionInfo {
            addr: conn_address,
            redis: redis_info,
        })
    }
}

impl TryFrom<RedisConnectionOption> for ClusterClient {
    type Error = anyhow::Error;

    fn try_from(value: RedisConnectionOption) -> Result<Self> {
        let hosts: Vec<RedisClusterHost> = value.clone().into();
        let mut builder = ClusterClientBuilder::new(hosts);
        let RedisConnectionOption {
            hosts: _,
            common_options,
        } = value;
        if let Some(enabled) = common_options.ssl
            && enabled
        {
            let certificates = TlsCertificates::try_from(&common_options)?;
            builder = builder.certs(certificates);
        }
        if let Some(username) = common_options.username {
            builder = builder.username(username);
        }
        if let Some(password) = common_options.password {
            builder = builder.password(password);
        }
        Ok(builder.build()?)
    }
}

fn parse_common_options(
    result: &mut CommonConnectionOptions,
    i: usize,
    key: &str,
    value: &str,
) -> Result<()> {
    match key.to_ascii_lowercase().as_str() {
        "password" => result.password = Some(value.to_string()),
        "username" => result.username = Some(value.to_string()),
        "db" => result.db = Some(value.parse::<i64>()?),
        "ssl" => {
            let ssl_value = match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => true,
                "false" | "0" | "no" => false,
                _ => {
                    return Err(anyhow!(
                        "Invalid ssl value at index {}: {}, supported values are 'true', 'false', '1', '0', 'yes', 'no'",
                        i,
                        value
                    ));
                }
            };
            result.ssl = Some(ssl_value);
        }
        "ssl_certfile" => result.ssl_certfile = Some(value.to_string()),
        "ssl_keyfile" => result.ssl_keyfile = Some(value.to_string()),
        "ssl_ca_certs" => result.ssl_ca_certs = Some(value.to_string()),
        other => {
            return Err(anyhow!(
                "Invalid connection option at index {}: {}",
                i,
                other
            ));
        }
    }
    Ok(())
}

async fn check_redis_connection(client: &Client) -> Result<()> {
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
    Ok(())
}

pub async fn new(
    project: String,
    redis_type: RedisType,
    connection_string: String,
    sentinel_master: Option<String>,
) -> Result<Arc<dyn OnlineStore>> {
    match redis_type {
        RedisType::SingleNode => {
            let connection_option = parse_redis_connection_string(&connection_string)?;
            let certificates = TlsCertificates::try_from(&connection_option.common_options)?;
            let single_node_option = SingleNodeConnectionOption::try_from(connection_option)?;
            let client = Client::build_with_tls(single_node_option, certificates)?;

            check_redis_connection(&client).await?;
            let connection_pool = ConnectionManager::new(client).await?;
            Ok(Arc::new(RedisSingleNodeOnlineStore {
                project,
                connection_pool,
            }))
        }
        RedisType::RedisCluster => {
            let connection_option = parse_redis_connection_string(&connection_string)?;
            let cluster_client = ClusterClient::try_from(connection_option)?;
            let mut connection_pool = cluster_client
                .get_async_connection()
                .await
                .with_context(|| anyhow!("Cannot establish redis cluster connection"))?;

            Ok(Arc::new(RedisClusterOnlineStore {
                project,
                connection_pool,
            }))
        }
        RedisType::Sentinel => Err(anyhow!("Sentinel Redis type is not supported yet")),
    }
}
pub async fn from_config(
    project: String,
    config: OnlineStoreConfig,
) -> Result<Arc<dyn OnlineStore>> {
    match config {
        OnlineStoreConfig::Redis {
            redis_type,
            connection_string,
            sentinel_master,
        } => new(project, redis_type, connection_string, sentinel_master).await,
        _ => Err(anyhow!("Invalid config for RedisOnlineStore")),
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
impl<T> OnlineStore for T
where
    T: GetConnection + GetProject + Send + Sync + 'static,
{
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
            hset_entity_key.extend_from_slice(self.get_project().as_bytes());
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

        let mut connection = self.get_connection();

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
    use redis::cluster::ClusterClientBuilder;
    use std::collections::HashMap;
    use std::sync::Arc;

    impl super::RedisSingleNodeOnlineStore {
        async fn new_from_manager(
            project: String,
            connection_pool: ConnectionManager,
        ) -> Result<Self> {
            Ok(Self {
                project,
                connection_pool,
            })
        }
    }

    #[tokio::test]
    #[ignore]
    async fn trait_test() -> Result<()> {
        let client = redis::Client::open("redis://127.0.0.1:7000/")?;
        let con = client.get_connection_manager().await?;
        let redis_store =
            super::RedisSingleNodeOnlineStore::new_from_manager("careful_tomcat".to_string(), con)
                .await?;
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
