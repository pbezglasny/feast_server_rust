use crate::config::RegistryConfig;
use crate::model::{FeatureRegistry, FeatureService, FeatureView, GetOnlineFeaturesRequest};
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::{Result, anyhow};
use redis::AsyncTypedCommands;
use sqlx::pool::PoolOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Acquire, Database, Executor, Pool, Postgres};
use std::collections::HashMap;
use std::str::FromStr;

const FEAST_SQL_REGISTRY_MAX_CONNECTIONS_ENV_VAR: &str = "FEAST_SQL_REGISTRY_MAX_CONNECTIONS";
const DEFAULT_MAX_CONNECTIONS: u32 = 5;
const FEAST_SQL_REGISTRY_MIN_CONNECTIONS_ENV_VAR: &str = "FEAST_SQL_REGISTRY_MIN_CONNECTIONS";
const DEFAULT_MIN_CONNECTIONS: u32 = 1;

const FEAST_SQL_REGISTRY_USERNAME_ENV_VAR: &str = "FEAST_SQL_REGISTRY_USERNAME";
const FEAST_SQL_REGISTRY_PASSWORD_ENV_VAR: &str = "FEAST_SQL_REGISTRY_PASSWORD";

enum SqlRegistryType {
    Postgres,
    MySql,
}

impl FromStr for SqlRegistryType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            _ if s.starts_with("postgres") => Ok(SqlRegistryType::Postgres),
            _ if s.starts_with("mysql") => Ok(SqlRegistryType::MySql),
            _ => Err(anyhow!("Unsupported SQL registry type: {}", s)),
        }
    }
}

fn read_pool_options<DB: Database>(mut options: PoolOptions<DB>) -> Result<PoolOptions<DB>> {
    fn from_env_variable<T: FromStr + ToString>(key: &str, default: T) -> Result<T> {
        std::env::var(key)
            .or_else(|_| Ok(default.to_string()))
            .and_then(|conns| {
                conns
                    .parse::<T>()
                    .map_err(|_| anyhow!("Failed to parse {}", key))
            })
    }

    let max_connections: u32 = from_env_variable(
        FEAST_SQL_REGISTRY_MAX_CONNECTIONS_ENV_VAR,
        DEFAULT_MAX_CONNECTIONS,
    )?;
    let min_connections: u32 = from_env_variable(
        FEAST_SQL_REGISTRY_MIN_CONNECTIONS_ENV_VAR,
        DEFAULT_MIN_CONNECTIONS,
    )?;
    Ok(options
        .max_connections(max_connections)
        .min_connections(min_connections))
}

fn read_credentials(mut options: PgConnectOptions) -> Result<PgConnectOptions> {
    let username = std::env::var(FEAST_SQL_REGISTRY_USERNAME_ENV_VAR).ok();
    let password = std::env::var(FEAST_SQL_REGISTRY_PASSWORD_ENV_VAR).ok();

    if let Some(user) = username {
        options = options.username(&user);
    }
    if let Some(pass) = password {
        options = options.password(&pass);
    }
    Ok(options)
}
async fn new_postgres_connection(path: &str) -> Result<Pool<Postgres>> {
    let mut options = PgConnectOptions::from_str(path)?;
    options = read_credentials(options)?;
    let mut pool_options = PgPoolOptions::new();
    pool_options = read_pool_options(pool_options)?;
    pool_options.connect_with(options).await.map_err(Into::into)
}
pub(crate) async fn new(config: RegistryConfig, project: String) -> Result<SqlFeatureRegistry> {
    let registry_type = SqlRegistryType::from_str(&config.path)?;
    match registry_type {
        SqlRegistryType::Postgres => {
            let pool = new_postgres_connection(&config.path).await?;
            let registry = SqlFeatureRegistry {
                project,
                connection_pool: pool,
            };
            Ok(registry)
        }
        SqlRegistryType::MySql => Err(anyhow!("MySQL registry not yet implemented")),
    }
}

pub(crate) struct SqlFeatureRegistry {
    project: String,
    connection_pool: Pool<Postgres>,
}

impl SqlFeatureRegistry {
    pub async fn query_registry(&self) -> Result<FileFeatureRegistry> {
        let mut connection = self.connection_pool.acquire().await?;
        let entities_vec: Vec<(String, Vec<u8>)> =
            sqlx::query_as("SELECT entity_name, entity_proto FROM entities WHERE project_id=$1")
                .bind(&self.project)
                .fetch_all(&mut *connection)
                .await?;
        let entities: HashMap<String, crate::model::Entity> = entities_vec
            .into_iter()
            .map(|(name, proto)| {
                let entity =
                    crate::model::Entity::try_from(proto).expect("Failed to convert Entity proto");
                (name, entity)
            })
            .collect();
        let feature_views_vec: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT feature_view_name, feature_view_proto FROM feature_views WHERE project_id=$1",
        )
        .bind(&self.project)
        .fetch_all(&mut *connection)
        .await?;
        let feature_views: HashMap<String, FeatureView> = feature_views_vec
            .into_iter()
            .map(|(name, proto)| {
                let fv = FeatureView::try_from(proto)
                    .map_err(|e| anyhow!("Failed to convert FeatureView proto for '{}': {}", name, e))?;
                (name, fv)
            })
            .collect();
        let feature_services_vec: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT feature_service_name, feature_service_proto FROM feature_services WHERE project_id=$1",
        )
            .bind(&self.project)
            .fetch_all(&mut *connection)
            .await?;
        let on_demand_features_vec: Vec<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT feature_view_name, feature_view_proto FROM on_demand_feature_views WHERE project_id=$1",
        )
            .bind(&self.project)
            .fetch_all(&mut *connection)
            .await?;
        let on_demand_feature_views: HashMap<String, crate::model::OnDemandFeatureView> =
            on_demand_features_vec
                .into_iter()
                .map(|(name, proto)| {
                    let odf = crate::model::OnDemandFeatureView::try_from(proto)
                        .expect("Failed to convert OnDemandFeatureView proto");
                    (name, odf)
                })
                .collect();
        let feature_services: HashMap<String, FeatureService> = feature_services_vec
            .into_iter()
            .map(|(name, proto)| {
                let fs = FeatureService::try_from(proto)
                    .expect("Failed to convert FeatureService proto");
                (name, fs)
            })
            .collect();
        Ok(FileFeatureRegistry::from_registry(FeatureRegistry {
            entities,
            feature_views,
            on_demand_features: on_demand_feature_views,
            feature_services,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[tokio::test]
    #[ignore]
    async fn test_feature_service() -> Result<()> {
        let config = RegistryConfig {
            path: "postgresql://postgres:mysecretpassword@127.0.0.1:5432/postgres".to_string(),
            cache_ttl_seconds: Some(60),
            ..Default::default()
        };
        let registry = new(config, "careful_tomcat".to_string()).await?;

        let request = GetOnlineFeaturesRequest {
            entities: HashMap::new(),
            feature_service: None,
            features: vec!["driver_hourly_stats_fresh:conv_rate".to_string()].into(),
            ..Default::default()
        };
        let registry_data = registry.query_registry().await?;
        println!("{:#?}", registry_data);
        Ok(())
    }
}
