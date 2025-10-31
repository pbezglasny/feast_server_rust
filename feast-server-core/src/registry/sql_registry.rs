use crate::config::RegistryConfig;
use crate::model::{
    Entity, FeatureRegistry, FeatureService, FeatureView, GetOnlineFeaturesRequest,
};
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::{Result, anyhow};
use lasso::{Spur, ThreadedRodeo};
use rustc_hash::FxHashMap as HashMap;
use sqlx::pool::PoolOptions;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Acquire, Database, Executor, Pool, Postgres};
use std::str::FromStr;
use std::sync::Arc;

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
/// Establishes a new PostgreSQL connection pool.
///
/// # Parameters
/// - `path`: PostgreSQL connection string, e.g. `"postgres://user:password@host:port/database"`.
///   The username and password in the connection string can be overridden by the
///   `FEAST_SQL_REGISTRY_USERNAME` and `FEAST_SQL_REGISTRY_PASSWORD` environment variables.
///
/// # Returns
/// A connection pool to the PostgreSQL database.
async fn new_postgres_connection(path: &str) -> Result<Pool<Postgres>> {
    let mut options = PgConnectOptions::from_str(path)?;
    options = read_credentials(options)?;
    let mut pool_options = PgPoolOptions::new();
    pool_options = read_pool_options(pool_options)?;
    pool_options.connect_with(options).await.map_err(Into::into)
}
pub(crate) async fn new(
    config: RegistryConfig,
    project: String,
    rodeo: Arc<ThreadedRodeo>,
) -> Result<SqlFeatureRegistry> {
    let registry_type = SqlRegistryType::from_str(&config.path)?;
    match registry_type {
        SqlRegistryType::Postgres => {
            let pool = new_postgres_connection(&config.path).await?;
            let registry = SqlFeatureRegistry {
                project,
                connection_pool: pool,
                rodeo,
            };
            Ok(registry)
        }
        SqlRegistryType::MySql => Err(anyhow!("MySQL registry not yet implemented")),
    }
}

pub(crate) struct SqlFeatureRegistry {
    project: String,
    connection_pool: Pool<Postgres>,
    rodeo: Arc<ThreadedRodeo>,
}

impl SqlFeatureRegistry {
    /// Queries all registry entities, feature views, on-demand feature views, and feature services
    /// from the database for the current project, and constructs a `FileFeatureRegistry` from the results.
    ///
    /// # Errors
    /// Returns an error if the database connection fails, if any query fails, or if deserialization
    /// of protocol buffer data into model structs fails.
    pub async fn query_registry(&self) -> Result<FileFeatureRegistry> {
        let mut connection = self.connection_pool.acquire().await?;

        async fn query_table<'a, T>(
            rodeo: Arc<ThreadedRodeo>,
            conn: &'a mut sqlx::PgConnection,
            project: &'a str,
            table_name: &'a str,
            name_col: &'a str,
            proto_col: &'a str,
            type_name: &'a str,
        ) -> Result<HashMap<Spur, T>>
        where
            T: TryFrom<(Arc<ThreadedRodeo>, Vec<u8>), Error = anyhow::Error>,
        {
            let query_str = format!(
                "SELECT {}, {} FROM {} WHERE project_id=$1",
                name_col, proto_col, table_name
            );
            let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(&query_str)
                .bind(project)
                .fetch_all(conn)
                .await?;

            rows.into_iter()
                .map(|(name, proto)| {
                    T::try_from((rodeo.clone(), proto))
                        .map_err(|e| {
                            anyhow!(
                                "Failed to convert {} proto for '{}': {}",
                                type_name,
                                name,
                                e
                            )
                        })
                        .map(|item| (rodeo.get_or_intern(name), item))
                })
                .collect::<Result<HashMap<_, _>>>()
        }

        let entities = query_table::<Entity>(
            self.rodeo.clone(),
            &mut connection,
            &self.project,
            "entities",
            "entity_name",
            "entity_proto",
            "Entity",
        )
        .await?;

        let feature_views = query_table::<FeatureView>(
            self.rodeo.clone(),
            &mut connection,
            &self.project,
            "feature_views",
            "feature_view_name",
            "feature_view_proto",
            "FeatureView",
        )
        .await?;

        let on_demand_feature_views = query_table::<crate::model::OnDemandFeatureView>(
            self.rodeo.clone(),
            &mut connection,
            &self.project,
            "on_demand_feature_views",
            "feature_view_name",
            "feature_view_proto",
            "OnDemandFeatureView",
        )
        .await?;

        let feature_services = query_table::<FeatureService>(
            self.rodeo.clone(),
            &mut connection,
            &self.project,
            "feature_services",
            "feature_service_name",
            "feature_service_proto",
            "FeatureService",
        )
        .await?;

        Ok(FileFeatureRegistry::from_registry(
            FeatureRegistry::new(
                entities,
                feature_views,
                on_demand_feature_views,
                feature_services,
            ),
            self.rodeo.clone(),
        ))
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
            path: "postgresql://127.0.0.1:5432/postgres".to_string(),
            cache_ttl_seconds: Some(60),
            ..Default::default()
        };
        let registry = new(config, "careful_tomcat".to_string()).await?;

        let registry_data = registry.query_registry().await?;
        println!("{:#?}", registry_data);
        Ok(())
    }
}
