use anyhow::Result;
use anyhow::anyhow;
use axum::routing::get;
use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use axum_server::tls_rustls::RustlsConfig;
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::model::GetOnlineFeatureRequest;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct FeastServer {
    feature_store: Arc<FeatureStore>,
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6566,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

pub async fn start_server(
    server_config: ServerConfig,
    feature_store: FeatureStore,
    _enable_metrics: bool,
) -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let server = FeastServer {
        feature_store: Arc::new(feature_store),
    };

    let app = Router::new()
        .route("/get-online-features", post(handle_feature_reqeust))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(server);

    let addr: SocketAddr = format!("{}:{}", server_config.host, server_config.port)
        .to_socket_addrs()?
        .next()
        .ok_or(anyhow!("Cannot resolve host"))?;

    tracing::info!(
        "Server listening on {}:{}",
        server_config.host,
        server_config.port
    );
    if server_config.tls_enabled {
        let cert_path = server_config
            .tls_cert_path
            .ok_or(anyhow!("TLS is enabled but cert path is not provided"))?;
        let key_path = server_config
            .tls_key_path
            .ok_or(anyhow!("TLS is enabled but key path is not provided"))?;
        let rustls_config = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| anyhow!("Failed to load TLS config: {}", e))?;
        axum_server::bind_rustls(addr, rustls_config)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    } else {
        axum_server::bind(addr)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    }
}

async fn handle_feature_reqeust(
    State(server): State<FeastServer>,
    Json(get_online_feature_request): Json<GetOnlineFeatureRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    server
        .feature_store
        .get_online_features(get_online_feature_request)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        .map(Json)
}

#[cfg(test)]
mod tests {
    use super::{ServerConfig, start_server};
    use anyhow::Result;
    use feast_server_core::feature_store::FeatureStore;
    use feast_server_core::onlinestore::sqlite_onlinestore::{
        ConnectionOptions, SqliteOnlineStore,
    };
    use feast_server_core::registry::FeatureRegistryProto;
    use std::sync::Arc;

    #[tokio::test]
    async fn start_server_test() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/../feast-server-core/test_data/registry.pb", project_dir);
        let feature_registry = FeatureRegistryProto::from_path(&registry_file)?;
        let sqlite_path = format!(
            "{}/../feast-server-core/test_data/online_store.db",
            project_dir
        );
        let sqlite_store = SqliteOnlineStore::from_options(
            &sqlite_path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        let store = FeatureStore::new(Arc::new(feature_registry), Arc::new(sqlite_store));
        start_server(ServerConfig::default(), store, false).await?;

        Ok(())
    }
}
