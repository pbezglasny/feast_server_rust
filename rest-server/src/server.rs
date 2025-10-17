use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_prometheus::PrometheusMetricLayer;
use axum_server::tls_rustls::RustlsConfig;
use feast_server_core::error::FeastCoreError;
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::model::GetOnlineFeaturesRequest;
use serde::Serialize;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;

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

#[derive(Serialize)]
struct ErrorResponse {
    message: String,
}

pub struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        Self::new(rejection.status(), rejection.body_text())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status;
        let body = Json(ErrorResponse {
            message: self.message,
        });
        (status, body).into_response()
    }
}

pub async fn start_server(
    server_config: ServerConfig,
    feature_store: FeatureStore,
    metrics_enabled: bool,
    shutdown_handler: axum_server::Handle,
) -> Result<()> {
    let server = FeastServer {
        feature_store: Arc::new(feature_store),
    };

    let mut app = Router::new()
        .route("/get-online-features", post(handle_feature_request))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(server);
    let trace = tower_http::trace::TraceLayer::new_for_http();
    app = app.layer(trace);
    if metrics_enabled {
        let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();
        app = app
            .route("/metrics", get(|| async move { metric_handle.render() }))
            .layer(prometheus_layer)
    }

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
            .handle(shutdown_handler)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    } else {
        axum_server::bind(addr)
            .handle(shutdown_handler)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    }
}

async fn handle_feature_request(
    State(server): State<FeastServer>,
    payload: Result<Json<GetOnlineFeaturesRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let Json(get_online_feature_request) = payload?;

    server
        .feature_store
        .get_online_features(get_online_feature_request)
        .await
        .map(Json)
        .map_err(|err| {
            if let Some(feast_error) = err.downcast_ref::<FeastCoreError>()
                && feast_error.is_not_found()
            {
                return AppError::new(StatusCode::NOT_FOUND, feast_error.to_string());
            }
            AppError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
        })
}
