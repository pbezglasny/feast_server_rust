use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::model::GetOnlineFeatureRequest;
use std::sync::Arc;

struct ServerError {
    message: String,
}

#[derive(Clone)]
pub struct FeastServer {
    feature_store: Arc<FeatureStore>,
}

pub struct ServerConfig {}
pub async fn start_server(server_config: ServerConfig, feature_store: FeatureStore) {
    let server = FeastServer {
        feature_store: Arc::new(feature_store),
    };

    let app = Router::new()
        .route("/get-online-features", post(handle_feature_reqeust))
        .with_state(server);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
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
        .map(|response| Json(response))
}

#[cfg(test)]
mod tests {
    use super::{start_server, ServerConfig};
    use anyhow::Result;
    use feast_server_core::feature_store::FeatureStore;
    use feast_server_core::onlinestore::sqlite_onlinestore::{
        ConnectionOptions, SqliteOnlineStore,
    };
    use feast_server_core::registry::FeatureRegistryProto;
    use std::sync::Arc;

    #[tokio::test]
    async fn start_server_test() -> Result<()> {
        let registry_file =
            "/Users/pavel/work/rust/feast_rust/feast-server-core/test_data/registry.pb";
        let feature_registry = FeatureRegistryProto::from_path(registry_file)?;
        let sqlite_path =
            "/Users/pavel/work/rust/feast_rust/dev/golden_hornet/feature_repo/data/online_store.db";
        let sqlite_store = SqliteOnlineStore::from_options(
            sqlite_path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        let store = FeatureStore::new(Arc::new(feature_registry), Arc::new(sqlite_store));
        start_server(ServerConfig {}, store).await;

        Ok(())
    }
}
