use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum_server::Handle;
use criterion::{Criterion, criterion_group, criterion_main};
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::onlinestore::OnlineStore;
use feast_server_core::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use feast_server_core::registry::FeatureRegistryService;
use feast_server_core::registry::file_registry::FileFeatureRegistry;
use reqwest::Client;
use tokio::runtime::Runtime;

fn workspace_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(relative)
}

fn build_registry_service() -> Arc<dyn FeatureRegistryService> {
    let registry_path = workspace_path("feast-server-core/test_data/registry.pb");
    let registry =
        FileFeatureRegistry::from_path(&registry_path).expect("failed to load registry protobuf");
    Arc::new(registry)
}

async fn build_online_store() -> Arc<dyn OnlineStore> {
    let sqlite_path = workspace_path("feast-server-core/test_data/online_store.db");
    let sqlite = SqliteOnlineStore::from_options(
        sqlite_path
            .to_str()
            .expect("online store path is not valid UTF-8"),
        "golden_hornet".to_string(),
        ConnectionOptions::default(),
    )
    .await
    .expect("failed to open sqlite online store");
    Arc::new(sqlite)
}

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind local port")
        .local_addr()
        .expect("failed to read local addr")
        .port()
}

async fn wait_for_server(client: &Client, url: &str) {
    const MAX_ATTEMPTS: usize = 50;
    for _ in 0..MAX_ATTEMPTS {
        if let Ok(response) = client.get(url).send().await {
            if response.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become ready on {url}");
}

fn start_rest_server(
    runtime: &Runtime,
    registry: Arc<dyn FeatureRegistryService>,
    online_store: Arc<dyn OnlineStore>,
    port: u16,
) -> (Handle, tokio::task::JoinHandle<Result<()>>) {
    let handle = Handle::new();
    let server_handle = handle.clone();
    let feature_store = FeatureStore::new(registry, online_store);
    let server_config = rest_server::server::ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let join = runtime.spawn(async move {
        rest_server::server::start_server(server_config, feature_store, false, server_handle).await
    });
    (handle, join)
}

fn bench_rest_server(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let registry = build_registry_service();
    let online_store = runtime.block_on(build_online_store());
    let port = find_free_port();
    let (shutdown_handle, server_task) =
        start_rest_server(&runtime, registry.clone(), online_store.clone(), port);

    let client = Client::new();
    let health_url = format!("http://127.0.0.1:{port}/health");
    runtime.block_on(wait_for_server(&client, &health_url));

    let request_path = workspace_path("body.json");
    let request_body =
        std::fs::read_to_string(&request_path).expect("failed to read request payload");
    let request_url = format!("http://127.0.0.1:{port}/get-online-features");

    c.bench_function("rest_server_get_online_features", |b| {
        b.to_async(&runtime).iter(|| {
            let client = client.clone();
            let request_url = request_url.clone();
            let request_body = request_body.clone();
            async move {
                let response = client
                    .post(&request_url)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(request_body)
                    .send()
                    .await
                    .expect("request failed")
                    .error_for_status()
                    .expect("non-success status");
                let bytes = response
                    .bytes()
                    .await
                    .expect("failed to read response body");
                criterion::black_box(bytes);
            }
        });
    });

    shutdown_handle.shutdown();
    runtime.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
    runtime
        .block_on(async { server_task.await })
        .expect("server task panicked")
        .expect("server returned error");
}

criterion_group!(rest_server_bench, bench_rest_server);
criterion_main!(rest_server_bench);
