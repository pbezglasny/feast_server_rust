use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use criterion::{Criterion, criterion_group, criterion_main};
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::onlinestore::OnlineStore;
use feast_server_core::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use feast_server_core::registry::FeatureRegistryService;
use feast_server_core::registry::file_registry::FileFeatureRegistry;
use grpc_server::server::{ServerConfig, start_server as grpc_start_server};
use tokio::runtime::Runtime;
use tonic::transport::Channel;

mod proto {
    pub mod feast {
        pub mod serving {
            tonic::include_proto!("feast.serving");
        }
        pub mod types {
            tonic::include_proto!("feast.types");
        }
    }
}

use proto::feast::serving::get_online_features_request::Kind;
use proto::feast::serving::serving_service_client::ServingServiceClient;
use proto::feast::serving::{FeatureList, GetOnlineFeaturesRequest};
use proto::feast::types::value::Val;
use proto::feast::types::{RepeatedValue, Value};

fn workspace_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli crate has no parent directory")
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

async fn connect_with_retry(endpoint: &str) -> ServingServiceClient<Channel> {
    const MAX_ATTEMPTS: usize = 50;
    let mut last_error = None;
    for _ in 0..MAX_ATTEMPTS {
        match ServingServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return client,
            Err(err) => {
                last_error = Some(err);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "gRPC server did not become ready at {endpoint}: {:?}",
        last_error
    );
}

fn build_request() -> GetOnlineFeaturesRequest {
    let features = vec![
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "driver_hourly_stats_fresh:avg_daily_trips",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    let entity_values = vec![1005_i64, 1002, 1003]
        .into_iter()
        .map(|value| Value {
            val: Some(Val::Int64Val(value)),
        })
        .collect();

    let mut entities = HashMap::new();
    entities.insert(
        "driver_id".to_string(),
        RepeatedValue { val: entity_values },
    );

    GetOnlineFeaturesRequest {
        kind: Some(Kind::Features(FeatureList { val: features })),
        entities,
        full_feature_names: true,
        request_context: HashMap::new(),
    }
}

fn start_grpc_server(
    runtime: &Runtime,
    registry: Arc<dyn FeatureRegistryService>,
    online_store: Arc<dyn OnlineStore>,
    port: u16,
) -> tokio::task::JoinHandle<Result<()>> {
    let feature_store = FeatureStore::new(registry, online_store);
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    runtime.spawn(async move { grpc_start_server(config, feature_store).await })
}

fn bench_grpc_server(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let registry = build_registry_service();
    let online_store = runtime.block_on(build_online_store());
    let port = find_free_port();
    let server_task = start_grpc_server(&runtime, registry.clone(), online_store.clone(), port);

    let endpoint = format!("http://127.0.0.1:{port}");
    let client = runtime.block_on(async { connect_with_retry(&endpoint).await });
    let request = build_request();

    c.bench_function("grpc_server_get_online_features", |b| {
        b.to_async(&runtime).iter(|| {
            let mut client = client.clone();
            let request = request.clone();
            async move {
                let response = client
                    .get_online_features(request)
                    .await
                    .expect("gRPC request failed");
                criterion::black_box(response.into_inner());
            }
        });
    });

    server_task.abort();
    runtime.block_on(async { server_task.await }).expect("server shutdown failed");
}

criterion_group!(grpc_server_bench, bench_grpc_server);
criterion_main!(grpc_server_bench);
