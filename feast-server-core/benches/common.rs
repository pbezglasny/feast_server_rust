#![allow(dead_code)]

use rustc_hash::FxHashMap as HashMap;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::model::{EntityIdValue, GetOnlineFeaturesRequest};
use feast_server_core::onlinestore::OnlineStore;
use feast_server_core::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
use feast_server_core::registry::FeatureRegistryService;
use feast_server_core::registry::file_registry::FileFeatureRegistry;
use tokio::sync::OnceCell;

fn manifest_path(relative: &str) -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), relative)
}

fn load_registry_proto() -> Result<FileFeatureRegistry> {
    let registry_path = std::path::PathBuf::from(manifest_path("test_data/registry.pb"));
    FileFeatureRegistry::from_path(&registry_path)
}

static REGISTRY_SERVICE: OnceLock<Arc<dyn FeatureRegistryService>> = OnceLock::new();

pub fn registry_service() -> Arc<dyn FeatureRegistryService> {
    REGISTRY_SERVICE
        .get_or_init(|| {
            let registry = load_registry_proto().expect("failed to load registry");
            Arc::new(registry) as Arc<dyn FeatureRegistryService>
        })
        .clone()
}

static SQLITE_STORE: OnceCell<Arc<dyn OnlineStore>> = OnceCell::const_new();

pub async fn online_store() -> Result<Arc<dyn OnlineStore>> {
    SQLITE_STORE
        .get_or_try_init(|| async {
            let sqlite_path = manifest_path("test_data/online_store.db");
            SqliteOnlineStore::from_options(
                &sqlite_path,
                "golden_hornet".to_string(),
                ConnectionOptions::default(),
            )
            .await
            .map(|store| Arc::new(store) as Arc<dyn OnlineStore>)
        })
        .await
        .map(Clone::clone)
}

static FEATURE_STORE: OnceCell<Arc<FeatureStore>> = OnceCell::const_new();

pub async fn feature_store() -> Result<Arc<FeatureStore>> {
    FEATURE_STORE
        .get_or_try_init(|| async {
            let registry = registry_service();
            let online = online_store().await?;
            Ok(Arc::new(FeatureStore::new(registry, online)))
        })
        .await
        .map(Clone::clone)
}

pub fn sample_request() -> GetOnlineFeaturesRequest {
    let entities = HashMap::from_iter([(
        Arc::<str>::from("driver_id"),
        vec![
            EntityIdValue::Int(1005),
            EntityIdValue::Int(1002),
            EntityIdValue::Int(2003),
        ],
    )]);
    GetOnlineFeaturesRequest {
        entities,
        feature_service: None,
        features: vec![
            "driver_hourly_stats_fresh:conv_rate".to_string(),
            "driver_hourly_stats:acc_rate".to_string(),
        ]
        .into(),
        full_feature_names: Some(false),
    }
}
