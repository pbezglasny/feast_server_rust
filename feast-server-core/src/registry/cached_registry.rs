use crate::model::{Feature, FeatureView, GetOnlineFeatureRequest};
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CachedFileRegistry {
    inner: ArcSwap<Box<dyn FeatureRegistryService>>,
}

impl CachedFileRegistry {
    pub async fn create_cached_registry_and_start_background_thread<F, Fut>(
        feature_registry_fn: F,
        ttl: u64,
    ) -> Arc<CachedFileRegistry>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FileFeatureRegistry>> + Send + 'static,
    {
        let feature_registry = feature_registry_fn().await;
        let result = Arc::new(CachedFileRegistry {
            inner: ArcSwap::new(Arc::new(Box::new(feature_registry.unwrap()))),
        });
        start_refresh_task(result.clone(), feature_registry_fn, ttl);
        result
    }
}

fn start_refresh_task<F, Fut>(
    mut registry: Arc<CachedFileRegistry>,
    feature_registry_fn: F,
    ttl: u64,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<FileFeatureRegistry>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(ttl));
        loop {
            interval.tick().await;
            let new_registry = feature_registry_fn().await;
            // TODO: handle error
            registry
                .inner
                .store(Arc::new(Box::new(new_registry.unwrap())));
        }
    });
}

#[async_trait]
impl FeatureRegistryService for CachedFileRegistry {
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<HashMap<Feature, FeatureView>> {
        let registry = self.inner.load();
        registry.request_to_view_keys(request).await
    }
}
