use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use crate::registry::{FeatureRegistryProto, FeatureRegistryService};
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CachedFileRegistry {
    inner: Arc<ArcSwap<Arc<FeatureRegistryProto>>>,
}

impl CachedFileRegistry {
    pub async fn create_cached_registry_and_start_background_thread<F, Fut>(
        feature_registry_fn: F,
        ttl: u32,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FeatureRegistryProto>> + Send + 'static,
    {
        let feature_registry = feature_registry_fn().await;
        let result = Self {
            inner: Arc::new(ArcSwap::new(Arc::new(feature_registry.unwrap().into()))),
        };
        result.start_refresh_task(feature_registry_fn, ttl);
        result
    }

    fn start_refresh_task<F, Fut>(&self, feature_registry_fn: F, ttl: u32)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FeatureRegistryProto>> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(ttl as u64));
            loop {
                interval.tick().await;
                let new_registry = feature_registry_fn().await;
                // TODO: handle error
                inner.store(Arc::new(new_registry.unwrap().into()));
            }
        });
    }
}

#[async_trait]
impl FeatureRegistryService for CachedFileRegistry {
    async fn request_to_view_keys(
        &self,
        request: Arc<GetOnlineFeatureRequest>,
    ) -> Result<HashMap<RequestedFeature, FeatureView>> {
        let registry = self.inner.load();
        registry.request_to_view_keys(request).await
    }
}
