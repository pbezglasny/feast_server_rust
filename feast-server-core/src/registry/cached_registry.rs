use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use crate::registry::FeatureRegistryService;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CachedRegistry {
    inner: Arc<ArcSwap<Arc<dyn FeatureRegistryService>>>,
}

impl CachedRegistry {
    pub fn create_cached_registry_and_start_background_thread(
        feature_registry_fn: fn() -> Arc<dyn FeatureRegistryService>,
        ttl: u32,
    ) -> Self {
        let feature_registry = feature_registry_fn();
        let result = Self {
            inner: Arc::new(ArcSwap::new(feature_registry.into())),
        };
        result.start_refresh_task(feature_registry_fn, ttl);
        result
    }

    fn start_refresh_task(
        &self,
        feature_registry: fn() -> Arc<dyn FeatureRegistryService>,
        ttl: u32,
    ) {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(ttl as u64));
            loop {
                interval.tick().await;
                let new_registry = feature_registry();
                inner.store(new_registry.into());
            }
        });
    }
}

#[async_trait]
impl FeatureRegistryService for CachedRegistry {
    async fn request_to_view_keys(
        &self,
        request: Arc<GetOnlineFeatureRequest>,
    ) -> Result<HashMap<RequestedFeature, FeatureView>> {
        let registry = self.inner.load();
        registry.request_to_view_keys(request).await
    }
}
