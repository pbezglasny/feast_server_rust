use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use crate::registry::FeatureRegistryService;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CachedRegistry {
    inner: Arc<ArcSwap<Arc<dyn FeatureRegistryService>>>,
    ttl: u32,
}

impl CachedRegistry {
    pub fn new(feature_registry: Arc<dyn FeatureRegistryService>, ttl: u32) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(feature_registry.into())),
            ttl,
        }
    }

    fn start_refresh_task(&self, feature_registry: fn() -> Arc<dyn FeatureRegistryService>) {
        let ttl = self.ttl;
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
