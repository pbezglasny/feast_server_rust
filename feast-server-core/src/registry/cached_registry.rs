use crate::model::{Feature, FeatureView, GetOnlineFeatureRequest};
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;

pub struct CachedFileRegistry {
    inner: ArcSwap<Box<dyn FeatureRegistryService>>,
    created_at: ArcSwap<DateTime<Utc>>,
    ttl: u64,
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
            inner: ArcSwap::from_pointee(Box::new(feature_registry.unwrap())),
            created_at: ArcSwap::from_pointee(Utc::now()),
            ttl,
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
            match new_registry {
                Ok(reg) => {
                    registry.inner.store(Arc::new(Box::new(reg)));
                    registry.created_at.store(Arc::new(Utc::now()));
                }
                Err(msg) => {
                    tracing::error!("Failed to refresh registry: {:?}", msg);
                }
            }
        }
    });
}

#[async_trait]
impl FeatureRegistryService for CachedFileRegistry {
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<IndexMap<Feature, FeatureView>> {
        if self
            .created_at
            .load()
            .add(TimeDelta::seconds(self.ttl as i64))
            .lt(&Utc::now())
        {
            tracing::warn!("Using stale registry");
        }
        let registry = self.inner.load();
        registry.request_to_view_keys(request).await
    }
}
