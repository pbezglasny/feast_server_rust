use crate::model::{Feature, FeatureView, GetOnlineFeatureRequest};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

mod cached_registry;
mod feature_registry;
pub mod file_registry;
mod s3_registry;

pub use feature_registry::get_registry;
pub use file_registry::FileFeatureRegistry;

#[derive(Debug, Clone)]
pub struct RegistryLookupResult {
    pub feature_to_view: HashMap<Feature, FeatureView>,
    pub ordered_features: Vec<Feature>,
}

#[async_trait]
pub trait FeatureRegistryService: Send + Sync {
    /// Get Feature View objects for the requested features in the request
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<RegistryLookupResult>;
}
