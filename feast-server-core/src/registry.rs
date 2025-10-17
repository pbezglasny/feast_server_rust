//! Registry module for managing feature views and features metadata.

use crate::model::{Feature, FeatureView, GetOnlineFeaturesRequest};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

mod cached_registry;
mod feature_registry;
pub mod file_registry;

pub use feature_registry::get_registry;
pub use file_registry::FileFeatureRegistry;

#[async_trait]
pub trait FeatureRegistryService: Send + Sync {
    /// Get Feature View objects for the requested features in the request
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeaturesRequest,
    ) -> Result<HashMap<Feature, FeatureView>>;
}
