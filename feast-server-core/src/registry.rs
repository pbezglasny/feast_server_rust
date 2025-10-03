use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

mod cached_registry;
mod feature_registry;
pub mod file_registry;
mod s3_registry;

pub use file_registry::FeatureRegistryProto;
pub use feature_registry::get_registry;

#[async_trait]
pub trait FeatureRegistryService: Send + Sync {
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<HashMap<RequestedFeature, FeatureView>>;
}
