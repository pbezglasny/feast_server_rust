use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub mod file_registry;
mod cached_registry;
mod s3_registry;
mod local_registry;
mod feature_registry;

pub use file_registry::FeatureRegistryProto;

#[async_trait]
pub trait FeatureRegistryService: Send + Sync {
    async fn request_to_view_keys(
        &self,
        request: Arc<GetOnlineFeatureRequest>,
    ) -> Result<HashMap<RequestedFeature, FeatureView>>;
}
