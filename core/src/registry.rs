use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

pub mod feature_registry;

#[async_trait]
pub trait FeatureRegistryService: Send + Sync {
    async fn request_to_view_keys(
        &self,
        request: Arc<GetOnlineFeatureRequest>,
    ) -> HashMap<RequestedFeature, FeatureView>;
}
