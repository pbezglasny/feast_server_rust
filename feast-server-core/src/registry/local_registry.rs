use crate::registry::FeatureRegistryService;
use crate::registry::feature_registry::FeatureRegistry;
use std::sync::Arc;

pub struct LocalRegistry {
    registry: Arc<FeatureRegistry>,
}

impl LocalRegistry {
    pub fn new(feature_registry: FeatureRegistry) -> Self {
        Self {
            registry: Arc::new(feature_registry),
        }
    }
}


