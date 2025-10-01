use crate::registry::FeatureRegistryProto;
use crate::registry::cached_registry::CachedFileRegistry;

pub enum FeatureRegistry {
    NonCached(FeatureRegistryProto),
    CachedRegistry(CachedFileRegistry),
}
