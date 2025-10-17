use crate::config::{Provider, RegistryConfig, RegistryType};
use crate::registry::cached_registry::CachedFileRegistry;
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::Arc;

fn get_provider(provider_opt: Option<Provider>, path: &str) -> Provider {
    if let Some(provider) = provider_opt {
        provider
    } else if path.starts_with("s3://") {
        Provider::AWS
    } else if path.starts_with("gs://") {
        Provider::GCP
    } else {
        Provider::Local
    }
}

pub async fn get_registry(
    conf: &RegistryConfig,
    provider: Option<Provider>,
    cwd: Option<&str>,
) -> Result<Arc<dyn FeatureRegistryService>> {
    let path_prefix = cwd.unwrap_or("");
    match &conf.registry_type {
        RegistryType::File => match get_provider(provider, conf.path.as_str()) {
            Provider::Local => {
                let mut path_buf = PathBuf::new();
                path_buf.push(path_prefix);
                path_buf.push(conf.path.as_str());
                let registry =
                    CachedFileRegistry::new_local(path_buf, conf.cache_ttl_seconds.clone()).await?;
                Ok(registry)
            }
            Provider::AWS => {
                let registry =
                    CachedFileRegistry::new_s3(conf.path.clone(), conf.cache_ttl_seconds.clone())
                        .await?;
                Ok(registry)
            }
            Provider::GCP => {
                let registry =
                    CachedFileRegistry::new_gcs(conf.path.clone(), conf.cache_ttl_seconds.clone())
                        .await?;
                Ok(registry)
            }
            _ => Err(anyhow!("Unsupported provider for file registry")),
        },
        _ => Err(anyhow::anyhow!("Only file registry is supported now")),
    }
}
