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
                let path = path_buf.into_os_string().into_string().unwrap();
                if let Some(ttl) = conf.cache_ttl_seconds {
                    let producer_fn = {
                        move || {
                            let path = path.clone();
                            async move { FileFeatureRegistry::from_path(&path) }
                        }
                    };
                    let cached_registry =
                        CachedFileRegistry::create_cached_registry_and_start_background_thread(
                            producer_fn,
                            ttl,
                        )
                        .await;
                    Ok(cached_registry)
                } else {
                    let registry = FileFeatureRegistry::from_path(path.as_str())?;
                    Ok(Arc::new(registry))
                }
            }
            _ => Err(anyhow!("Unsupported provider for file registry")),
        },
        _ => Err(anyhow::anyhow!("Only file registry is supported now")),
    }
}
