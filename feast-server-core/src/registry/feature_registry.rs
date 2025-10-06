use crate::config::{Provider, RegistryConfig, RegistryType};
use crate::registry::cached_registry::CachedFileRegistry;
use crate::registry::{FeatureRegistryProto, FeatureRegistryService};
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
                if let Some(ttl) = conf.cache_ttl_seconds {
                    let producer_fn = {
                        let mut path_buf = PathBuf::new();
                        path_buf.push(&path_prefix);
                        path_buf.push(conf.path.clone());
                        move || {
                            let path = path_buf.clone().into_os_string().into_string().unwrap();
                            async move { FeatureRegistryProto::from_path(&path) }
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
                    let path = format!("{}/{}", path_prefix, conf.path);
                    let registry = FeatureRegistryProto::from_path(&path)?;
                    Ok(Arc::new(registry))
                }
            }
            _ => Err(anyhow!("")),
        },
        _ => Err(anyhow::anyhow!("Only file registry is supported now")),
    }
}
