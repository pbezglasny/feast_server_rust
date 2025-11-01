use crate::config::RegistryConfig;
use crate::model::{Feature, FeatureView, GetOnlineFeaturesRequest, RequestedFeatures};
use crate::registry::{FeatureRegistryService, FileFeatureRegistry};
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use google_cloud_storage::client::{Client as GcsClient, ClientConfig};
use prost::Message;
use rustc_hash::FxHashMap as HashMap;
use std::future::Future;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

pub struct CachedFileRegistry {
    inner: ArcSwap<Box<dyn FeatureRegistryService>>,
    created_at: ArcSwap<DateTime<Utc>>,
    ttl: u64,
}

impl CachedFileRegistry {
    async fn create_cached_registry_and_start_background_thread<F, Fut>(
        feature_registry_fn: F,
        ttl: u64,
    ) -> Result<Arc<dyn FeatureRegistryService>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FileFeatureRegistry>> + Send + 'static,
    {
        let feature_registry = feature_registry_fn().await;
        let result = Arc::new(CachedFileRegistry {
            inner: ArcSwap::from_pointee(Box::new(feature_registry?)),
            created_at: ArcSwap::from_pointee(Utc::now()),
            ttl,
        });
        start_refresh_task(result.clone(), feature_registry_fn, ttl);
        Ok(result)
    }

    async fn create_registry<F, Fut>(
        producer_fn: F,
        ttl: Option<u64>,
    ) -> Result<Arc<dyn FeatureRegistryService>>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<FileFeatureRegistry>> + Send + 'static,
    {
        if let Some(ttl_val) = ttl {
            Self::create_cached_registry_and_start_background_thread(producer_fn, ttl_val).await
        } else {
            let registry = producer_fn().await?;
            Ok(Arc::new(registry))
        }
    }

    pub async fn new_local(
        path: PathBuf,
        cache_ttl_seconds: Option<u64>,
    ) -> Result<Arc<dyn FeatureRegistryService>> {
        let path_arc = Arc::new(path);
        let producer_fn = {
            let path = Arc::clone(&path_arc);
            move || {
                let path = Arc::clone(&path);
                async move { FileFeatureRegistry::from_path(path.as_ref()) }
            }
        };
        Self::create_registry(producer_fn, cache_ttl_seconds).await
    }

    pub async fn new_s3(
        bucket_url: String,
        cache_ttl_seconds: Option<u64>,
    ) -> Result<Arc<dyn FeatureRegistryService>> {
        let (bucket, key) = parse_storage_url(&bucket_url, "s3", "S3")?;
        let bucket = Arc::new(bucket);
        let key = Arc::new(key);

        let config = aws_config::load_from_env().await;
        let client = Arc::new(aws_sdk_s3::Client::new(&config));

        let producer_fn = {
            let client = Arc::clone(&client);
            let bucket = Arc::clone(&bucket);
            let key = Arc::clone(&key);
            move || {
                let client = Arc::clone(&client);
                let bucket = Arc::clone(&bucket);
                let key = Arc::clone(&key);
                async move { from_s3(client, bucket.as_str(), key.as_str()).await }
            }
        };

        Self::create_registry(producer_fn, cache_ttl_seconds).await
    }

    pub async fn new_gcs(
        bucket_url: String,
        cache_ttl_seconds: Option<u64>,
    ) -> Result<Arc<dyn FeatureRegistryService>> {
        let (bucket, object) = parse_storage_url(&bucket_url, "gs", "GCS")?;
        let bucket = Arc::new(bucket);
        let object = Arc::new(object);

        let client_config = ClientConfig::default().with_auth().await?;
        let client = Arc::new(GcsClient::new(client_config));

        let producer_fn = {
            let client = Arc::clone(&client);
            let bucket = Arc::clone(&bucket);
            let object = Arc::clone(&object);
            move || {
                let client = Arc::clone(&client);
                let bucket = Arc::clone(&bucket);
                let object = Arc::clone(&object);
                async move { from_gcs(client, bucket.as_str(), object.as_str()).await }
            }
        };

        Self::create_registry(producer_fn, cache_ttl_seconds).await
    }

    pub async fn new_sql(
        config: RegistryConfig,
        project: String,
    ) -> Result<Arc<dyn FeatureRegistryService>> {
        let ttl = config.cache_ttl_seconds;
        let producer_fn = move || {
            let config = config.clone();
            let project = project.clone();
            async move {
                let sql_registry = crate::registry::sql_registry::new(config, project).await?;
                let registry = sql_registry.query_registry().await?;
                Ok(registry)
            }
        };
        Self::create_registry(producer_fn, ttl).await
    }
}

async fn from_s3(
    s3_client: Arc<aws_sdk_s3::Client>,
    bucket: &str,
    key: &str,
) -> Result<FileFeatureRegistry> {
    let proto_file = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let data = proto_file.body.collect().await?.into_bytes();
    let registry_proto = crate::feast::core::Registry::decode(&*data)?;
    FileFeatureRegistry::from_proto(registry_proto)
}

async fn from_gcs(
    gcs_client: Arc<GcsClient>,
    bucket: &str,
    object: &str,
) -> Result<FileFeatureRegistry> {
    use google_cloud_storage::http::objects::download::Range;
    use google_cloud_storage::http::objects::get::GetObjectRequest;

    let request = GetObjectRequest {
        bucket: bucket.to_string(),
        object: object.to_string(),
        ..Default::default()
    };

    let data = gcs_client
        .download_object(&request, &Range::default())
        .await?;
    let registry_proto = crate::feast::core::Registry::decode(&*data)?;
    FileFeatureRegistry::from_proto(registry_proto)
}

fn parse_storage_url(url_str: &str, scheme: &str, provider_name: &str) -> Result<(String, String)> {
    let url = url::Url::parse(url_str)?;
    if url.scheme() != scheme {
        return Err(anyhow::anyhow!(
            "Invalid {} URL scheme in '{}'",
            provider_name,
            url_str
        ));
    }
    let bucket = url
        .host_str()
        .ok_or(anyhow::anyhow!(
            "Invalid {} URL: could not determine host from '{}'",
            provider_name,
            url_str
        ))?
        .to_string();
    let key = url.path().trim_start_matches('/').to_string();
    Ok((bucket, key))
}

fn start_refresh_task<F, Fut>(
    mut registry: Arc<CachedFileRegistry>,
    feature_registry_fn: F,
    ttl: u64,
) where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<FileFeatureRegistry>> + Send + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(ttl));
        loop {
            interval.tick().await;
            let new_registry = feature_registry_fn().await;
            match new_registry {
                Ok(reg) => {
                    registry.inner.store(Arc::new(Box::new(reg)));
                    registry.created_at.store(Arc::new(Utc::now()));
                }
                Err(msg) => {
                    tracing::error!("Failed to refresh registry: {:?}", msg);
                }
            }
        }
    });
}

#[async_trait]
impl FeatureRegistryService for CachedFileRegistry {
    async fn request_to_view_keys(
        &self,
        request: RequestedFeatures,
    ) -> Result<HashMap<Feature, Arc<FeatureView>>> {
        if self
            .created_at
            .load()
            .add(TimeDelta::seconds(self.ttl as i64))
            .lt(&Utc::now())
        {
            tracing::warn!("Using stale registry");
        }
        let registry = self.inner.load();
        registry.request_to_view_keys(request).await
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{GetOnlineFeaturesRequest, RequestedFeatures};

    #[tokio::test]
    #[ignore]
    async fn read_registry_from_s3() -> anyhow::Result<()> {
        let bucket_url = "s3://feast-rust-feature-registry/registry.db".to_string();
        let s3_registry = super::CachedFileRegistry::new_s3(bucket_url, None).await?;
        let mut request_obj = GetOnlineFeaturesRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()].into();
        let requested_features = RequestedFeatures::from(&request_obj);
        let result = s3_registry.request_to_view_keys(requested_features).await?;
        println!("{:#?}", result);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn read_registry_from_gcs() -> anyhow::Result<()> {
        let bucket_url = "gs://feast-rust-feature-registry/registry.db".to_string();
        let gcs_registry = super::CachedFileRegistry::new_gcs(bucket_url, None).await?;
        let mut request_obj = GetOnlineFeaturesRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()].into();
        let requested_features = RequestedFeatures::from(&request_obj);
        let result = gcs_registry
            .request_to_view_keys(requested_features)
            .await?;
        println!("{:#?}", result);
        Ok(())
    }
}
