use crate::model::{FeatureView, GetOnlineFeatureRequest, RequestedFeature};
use crate::registry::cached_registry::CachedFileRegistry;
use crate::registry::{FeatureRegistryProto, FeatureRegistryService};
use anyhow::Result;
use async_trait::async_trait;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

pub struct S3Registry {
    registry: FeatureRegistryProto,
}

fn parse_s3_url(s3_url: &str) -> Result<(String, String)> {
    let url = url::Url::parse(s3_url)?;
    if url.scheme() != "s3" {
        return Err(anyhow::anyhow!("Invalid S3 URL scheme"));
    }
    let bucket = url
        .host_str()
        .ok_or(anyhow::anyhow!("Invalid S3 URL"))?
        .to_string();
    let key = url.path().trim_start_matches('/').to_string();
    Ok((bucket, key))
}

impl S3Registry {
    pub async fn new_non_cached(bucket_url: String) -> Result<Self> {
        let (bucket, key) = parse_s3_url(&bucket_url)?;
        let config = aws_config::load_from_env().await;
        let client = Arc::new(aws_sdk_s3::Client::new(&config));
        let registry = S3Registry::from_s3(client, &bucket, &key).await?;
        Ok(Self { registry })
    }

    /*    pub async fn new_cached(bucket_url: String, ttl: u64) -> Result<Self> {
        let (bucket, key) = parse_s3_url(&bucket_url)?;
        let config = aws_config::load_from_env().await;
        let client = Arc::new(aws_sdk_s3::Client::new(&config));
        let registry = S3Registry::from_s3(client.clone(), &bucket, &key).await?;
        let producer_fn = {
            let client = client.clone();
            move || {
                let bucket = bucket.clone();
                let key = key.clone();
                let client = client.clone();
                async move { S3Registry::from_s3((client).clone(), &bucket, &key).await }
            }
        };
        let cached_registry =
            CachedFileRegistry::create_cached_registry_and_start_background_thread(
                producer_fn,
                ttl,
            )
            .await;
        Ok(Self {
            registry: Arc::new(FeatureRegistry::CachedRegistry(cached_registry)),
        })
    }*/

    async fn from_s3(
        s3_client: Arc<aws_sdk_s3::Client>,
        bucket: &str,
        key: &str,
    ) -> Result<FeatureRegistryProto> {
        let proto_file = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        let data = proto_file.body.collect().await?.into_bytes();
        let registry_proto = crate::feast::core::Registry::decode(&*data)?;
        FeatureRegistryProto::from_proto(registry_proto)
    }
}

#[async_trait]
impl FeatureRegistryService for S3Registry {
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<HashMap<RequestedFeature, FeatureView>> {
        self.registry.request_to_view_keys(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::FeatureRegistryService;
    use crate::model::GetOnlineFeatureRequest;
    use anyhow::Result;
    use std::sync::Arc;
    #[tokio::test]
    async fn read_registry_from_s3() -> Result<()> {
        let buket_url = "s3://feast-rust-feature-registry/registry.db".to_string();
        let s3_registry = super::S3Registry::new_non_cached(buket_url).await?;
        let mut request_obj = GetOnlineFeatureRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()];
        let result = s3_registry.request_to_view_keys(&request_obj).await?;
        println!("{:#?}", result);
        Ok(())
    }
}
