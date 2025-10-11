use crate::feast::core::Registry;
use crate::model::{
    Feature, FeatureRegistry, FeatureService, FeatureView, GetOnlineFeatureRequest,
    RequestedFeatures,
};
use crate::registry::FeatureRegistryService;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use indexmap::IndexMap;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

pub struct FileFeatureRegistry {
    registry: FeatureRegistry,
}

impl FileFeatureRegistry {
    pub fn from_proto(proto_registry: Registry) -> Result<Self> {
        let registry = FeatureRegistry::try_from(proto_registry)?;
        Ok(Self { registry })
    }

    pub fn from_path(registry_file_path: &str) -> Result<Self> {
        let path = Path::new(registry_file_path);
        let mut file = fs::File::open(path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                anyhow!(
                    "Registry file not found at '{}'. Check your repository configuration (e.g. FEATURE_REPO_DIR or --chdir).",
                    path.display()
                )
            } else {
                anyhow::Error::new(err).context(format!("Failed to open registry file at '{}'", path.display()))
            }
        })?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .with_context(|| format!("Failed to read registry file at '{}'", path.display()))?;
        let registry_proto = Registry::decode(&*buf).with_context(|| {
            format!("Failed to parse registry protobuf at '{}'", path.display())
        })?;
        let registry = FeatureRegistry::try_from(registry_proto)?;
        Ok(Self { registry })
    }

    fn feature_views_from_service(
        &self,
        service_name: &str,
    ) -> Result<IndexMap<Feature, FeatureView>> {
        let service = self
            .registry
            .feature_services
            .get(service_name)
            // TODO use custom error type for 404 error
            .ok_or(anyhow!("Requested feature service not found"))?
            .clone();
        let mut result = IndexMap::new();
        let FeatureService {
            name,
            project,
            created_timestamp,
            last_updated_timestamp,
            projections,
            logging_config,
        } = service;
        for projection in projections {
            if self
                .registry
                .on_demand_features
                .contains_key(&projection.feature_view_name)
            {
                return Err(anyhow!("OnDemand feature view for now is not supported"));
            }
            let mut feature_view = self
                .registry
                .feature_views
                .get(projection.feature_view_name.as_str())
                .ok_or(anyhow!(
                    "Feature view {} not found for service {}",
                    projection.feature_view_name,
                    service_name
                ))?
                .clone();
            feature_view.join_key_map = Some(projection.join_key_map);
            for feature_name in projection.features {
                let req_feature = Feature {
                    feature_view_name: projection.feature_view_name.clone(),
                    feature_name: feature_name.name.clone(),
                };
                result.insert(req_feature, feature_view.clone());
            }
        }
        Ok(result)
    }

    fn feature_views_from_names(&self, names: &[Feature]) -> Result<IndexMap<Feature, FeatureView>> {
        names
            .iter()
            .map(|req_feature| {
                if self
                    .registry
                    .on_demand_features
                    .contains_key(&req_feature.feature_view_name)
                {
                    return Err(anyhow!("OnDemand feature view for now is not supported"));
                }
                self.registry
                    .feature_views
                    .get(req_feature.feature_view_name.as_str())
                    .map(|view| (req_feature.clone(), view.clone()))
                    .ok_or(anyhow!(
                        "Feature view {} not found",
                        req_feature.feature_view_name
                    ))
            })
            .collect()
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    fn get_feature_views(
        &self,
        requested_features: RequestedFeatures,
    ) -> Result<IndexMap<Feature, FeatureView>> {
        match requested_features {
            RequestedFeatures::FeatureService(service_name) => {
                self.feature_views_from_service(&service_name)
            }
            RequestedFeatures::FeatureNames(names) => {
                let mut bad_requests = vec![];
                let parsed_requested_features: Vec<Feature> = names
                    .into_iter()
                    .map(|f| Feature::try_from(f.as_str()))
                    .filter_map(|r| r.map_err(|e| bad_requests.push(e)).ok())
                    .collect();
                if !bad_requests.is_empty() {
                    let messages = bad_requests
                        .into_iter()
                        .map(|e| format!("{}", e))
                        .collect::<Vec<String>>()
                        .join("\n");
                    return Err(anyhow!(
                        "Error while requested next features: [{}]",
                        messages
                    ));
                }
                self.feature_views_from_names(&parsed_requested_features)
            }
        }
    }
}

#[async_trait]
impl FeatureRegistryService for FileFeatureRegistry {
    async fn request_to_view_keys(
        &self,
        request: &GetOnlineFeatureRequest,
    ) -> Result<IndexMap<Feature, FeatureView>> {
        let requested_features = RequestedFeatures::from(request);
        self.get_feature_views(requested_features)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{Feature, GetOnlineFeatureRequest};
    use crate::registry::FeatureRegistryService;
    use crate::registry::file_registry::FileFeatureRegistry;
    use anyhow::Result;
    use std::sync::Arc;

    #[test]
    fn create_feature_registry() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry = FileFeatureRegistry::from_path(&registry_file)?;
        let requested_features = vec![Feature {
            feature_view_name: "driver_hourly_stats_fresh".to_string(),
            feature_name: "conv_rate".to_string(),
        }];
        let found_views = feature_registry.feature_views_from_names(&requested_features)?;
        assert_eq!(found_views.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_features_by_name() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);

        let feature_registry_proto = FileFeatureRegistry::from_path(&registry_file)?;
        let feature_registry_service: Box<dyn FeatureRegistryService> =
            Box::new(feature_registry_proto);
        let mut request_obj = GetOnlineFeatureRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()].into();
        let result = feature_registry_service
            .request_to_view_keys(&request_obj)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
    #[tokio::test]
    async fn get_features_by_service() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry_proto = FileFeatureRegistry::from_path(&registry_file)?;
        let feature_registry_service: Box<dyn FeatureRegistryService> =
            Box::new(feature_registry_proto);
        let mut request_obj = GetOnlineFeatureRequest::default();
        request_obj.feature_service = Some("driver_activity_v4".to_string());
        let result = feature_registry_service
            .request_to_view_keys(&request_obj)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
}
