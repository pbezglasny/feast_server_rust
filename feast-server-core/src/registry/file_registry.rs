use crate::error::FeastCoreError;
use crate::feast::core::Registry;
use crate::model::{
    Feature, FeatureRegistry, FeatureService, FeatureView, GetOnlineFeaturesRequest,
    RequestedFeatures,
};
use crate::registry::FeatureRegistryService;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
pub struct FileFeatureRegistry {
    registry: FeatureRegistry,
}

impl FileFeatureRegistry {
    pub fn from_registry(registry: FeatureRegistry) -> Self {
        Self { registry }
    }
    pub fn from_proto(proto_registry: Registry) -> Result<Self> {
        let registry = FeatureRegistry::try_from(proto_registry)?;
        Ok(Self { registry })
    }

    pub fn from_path(registry_file_path: &PathBuf) -> Result<Self> {
        let mut file = fs::File::open(registry_file_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                anyhow!(
                    "Registry file not found at '{}'. Check your repository configuration (e.g. FEATURE_REPO_DIR or --chdir).",
                    registry_file_path.display()
                )
            } else {
                anyhow::Error::new(err).context(format!("Failed to open registry file at '{}'", registry_file_path.display()))
            }
        })?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).with_context(|| {
            format!(
                "Failed to read registry file at '{}'",
                registry_file_path.display()
            )
        })?;
        let registry_proto = Registry::decode(&*buf).with_context(|| {
            format!(
                "Failed to parse registry protobuf at '{}'",
                registry_file_path.display()
            )
        })?;
        let registry = FeatureRegistry::try_from(registry_proto)?;
        Ok(Self { registry })
    }

    fn feature_views_from_service(
        &self,
        service_name: &str,
    ) -> Result<HashMap<Feature, Arc<FeatureView>>> {
        let service = self
            .registry
            .feature_services
            .get(service_name)
            .ok_or_else(|| FeastCoreError::feature_service_not_found(service_name))?;
        if service.resolved_projections.len() != service.projections.len() {
            let feature_names = service.missing_feature_views.join(", ");
            return Err(FeastCoreError::feature_view_not_found_for_service(
                feature_names,
                service_name.to_string(),
            )
            .into());
        }
        let mut result: HashMap<Feature, Arc<FeatureView>> = HashMap::new();
        for projection in &service.projections {
            if self
                .registry
                .on_demand_feature_views
                .contains_key(projection.feature_view_name.as_ref())
            {
                return Err(anyhow!("OnDemand feature view for now is not supported"));
            }

            for resolved in &service.resolved_projections {
                for field in resolved.feature_view.features.as_ref() {
                    let feature =
                        Feature::new(resolved.feature_view.name.clone(), field.name.clone());
                    result.insert(feature, resolved.feature_view.clone());
                }
            }
        }
        Ok(result)
    }

    fn feature_views_from_names(
        &self,
        names: &[Feature],
    ) -> Result<HashMap<Feature, Arc<FeatureView>>> {
        names
            .iter()
            .map(|req_feature| -> Result<(Feature, Arc<FeatureView>)> {
                let feature_view_name = req_feature.feature_view_name.as_ref();
                if self
                    .registry
                    .on_demand_feature_views
                    .contains_key(feature_view_name)
                {
                    return Err(anyhow!("OnDemand feature view for now is not supported"));
                }
                let view = self
                    .registry
                    .feature_views
                    .get(feature_view_name)
                    .cloned()
                    .ok_or_else(|| {
                        FeastCoreError::feature_view_not_found(feature_view_name.to_string())
                    })?;
                Ok((req_feature.clone(), Arc::from(view)))
            })
            .collect()
    }

    fn get_feature_views(
        &self,
        requested_features: RequestedFeatures,
    ) -> Result<HashMap<Feature, Arc<FeatureView>>> {
        match requested_features {
            RequestedFeatures::FeatureService(service_name) => {
                self.feature_views_from_service(service_name)
            }
            RequestedFeatures::FeatureNames(names) => {
                let mut bad_requests = vec![];
                let parsed_requested_features: Vec<Feature> = names
                    .iter()
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
    async fn request_to_view_keys<'a>(
        &'a self,
        request: RequestedFeatures<'a>,
    ) -> Result<HashMap<Feature, Arc<FeatureView>>> {
        self.get_feature_views(request)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{Feature, GetOnlineFeaturesRequest, RequestedFeatures};
    use crate::registry::FeatureRegistryService;
    use crate::registry::file_registry::FileFeatureRegistry;
    use anyhow::Result;

    #[test]
    fn create_feature_registry() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let registry_path = std::path::PathBuf::from(&registry_file);
        let feature_registry = FileFeatureRegistry::from_path(&registry_path)?;
        let requested_features = vec![Feature::new("driver_hourly_stats_fresh", "conv_rate")];
        let found_views = feature_registry.feature_views_from_names(&requested_features)?;
        assert_eq!(found_views.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_features_by_name() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let registry_path = std::path::PathBuf::from(registry_file);
        let feature_registry_proto = FileFeatureRegistry::from_path(&registry_path)?;
        let feature_registry_service: Box<dyn FeatureRegistryService> =
            Box::new(feature_registry_proto);
        let mut request_obj = GetOnlineFeaturesRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()].into();
        let requested_features = RequestedFeatures::from(&request_obj);
        let result = feature_registry_service
            .request_to_view_keys(requested_features)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
    #[tokio::test]
    async fn get_features_by_service() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let registry_path = std::path::PathBuf::from(registry_file);
        let feature_registry_proto = FileFeatureRegistry::from_path(&registry_path)?;
        let feature_registry_service: Box<dyn FeatureRegistryService> =
            Box::new(feature_registry_proto);
        let mut request_obj = GetOnlineFeaturesRequest::default();
        request_obj.feature_service = Some("driver_activity_v4".to_string());
        let requested_features = RequestedFeatures::from(&request_obj);
        let result = feature_registry_service
            .request_to_view_keys(requested_features)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
}
