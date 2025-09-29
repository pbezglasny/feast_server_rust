use crate::feast::core::Registry;
use crate::model::{
    FeatureRegistry, FeatureView, GetOnlineFeatureRequest, RequestedFeature, RequestedFeatures,
};
use crate::registry::FeatureRegistryService;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use prost::Message;
use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io::Read;
use std::sync::Arc;

pub struct FeatureRegistryProto {
    registry: FeatureRegistry,
}

impl FeatureRegistryProto {
    pub fn from_proto(proto_registry: Registry) -> Result<Self> {
        let registry = FeatureRegistry::try_from(proto_registry)?;
        Ok(Self { registry })
    }

    pub fn from_path(registry_file_path: &str) -> Result<Self> {
        let mut file = fs::File::open(registry_file_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let registry_proto = Registry::decode(&*buf)?;
        let registry = FeatureRegistry::try_from(registry_proto)?;
        Ok(Self { registry })
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    fn feature_views_from_service(&self, service_name: &str) -> Vec<&FeatureView> {
        // TODO
        // let service = self.registry.feature_services.get(service_name).ok_or("");
        // service.unwrap().projections;
        Vec::new()
    }

    fn feature_views_from_names(
        &self,
        names: &[RequestedFeature],
    ) -> HashMap<RequestedFeature, FeatureView> {
        names
            .iter()
            .filter_map(|req_feature| {
                self.registry
                    .feature_views
                    .get(req_feature.feature_view_name.as_str())
                    .map(|view| (req_feature.clone(), view.clone()))
            })
            .collect()
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    fn get_feature_views(
        &self,
        requested_features: RequestedFeatures,
    ) -> Result<HashMap<RequestedFeature, FeatureView>> {
        match requested_features {
            RequestedFeatures::FeatureService(service_name) => Ok(HashMap::new()),
            RequestedFeatures::FeatureNames(names) => {
                let mut bad_requests = vec![];
                let parsed_requested_features: Vec<RequestedFeature> = names
                    .into_iter()
                    .map(|f| RequestedFeature::try_from(f.as_str()))
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
                Ok(self.feature_views_from_names(&parsed_requested_features))
            }
        }
    }
}

#[async_trait]
impl FeatureRegistryService for FeatureRegistryProto {
    async fn request_to_view_keys(
        &self,
        request: Arc<GetOnlineFeatureRequest>,
    ) -> Result<HashMap<RequestedFeature, FeatureView>> {
        let requested_features = RequestedFeatures::from(request.as_ref());
        self.get_feature_views(requested_features)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{GetOnlineFeatureRequest, RequestedFeature};
    use crate::registry::feature_registry::FeatureRegistryProto;
    use crate::registry::FeatureRegistryService;
    use anyhow::Result;
    use std::sync::Arc;

    #[test]
    fn create_feature_registry() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry = FeatureRegistryProto::from_path(&registry_file)?;
        let requested_features = vec![RequestedFeature {
            feature_view_name: "driver_hourly_stats_fresh".to_string(),
            feature_name: "conv_rate".to_string(),
        }];
        let found_views = feature_registry.feature_views_from_names(&requested_features);
        assert_eq!(found_views.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn get_features_by_name() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry_proto = FeatureRegistryProto::from_path(&registry_file)?;
        let feature_registry_service: Box<dyn FeatureRegistryService> =
            Box::new(feature_registry_proto);
        let mut request_obj = GetOnlineFeatureRequest::default();
        request_obj.features = vec!["driver_hourly_stats_fresh:conv_rate".to_string()];
        let request_arc = Arc::new(request_obj);
        let result = feature_registry_service
            .request_to_view_keys(request_arc)
            .await?;
        println!("{:?}", result);
        Ok(())
    }
}
