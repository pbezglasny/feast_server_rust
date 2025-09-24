use crate::feature_store::RequestedFeature;
use crate::model::{FeatureRegistry, FeatureView, RequestedFeatures};
use anyhow::Result;
use prost::Message;
use std::collections::HashMap;
use std::fs;
use std::io::Read;

struct FeatureRegistryProto {
    registry: FeatureRegistry,
}

impl FeatureRegistryProto {
    fn from_proto(proto_registry: crate::feast::core::Registry) -> Result<Self> {
        let registry = FeatureRegistry::try_from(proto_registry)?;
        Ok(Self { registry })
    }

    fn from_path(registry_file_path: &str) -> Result<Self> {
        let mut file = fs::File::open(registry_file_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let registry_proto = crate::feast::core::Registry::decode(&*buf)?;
        let registry = FeatureRegistry::try_from(registry_proto)?;
        Ok(Self { registry })
    }

    fn feature_views_from_service(&self, service_name: &str) -> Vec<&FeatureView> {
        // TODO
        // let service = self.registry.feature_services.get(service_name).ok_or("");
        // service.unwrap().projections;
        Vec::new()
    }

    fn feature_views_from_names<'a>(
        &self,
        names: &'a [RequestedFeature],
    ) -> HashMap<&'a RequestedFeature, &FeatureView> {
        names
            .iter()
            .filter_map(|req_feature| {
                self.registry
                    .feature_views
                    .get(req_feature.feature_view_name.as_str())
                    .map(|view| (req_feature, view))
            })
            .collect()
    }

    fn get_feature_views(&self, requested_features: RequestedFeatures) -> Vec<FeatureView> {
        match requested_features {
            RequestedFeatures::FeatureService(service_name) => Vec::new(),
            RequestedFeatures::FeatureNames(names) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::feature_registry::FeatureRegistryProto;
    use crate::feature_store::RequestedFeature;
    use anyhow::Result;

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
}
