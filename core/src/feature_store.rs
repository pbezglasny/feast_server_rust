use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value, value_type};
use crate::model::{
    EntityId, FeatureView, GetOnlineFeatureRequest, GetOnlineFeatureResponse, RequestedFeature,
};
use crate::onlinestore::OnlineStore;
use crate::registry::FeatureRegistryService;
use crate::registry::feature_registry::FeatureRegistryProto;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinSet;

struct FeatureStore {
    registry: Arc<dyn FeatureRegistryService>,
    online_store: Arc<dyn OnlineStore>,
}

impl FeatureStore {
    async fn get_online_features(
        &self,
        request: GetOnlineFeatureRequest,
    ) -> Result<GetOnlineFeatureResponse> {
        let request_arc = Arc::new(request);
        let feature_to_view: HashMap<RequestedFeature, FeatureView> = self
            .registry
            .request_to_view_keys(Arc::clone(&request_arc))
            .await;

        let keys_by_view: HashMap<&RequestedFeature, Result<Vec<EntityKey>>> =
            feature_views_to_keys(&feature_to_view, &request_arc.entities);

        let mut view_to_keys: HashMap<String, Vec<EntityKey>> = HashMap::new();
        let mut view_features: HashMap<String, Vec<String>> = HashMap::new();

        for (view_name, result_keys) in keys_by_view.into_iter() {
            match result_keys {
                Ok(kv) => {
                    view_to_keys.insert(view_name.feature_view_name.clone(), kv);
                    view_features
                        .entry(view_name.feature_view_name.clone())
                        .or_default();
                }
                Err(e) => {
                    eprintln!("error building keys: {:?}", e);
                }
            }
        }

        for (requested_feature, _fv) in feature_to_view.into_iter() {
            view_features
                .entry(requested_feature.feature_view_name.clone())
                .or_default()
                .push(requested_feature.feature_name.clone());
        }

        let mut join_set = JoinSet::new();
        for (view_name, entity_keys) in view_to_keys.into_iter() {
            // take the owned features for this view
            let features = view_features.remove(&view_name).unwrap_or_default();
            let online = Arc::clone(&self.online_store);

            join_set.spawn(async move {
                let feature_refs: Vec<&str> = features.iter().map(|s| s.as_str()).collect();
                online
                    .get_feature_values(view_name.as_str(), &entity_keys, &feature_refs)
                    .await
            });
        }

        let online = Arc::clone(&self.online_store);
        join_set.spawn(async move {
            let feature_view = "driver_hourly_stats";
            let entity_key = EntityKey {
                join_keys: vec!["driver_id".to_string()],
                entity_values: vec![Value {
                    val: Some(Val::Int64Val(1005)),
                }],
            };
            let keys = vec![entity_key];
            let features = vec!["conv_rate"];
            online
                .get_feature_values(feature_view, &keys, &features)
                .await
        });

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(val) => println!("{:?}", val),
                Err(e) => eprintln!("Task panicked: {:?}", e),
            }
        }
        todo!()
    }
}

fn feature_views_to_keys<'a>(
    feature_to_view: &'a HashMap<RequestedFeature, FeatureView>,
    requested_entity_keys: &HashMap<String, Vec<EntityId>>,
) -> HashMap<&'a RequestedFeature, Result<Vec<EntityKey>>> {
    // (feature_view, entity_col_name) -> type
    let mut entity_key_type: HashMap<(&str, &str), value_type::Enum> = HashMap::new();
    let mut entity_to_view: HashMap<&str, Vec<&str>> = HashMap::new();
    for feature_view in feature_to_view.values() {
        for entity_col in &feature_view.entity_columns {
            if !entity_to_view.contains_key(entity_col.name.as_str()) {
                entity_to_view.insert(entity_col.name.as_str(), Vec::new());
            }
            entity_to_view
                .get_mut(entity_col.name.as_str())
                .unwrap()
                .push(feature_view.name.as_str());
            entity_key_type.insert(
                (feature_view.name.as_str(), entity_col.name.as_str()),
                entity_col.value_type,
            );
        }
    }

    // view_name to key
    let mut views_keys: HashMap<&str, Vec<EntityKey>> = HashMap::new();
    for (entity_id, entity_keys) in requested_entity_keys {
        for feature_view_name in entity_to_view
            .get(entity_id.as_str())
            .unwrap_or(&Vec::new())
        {
            if !views_keys.contains_key(feature_view_name) {
                views_keys.insert(feature_view_name, Vec::with_capacity(entity_keys.len()));
            }
            let values = views_keys.get_mut(feature_view_name).unwrap();
            for (i, value) in entity_keys.iter().enumerate() {
                if i == values.len() {
                    let entity_key = EntityKey::default();
                    values.push(entity_key);
                }
                let entity_key = values.get_mut(i).unwrap();
                entity_key.join_keys.push(entity_id.clone());
                let col_type = entity_key_type
                    .get(&(*feature_view_name, entity_id.as_str()))
                    .unwrap();
                let val = value.to_proto_value(col_type.clone()).unwrap();
                entity_key.entity_values.push(val);
            }
        }
    }

    let mut result = HashMap::new();
    for (requested_feature, feature_view) in feature_to_view {
        result.insert(
            requested_feature,
            views_keys
                .get(feature_view.name.as_str())
                .map(|v| v.clone())
                .ok_or(anyhow!(
                    "Cannot build entity keys for feature {}_{}",
                    requested_feature.feature_view_name,
                    requested_feature.feature_name
                )),
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::model::{EntityId, Field, GetOnlineFeatureRequest};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn feature_views_to_keys_test() {
        use super::*;
        let feature_view_1 = FeatureView {
            name: "feature_view1".to_string(),
            features: vec![],
            ttl: Duration::new(1, 1),
            entity_names: vec!["entity_1".to_string()],
            entity_columns: vec![Field {
                name: "field1".to_string(),
                value_type: value_type::Enum::Int32,
            }],
        };
        let feature_view_2 = FeatureView {
            name: "feature_view2".to_string(),
            features: vec![],
            ttl: Duration::new(1, 1),
            entity_names: vec!["entity_1".to_string(), "entity_2".to_string()],
            entity_columns: vec![
                Field {
                    name: "field1".to_string(),
                    value_type: value_type::Enum::Int32,
                },
                Field {
                    name: "field2".to_string(),
                    value_type: value_type::Enum::Int32,
                },
            ],
        };
        let feature_1 = RequestedFeature {
            feature_view_name: "feature_view1".to_string(),
            feature_name: "col1".to_string(),
        };
        let feature_2 = RequestedFeature {
            feature_view_name: "feature_view2".to_string(),
            feature_name: "col2".to_string(),
        };
        let features = HashMap::from([(feature_1, feature_view_1), (feature_2, feature_view_2)]);
        let requested_entity_keys = HashMap::from([
            (
                "field1".to_string(),
                vec![EntityId::Int(12), EntityId::Int(14), EntityId::Int(16)],
            ),
            (
                "field2".to_string(),
                vec![EntityId::Int(22), EntityId::Int(24), EntityId::Int(26)],
            ),
        ]);
        let result = feature_views_to_keys(&features, &requested_entity_keys);
        println!("{:?}", result);
    }

    use crate::feature_store::FeatureStore;
    use crate::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
    use crate::registry::feature_registry::FeatureRegistryProto;
    use anyhow::Result;

    #[tokio::test]
    async fn get_features() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry = FeatureRegistryProto::from_path(&registry_file)?;
        let sqlite_path =
            "/Users/pavel/work/rust/feast_rust/dev/golden_hornet/feature_repo/data/online_store.db";
        let sqlite_store = SqliteOnlineStore::from_options(
            sqlite_path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        let sqlite_store_arc = Arc::new(sqlite_store);
        let store = FeatureStore {
            registry: Arc::new(feature_registry),
            online_store: sqlite_store_arc,
        };

        let entities = HashMap::from([("driver_id".to_string(), vec![EntityId::Int(1005)])]);
        let request = GetOnlineFeatureRequest {
            entities,
            feature_service: None,
            features: vec!["driver_hourly_stats".to_string()],
            full_feature_names: Some(false),
        };
        let res = store.get_online_features(request).await;

        Ok(())
    }
}
