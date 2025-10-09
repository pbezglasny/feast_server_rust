use crate::feast::types::{EntityKey, value_type};
use crate::model::{
    EntityId, Feature, FeatureView, GetOnlineFeatureRequest, GetOnlineFeatureResponse,
};
use crate::onlinestore::{OnlineStore, OnlineStoreRow};
use crate::registry::FeatureRegistryService;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing;

pub struct FeatureStore {
    registry: Arc<dyn FeatureRegistryService>,
    online_store: Arc<dyn OnlineStore>,
}

impl FeatureStore {
    pub fn new(
        registry: Arc<dyn FeatureRegistryService>,
        online_store: Arc<dyn OnlineStore>,
    ) -> Self {
        Self {
            registry,
            online_store,
        }
    }

    pub async fn get_online_features(
        &self,
        request: GetOnlineFeatureRequest,
    ) -> Result<GetOnlineFeatureResponse> {
        let feature_to_view: HashMap<Feature, FeatureView> =
            self.registry.request_to_view_keys(&request).await?;

        let keys_by_view: HashMap<&Feature, Vec<EntityKey>> =
            feature_views_to_keys(&feature_to_view, &request.entities)?;

        // feature view name to requested entity keys values
        let mut view_to_keys: HashMap<String, Vec<EntityKey>> = HashMap::new();
        // feature view name to features
        let mut view_features: HashMap<String, Vec<String>> = HashMap::new();

        // feature view name to feature view
        let mut view_name_to_view: HashMap<String, FeatureView> = HashMap::new();

        for (view_name, result_keys) in keys_by_view.into_iter() {
            view_to_keys.insert(view_name.feature_view_name.clone(), result_keys);
            view_features
                .entry(view_name.feature_view_name.clone())
                .or_default();
        }

        for (requested_feature, fv) in feature_to_view.into_iter() {
            view_features
                .entry(requested_feature.feature_view_name.clone())
                .or_default()
                .push(requested_feature.feature_name.clone());
            view_name_to_view.insert(fv.name.clone(), fv);
        }

        let mut join_set = JoinSet::new();
        for (view_name, entity_keys) in view_to_keys.into_iter() {
            let features = view_features.remove(&view_name).unwrap_or_default();
            let online = Arc::clone(&self.online_store);

            join_set.spawn(async move {
                let feature_refs: Vec<&str> = features.iter().map(|s| s.as_str()).collect();
                online
                    .get_feature_values(view_name.as_str(), &entity_keys, &feature_refs)
                    .await
            });
        }

        let mut feature_rows = Vec::new();
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(val) => feature_rows.push(val),
                Err(e) => return Err(anyhow!("Error joining online feature task: {:?}", e)),
            }
        }
        let mut errors = vec![];
        let clean_data: Vec<OnlineStoreRow> = feature_rows
            .into_iter()
            .filter_map(|r| r.map_err(|e| errors.push(e)).ok())
            .flatten()
            .collect();
        if !errors.is_empty() {
            return Err(anyhow!(
                "error while getting online data, errors: {:?}",
                errors
            ));
        }
        GetOnlineFeatureResponse::try_from(
            request.entities,
            clean_data,
            view_name_to_view,
            request.full_feature_names.unwrap_or(false),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EntityColumnRef {
    view_name: String,
    column_name: String,
}

impl EntityColumnRef {
    fn new(view_name: String, column_name: String) -> Self {
        Self {
            view_name,
            column_name,
        }
    }
}

/// Extract entity keys for each feature view from requested entity keys.
/// Returns mapping from requested feature to entity keys.
/// TODO: In the returned HashMap, replace the Vec<EntityKey> with &Vec<EntityKey>
fn feature_views_to_keys<'a>(
    feature_to_view: &'a HashMap<Feature, FeatureView>,
    requested_entity_keys: &HashMap<String, Vec<EntityId>>,
) -> Result<HashMap<&'a Feature, Vec<EntityKey>>> {
    let mut entity_key_type: HashMap<EntityColumnRef, value_type::Enum> = HashMap::new();
    // mapping provided entity to list of features views
    let mut entity_to_view: HashMap<String, Vec<&str>> = HashMap::new();
    let mut reverse_join_key_mapping: HashMap<String, Vec<&str>> = HashMap::new();
    for feature_view in feature_to_view.values() {
        if let Some(mapping) = &feature_view.join_key_map {
            for (from, to) in mapping {
                reverse_join_key_mapping
                    .entry(to.clone())
                    .or_default()
                    .push(from);
            }
        }
        for entity_col in &feature_view.entity_columns {
            let entry = entity_to_view.entry(entity_col.name.clone()).or_default();
            entry.push(feature_view.name.as_str());
            entity_key_type.insert(
                EntityColumnRef::new(feature_view.name.clone(), entity_col.name.clone()),
                entity_col.value_type,
            );
        }
    }

    // view_name to key
    let mut views_keys: HashMap<String, Vec<EntityKey>> = HashMap::new();
    for (entity_id, entity_keys_values) in requested_entity_keys {
        let mut possible_keys = reverse_join_key_mapping
            .get(entity_id)
            .cloned()
            .unwrap_or_else(Vec::new);
        possible_keys.push(entity_id.as_str());
        for mapped_key in possible_keys {
            for feature_view_name in entity_to_view.get(mapped_key).unwrap_or(&Vec::new()) {
                let mut value_entry = views_keys
                    .entry(feature_view_name.to_string())
                    .or_insert(Vec::with_capacity(entity_keys_values.len()));
                for (i, value) in entity_keys_values.iter().enumerate() {
                    if i == value_entry.len() {
                        let entity_key = EntityKey::default();
                        value_entry.push(entity_key);
                    }
                    let entity_key = value_entry.get_mut(i).unwrap();
                    entity_key.join_keys.push(mapped_key.to_string());
                    let col_type = entity_key_type
                        .get(&EntityColumnRef::new(
                            feature_view_name.to_string(),
                            mapped_key.to_string(),
                        ))
                        .ok_or_else(|| {
                            anyhow!(
                                "Could not find type for entity column '{}' in feature view '{}'",
                                mapped_key,
                                feature_view_name
                            )
                        })?;
                    let val = value.to_proto_value(*col_type)?;
                    entity_key.entity_values.push(val);
                }
            }
        }
    }

    let mut result = HashMap::new();
    for (requested_feature, feature_view) in feature_to_view {
        result.insert(
            requested_feature,
            views_keys
                .get(feature_view.name.as_str()).cloned()
                .ok_or(anyhow!(
                    "Cannot build entity keys for feature {}_{}. Not all entity columns are provided. Entity columns: {:?} and key_join_mapping [{}]",
                    requested_feature.feature_view_name,
                    requested_feature.feature_name,
                    feature_view.entity_columns,
                    feature_view.join_key_map.as_ref().map(|m| format!("{:?}", m)).unwrap_or_else(|| "None".to_string())
                ))?,
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feast::types::{value, value_type};
    use crate::model::{
        EntityId, Field, GetOnlineFeatureRequest, GetOnlineFeatureResponseMetadata,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    trait ToValue {
        fn to_values(&self) -> Vec<Value>;
    }

    impl ToValue for i32 {
        fn to_values(&self) -> Vec<Value> {
            vec![Value {
                val: Some(value::Val::Int32Val(*self)),
            }]
        }
    }

    impl<T> ToValue for (T, T)
    where
        T: ToValue,
    {
        fn to_values(&self) -> Vec<Value> {
            let (first, second) = self;
            vec![first.to_values()[0].clone(), second.to_values()[0].clone()]
        }
    }

    fn build_entity_keys<T: ToValue>(join_keys: &[&str], entity_values: &[T]) -> Vec<EntityKey> {
        entity_values
            .iter()
            .map(|v| EntityKey {
                join_keys: join_keys.iter().map(|s| s.to_string()).collect(),
                entity_values: v.to_values(),
            })
            .collect()
    }

    fn get_features_views() -> Vec<FeatureView> {
        let feature_view_1 = FeatureView {
            name: "feature_view1".to_string(),
            features: vec![],
            ttl: Duration::new(1, 1),
            entity_names: vec!["entity_1".to_string()],
            entity_columns: vec![Field {
                name: "entity_col_1".to_string(),
                value_type: value_type::Enum::Int32,
            }],
            join_key_map: None,
        };
        let feature_view_2 = FeatureView {
            name: "feature_view2".to_string(),
            features: vec![],
            ttl: Duration::new(1, 1),
            entity_names: vec!["entity_1".to_string(), "entity_2".to_string()],
            entity_columns: vec![
                Field {
                    name: "entity_col_1".to_string(),
                    value_type: value_type::Enum::Int32,
                },
                Field {
                    name: "entity_col_2".to_string(),
                    value_type: value_type::Enum::Int32,
                },
            ],
            join_key_map: None,
        };
        vec![feature_view_1, feature_view_2]
    }

    fn assert_equal_results(
        result: HashMap<&Feature, Vec<EntityKey>>,
        mut expected: HashMap<&Feature, Vec<EntityKey>>,
    ) {
        let mut result_keys = result.keys().collect::<Vec<&&Feature>>();
        let mut expected_keys = expected.keys().collect::<Vec<&&Feature>>();
        result_keys.sort();
        expected_keys.sort();
        assert_eq!(result_keys, expected_keys);
        for (key, result_values) in result.into_iter() {
            let result_vec: Vec<EntityKeyWrapper> = result_values
                .into_iter()
                .map(|e| EntityKeyWrapper(e))
                .collect();
            let expected_vec: Vec<EntityKeyWrapper> = expected
                .remove(key)
                .unwrap()
                .into_iter()
                .map(|e| EntityKeyWrapper(e))
                .collect();
            assert_eq!(result_vec, expected_vec);
        }
    }

    #[test]
    fn feature_views_to_keys_test() -> Result<()> {
        let (feature_view_1, feature_view_2) = {
            let features = get_features_views();
            (features[0].clone(), features[1].clone())
        };
        let feature_1 = Feature {
            feature_view_name: "feature_view1".to_string(),
            feature_name: "col1".to_string(),
        };
        let feature_2 = Feature {
            feature_view_name: "feature_view2".to_string(),
            feature_name: "col2".to_string(),
        };
        let features = HashMap::from([(feature_1, feature_view_1), (feature_2, feature_view_2)]);
        let requested_entity_keys = HashMap::from([
            (
                "entity_col_1".to_string(),
                vec![EntityId::Int(12), EntityId::Int(14), EntityId::Int(16)],
            ),
            (
                "entity_col_2".to_string(),
                vec![EntityId::Int(22), EntityId::Int(24), EntityId::Int(26)],
            ),
        ]);
        let result = feature_views_to_keys(&features, &requested_entity_keys)?;
        assert_eq!(result.len(), 2);
        let feature_1 = Feature {
            feature_view_name: "feature_view1".to_string(),
            feature_name: "col1".to_string(),
        };
        let feature_2 = Feature {
            feature_view_name: "feature_view2".to_string(),
            feature_name: "col2".to_string(),
        };

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);
        let entity_values_2 = build_entity_keys(
            &vec!["entity_col_1", "entity_col_2"],
            &[(12, 22), (14, 24), (16, 26)],
        );

        let mut expected =
            HashMap::from([(&feature_1, entity_values_1), (&feature_2, entity_values_2)]);
        assert_equal_results(result, expected);
        Ok(())
    }

    #[test]
    fn feature_views_to_keys_mapping_test() -> Result<()> {
        let mut feature_view_1 = {
            let features = get_features_views();
            features[0].clone()
        };
        feature_view_1.join_key_map = Some(HashMap::from([(
            "entity_col_1".to_string(),
            "alias_1".to_string(),
        )]));
        let feature_1 = Feature {
            feature_view_name: "feature_view1".to_string(),
            feature_name: "col1".to_string(),
        };
        let features = HashMap::from([(feature_1, feature_view_1)]);
        let requested_entity_keys = HashMap::from([(
            "alias_1".to_string(),
            vec![EntityId::Int(12), EntityId::Int(14), EntityId::Int(16)],
        )]);
        let result = feature_views_to_keys(&features, &requested_entity_keys)?;
        assert_eq!(result.len(), 1);
        let feature_1 = Feature {
            feature_view_name: "feature_view1".to_string(),
            feature_name: "col1".to_string(),
        };

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);

        let mut expected = HashMap::from([(&feature_1, entity_values_1)]);
        assert_equal_results(result, expected);
        Ok(())
    }

    use crate::feast::types::Value;
    use crate::feature_store::feature_store_impl::FeatureStore;
    use crate::onlinestore::sqlite_onlinestore::{ConnectionOptions, SqliteOnlineStore};
    use crate::registry::file_registry::FileFeatureRegistry;
    use crate::util::EntityKeyWrapper;
    use anyhow::Result;

    async fn get_feature_store() -> Result<FeatureStore> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let registry_file = format!("{}/test_data/registry.pb", project_dir);
        let feature_registry = FileFeatureRegistry::from_path(&registry_file)?;
        let sqlite_path = format!("{}/test_data/online_store.db", project_dir);
        let sqlite_store = SqliteOnlineStore::from_options(
            &sqlite_path,
            "golden_hornet".to_string(),
            ConnectionOptions::default(),
        )
        .await?;
        Ok(FeatureStore {
            registry: Arc::new(feature_registry),
            online_store: Arc::new(sqlite_store),
        })
    }

    #[tokio::test]
    async fn get_features() -> Result<()> {
        let store = get_feature_store().await?;

        let entities = HashMap::from([(
            "driver_id".to_string(),
            vec![
                EntityId::Int(1005),
                EntityId::Int(1002),
                EntityId::Int(2003),
            ],
        )]);
        let request = GetOnlineFeatureRequest {
            entities,
            feature_service: None,
            features: Some(vec![
                "driver_hourly_stats_fresh:conv_rate".to_string(),
                "driver_hourly_stats:acc_rate".to_string(),
            ]),
            full_feature_names: Some(false),
        };
        let result = store.get_online_features(request).await?;
        assert_eq!(result.metadata.feature_names.len(), 3);
        assert_eq!(result.results.len(), 3);
        for (i, feature) in result.metadata.feature_names.iter().enumerate() {
            match feature.as_str() {
                "driver_id" => {
                    let vec_res: Vec<Option<value::Val>> = result.results[i]
                        .values
                        .iter()
                        .map(|v| v.clone().0.val)
                        .collect();
                    assert_eq!(
                        vec_res,
                        vec![
                            Some(value::Val::Int64Val(1005)),
                            Some(value::Val::Int64Val(1002)),
                            Some(value::Val::Int64Val(2003))
                        ]
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn get_features_alias() -> Result<()> {
        let store = get_feature_store().await?;

        let entities = HashMap::from([
            (
                "truck_id".to_string(),
                vec![EntityId::Int(1002), EntityId::Int(2003)],
            ),
            (
                "driver_id".to_string(),
                vec![EntityId::Int(1002), EntityId::Int(1005)],
            ),
        ]);
        let request = GetOnlineFeatureRequest {
            entities,
            feature_service: Some("driver_activity_alias".to_string()),
            features: None,
            full_feature_names: Some(false),
        };

        let result = store.get_online_features(request).await?;
        assert_eq!(result.metadata.feature_names.len(), 5);
        let mut feature_names = result.metadata.feature_names.clone();
        feature_names.sort();
        assert_eq!(
            feature_names,
            vec![
                "acc_rate".to_string(),
                "avg_daily_trips".to_string(),
                "conv_rate".to_string(),
                "driver_id".to_string(),
                "truck_id".to_string()
            ]
        );
        Ok(())
    }
}
