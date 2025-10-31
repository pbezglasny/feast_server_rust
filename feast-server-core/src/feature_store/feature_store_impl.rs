use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value, value_type};
use crate::model;
use crate::model::{
    DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, EntityIdValue, Feature, FeatureType, FeatureView,
    GetOnlineFeatureResponse, GetOnlineFeaturesRequest, HashEntityKey, RequestedFeatures,
};
use crate::onlinestore::OnlineStore;
use crate::registry::FeatureRegistryService;
use anyhow::{Result, anyhow};
use rustc_hash::FxHashMap as HashMap;
use std::collections::{HashSet, hash_map::Entry};
use std::sync::Arc;
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
        request: GetOnlineFeaturesRequest,
    ) -> Result<GetOnlineFeatureResponse> {
        let requested_features: RequestedFeatures = RequestedFeatures::from(&request);
        let feature_to_view: HashMap<Feature, Arc<FeatureView>> = self
            .registry
            .request_to_view_keys(requested_features)
            .await?;

        let lookup_mapping = build_lookup_key_mapping(
            &feature_to_view,
            request
                .entities
                .keys()
                .cloned()
                .collect::<HashSet<Arc<str>>>(),
        );
        // feature view name to feature view
        let view_name_to_view: HashMap<&str, Arc<FeatureView>> = feature_to_view
            .values()
            .map(|view| (view.name.as_ref(), view.clone()))
            .collect();

        let features_with_keys: Vec<FeatureWithKeys> =
            feature_views_to_keys(&feature_to_view, &request.entities, &lookup_mapping)?;

        let mut features: HashMap<HashEntityKey, Vec<Feature>> = HashMap::default();

        for feature in features_with_keys.iter() {
            for entity_key in feature.entity_keys.iter() {
                features
                    .entry(HashEntityKey(entity_key.clone()))
                    .or_default()
                    .push(feature.feature.clone());
            }
        }

        let feature_rows = self.online_store.get_feature_values(features).await?;

        let feature_set = features_with_keys
            .iter()
            .map(|f| f.feature.clone())
            .collect();

        GetOnlineFeatureResponse::try_from(
            request.entities,
            feature_rows,
            view_name_to_view,
            &lookup_mapping,
            feature_set,
            request.full_feature_names.unwrap_or(false),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeatureWithKeys {
    pub feature: Feature,
    pub feature_type: FeatureType,
    pub entity_keys: Arc<Vec<Arc<EntityKey>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EntityColumnRef<'a> {
    pub view_name: &'a str,
    pub column_name: &'a str,
}

impl<'a> EntityColumnRef<'a> {
    pub(crate) fn new(view_name: &'a str, column_name: &'a str) -> Self {
        Self {
            view_name,
            column_name,
        }
    }
}

fn entity_key_for_entity_less_feature() -> Arc<Vec<Arc<EntityKey>>> {
    Arc::new(vec![Arc::new(EntityKey {
        join_keys: vec![DUMMY_ENTITY_ID.to_string()],
        entity_values: vec![Value {
            val: Some(Val::StringVal(DUMMY_ENTITY_VAL.to_string())),
        }],
    })])
}

struct LookupKey<'a> {
    origin_col_name: &'a str,
    lookup: &'a str,
    value_type: value_type::Enum,
}

fn build_lookup_key_mapping(
    feature_to_view: &HashMap<Feature, Arc<FeatureView>>,
    entities_from_request: HashSet<Arc<str>>,
) -> HashMap<EntityColumnRef<'_>, Arc<str>> {
    let mut mapping = HashMap::with_capacity_and_hasher(feature_to_view.len(), Default::default());

    for (feature, view) in feature_to_view {
        if view.is_entity_less() {
            continue;
        }
        for col in &view.entity_columns {
            let lookup_name = if let Some(join_key_map) = &view.join_key_map {
                join_key_map
                    .get(&col.name)
                    .filter(|col_name| entities_from_request.contains(col_name.as_ref()))
                    .cloned()
                    .unwrap_or(col.name.clone())
            } else {
                col.name.clone()
            };
            let key = EntityColumnRef::new(view.name.as_ref(), col.name.as_ref());
            mapping.insert(key, lookup_name.clone());
        }
    }
    mapping
}

/// Extract entity keys for each feature view from requested entity keys.
/// Returns a mapping from requested features to shared entity key vectors.
fn feature_views_to_keys(
    feature_to_view: &HashMap<Feature, Arc<FeatureView>>,
    requested_entity_keys: &HashMap<Arc<str>, Vec<EntityIdValue>>,
    lookup_mapping: &HashMap<EntityColumnRef, Arc<str>>,
) -> Result<Vec<FeatureWithKeys>> {
    let mut result = vec![];
    let mut key_cache: HashMap<Vec<&str>, Arc<Vec<Arc<EntityKey>>>> = HashMap::default();
    for (feature, view) in feature_to_view {
        if view.is_entity_less() {
            result.push(FeatureWithKeys {
                feature: feature.clone(),
                feature_type: FeatureType::EntityLess,
                entity_keys: entity_key_for_entity_less_feature(),
            });
        } else {
            let lookup_keys: Vec<LookupKey> = view
                .entity_columns
                .iter()
                .map(|col| {
                    let entity_col_ref =
                        EntityColumnRef::new(view.name.as_ref(), col.name.as_ref());
                    lookup_mapping
                        .get(&entity_col_ref)
                        .map(|lookup| LookupKey {
                            origin_col_name: col.name.as_ref(),
                            lookup: lookup.as_ref(),
                            value_type: col.value_type,
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "Missing entity column mapping for column {} in feature view {}",
                                col.name,
                                view.name.as_ref()
                            )
                        })
                })
                .collect::<Result<Vec<LookupKey>>>()?;
            if lookup_keys.is_empty() {
                return Err(anyhow!(
                    "Feature view {} has no entity columns",
                    view.name.as_ref()
                ));
            }
            for lookup_key in &lookup_keys {
                if !requested_entity_keys.contains_key(lookup_key.lookup) {
                    return Err(anyhow!(
                        "Missing entity key: {} for requested feature {}",
                        &lookup_key.lookup,
                        feature.feature_name
                    ));
                }
            }

            let cache_key = lookup_keys
                .iter()
                .map(|lookup_key| lookup_key.origin_col_name)
                .collect::<Vec<&str>>();
            let entity_keys = match key_cache.entry(cache_key) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    let first_lookup_key = lookup_keys
                        .first()
                        .expect("lookup_keys should not be empty")
                        .lookup;
                    let num_entities = requested_entity_keys[first_lookup_key].len();

                    let lookup_values_vec: Vec<_> = lookup_keys
                        .iter()
                        .map(|lookup_key| &requested_entity_keys[lookup_key.lookup])
                        .collect();

                    let mut entity_keys_vec = Vec::with_capacity(num_entities);
                    for i in 0..num_entities {
                        let entity_values = lookup_keys
                            .iter()
                            .zip(lookup_values_vec.iter())
                            .map(|(lookup_key, values)| {
                                values[i].clone().to_proto_value(lookup_key.value_type)
                            })
                            .collect::<Result<Vec<Value>>>()?;
                        let join_keys = lookup_keys
                            .iter()
                            .map(|lookup_key| lookup_key.origin_col_name.to_string())
                            .collect();
                        entity_keys_vec.push(Arc::new(EntityKey {
                            join_keys,
                            entity_values,
                        }));
                    }
                    Arc::clone(entry.insert(Arc::new(entity_keys_vec)))
                }
            };
            result.push(FeatureWithKeys {
                feature: feature.clone(),
                feature_type: FeatureType::Plain,
                entity_keys,
            });
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feast::types::{value, value_type};
    use crate::model::{EntityIdValue, Field, GetOnlineFeaturesRequest};
    use chrono::Duration;
    use rustc_hash::FxHashMap as HashMap;
    use std::sync::Arc;

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

    fn build_entity_keys<T: ToValue>(
        join_keys: &[&str],
        entity_values: &[T],
    ) -> Vec<Arc<EntityKey>> {
        entity_values
            .iter()
            .map(|v| {
                Arc::new(EntityKey {
                    join_keys: join_keys.iter().map(|s| s.to_string()).collect(),
                    entity_values: v.to_values(),
                })
            })
            .collect()
    }

    fn get_features_views() -> Vec<FeatureView> {
        let feature_view_1 = FeatureView {
            name: Arc::<str>::from("feature_view1"),
            features: Arc::new(vec![]),
            ttl: Duration::seconds(1),
            entity_names: vec![Arc::<str>::from("entity_1")],
            entity_columns: vec![Field {
                name: Arc::<str>::from("entity_col_1"),
                value_type: value_type::Enum::Int32,
            }],
            join_key_map: None,
        };
        let feature_view_2 = FeatureView {
            name: Arc::<str>::from("feature_view2"),
            features: Arc::new(vec![]),
            ttl: Duration::seconds(1),
            entity_names: vec![Arc::<str>::from("entity_1"), Arc::<str>::from("entity_2")],
            entity_columns: vec![
                Field {
                    name: Arc::<str>::from("entity_col_1"),
                    value_type: value_type::Enum::Int32,
                },
                Field {
                    name: Arc::<str>::from("entity_col_2"),
                    value_type: value_type::Enum::Int32,
                },
            ],
            join_key_map: None,
        };
        vec![feature_view_1, feature_view_2]
    }

    fn assert_equal_results(
        result: HashMap<&Feature, Arc<Vec<EntityKey>>>,
        mut expected: HashMap<&Feature, Arc<Vec<EntityKey>>>,
    ) {
        let mut result_keys = result.keys().collect::<Vec<&&Feature>>();
        let mut expected_keys = expected.keys().collect::<Vec<&&Feature>>();
        result_keys.sort();
        expected_keys.sort();
        assert_eq!(result_keys, expected_keys);
        for (key, result_values) in result.into_iter() {
            let result_arc = result_values;
            let result_vec: Vec<EntityKeyWrapper> =
                result_arc.iter().cloned().map(EntityKeyWrapper).collect();
            let expected_arc = expected.remove(key).unwrap();
            let expected_vec: Vec<EntityKeyWrapper> =
                expected_arc.iter().cloned().map(EntityKeyWrapper).collect();
            assert_eq!(result_vec, expected_vec);
        }
    }

    #[test]
    fn feature_views_to_keys_test() -> Result<()> {
        let (feature_view_1, feature_view_2) = {
            let features = get_features_views();
            (features[0].clone(), features[1].clone())
        };
        let feature_1 = Feature::new("feature_view1", "col1");
        let feature_2 = Feature::new("feature_view2", "col2");
        let features = HashMap::from_iter([
            (feature_1.clone(), Arc::new(feature_view_1)),
            (feature_2.clone(), Arc::new(feature_view_2)),
        ]);
        let requested_entity_keys = HashMap::from_iter([
            (
                Arc::<str>::from("entity_col_1"),
                vec![
                    EntityIdValue::Int(12),
                    EntityIdValue::Int(14),
                    EntityIdValue::Int(16),
                ],
            ),
            (
                Arc::<str>::from("entity_col_2"),
                vec![
                    EntityIdValue::Int(22),
                    EntityIdValue::Int(24),
                    EntityIdValue::Int(26),
                ],
            ),
        ]);
        let lookup_mapping = build_lookup_key_mapping(
            &features,
            requested_entity_keys
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
        );
        let mut result = feature_views_to_keys(&features, &requested_entity_keys, &lookup_mapping)?;
        result.sort_by_key(|f| {
            (
                f.feature.feature_view_name.clone(),
                f.feature.feature_name.clone(),
            )
        });
        assert_eq!(result.len(), 2);
        let feature_1 = Feature::new("feature_view1", "col1");
        let feature_2 = Feature::new("feature_view2", "col2");

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);
        let entity_values_2 = build_entity_keys(
            &vec!["entity_col_1", "entity_col_2"],
            &[(12, 22), (14, 24), (16, 26)],
        );

        let mut expected = vec![
            FeatureWithKeys {
                feature: feature_1,
                feature_type: FeatureType::Plain,
                entity_keys: Arc::new(entity_values_1),
            },
            FeatureWithKeys {
                feature: feature_2,
                feature_type: FeatureType::Plain,
                entity_keys: Arc::new(entity_values_2),
            },
        ];

        expected.sort_by_key(|f| {
            (
                f.feature.feature_view_name.clone(),
                f.feature.feature_name.clone(),
            )
        });
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn feature_views_to_keys_mapping_test() -> Result<()> {
        let mut feature_view_1 = {
            let features = get_features_views();
            features[0].clone()
        };
        feature_view_1.join_key_map = Some(HashMap::from_iter([(
            Arc::<str>::from("entity_col_1"),
            Arc::<str>::from("alias_1"),
        )]));
        let feature_1 = Feature::new("feature_view1", "col1");
        let features = HashMap::from_iter([(feature_1.clone(), Arc::from(feature_view_1))]);
        let requested_entity_keys = HashMap::from_iter([(
            Arc::<str>::from("alias_1"),
            vec![
                EntityIdValue::Int(12),
                EntityIdValue::Int(14),
                EntityIdValue::Int(16),
            ],
        )]);
        let lookup_mapping = build_lookup_key_mapping(
            &features,
            requested_entity_keys
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
        );
        let result = feature_views_to_keys(&features, &requested_entity_keys, &lookup_mapping)?;
        assert_eq!(result.len(), 1);
        let feature_1 = Feature::new("feature_view1", "col1");

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);

        let expected = vec![FeatureWithKeys {
            feature: feature_1,
            feature_type: FeatureType::Plain,
            entity_keys: Arc::new(entity_values_1),
        }];
        assert_eq!(result, expected);
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
        let registry_file_path = std::path::PathBuf::from(&registry_file);
        let feature_registry = FileFeatureRegistry::from_path(&registry_file_path)?;
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

        let entities = HashMap::from_iter([(
            Arc::<str>::from("driver_id"),
            vec![
                EntityIdValue::Int(1005),
                EntityIdValue::Int(1002),
                EntityIdValue::Int(2003),
            ],
        )]);
        let request = GetOnlineFeaturesRequest {
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
            match feature.as_ref() {
                "driver_id" => {
                    let vec_res: Vec<Option<Val>> = result.results[i]
                        .values
                        .iter()
                        .map(|v| v.clone().0.val)
                        .collect();
                    assert_eq!(
                        vec_res,
                        vec![
                            Some(Val::Int64Val(1005)),
                            Some(Val::Int64Val(1002)),
                            Some(Val::Int64Val(2003))
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

        let entities = HashMap::from_iter([
            (
                Arc::<str>::from("truck_id"),
                vec![EntityIdValue::Int(1002), EntityIdValue::Int(2003)],
            ),
            (
                Arc::<str>::from("driver_id"),
                vec![EntityIdValue::Int(1002), EntityIdValue::Int(1005)],
            ),
        ]);
        let request = GetOnlineFeaturesRequest {
            entities,
            feature_service: Some("driver_activity_alias".to_string()),
            features: None,
            full_feature_names: Some(false),
        };

        let result = store.get_online_features(request).await?;
        assert_eq!(
            result.metadata.feature_names.len(),
            5,
            "Feature names: {:?}",
            result.metadata.feature_names
        );
        let mut feature_names = result.metadata.feature_names.clone();
        feature_names.sort();
        assert_eq!(
            feature_names,
            vec![
                Arc::<str>::from("acc_rate"),
                Arc::<str>::from("avg_daily_trips"),
                Arc::<str>::from("conv_rate"),
                Arc::<str>::from("driver_id"),
                Arc::<str>::from("truck_id")
            ]
        );
        Ok(())
    }
}
