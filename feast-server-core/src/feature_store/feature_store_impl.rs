use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value, value_type};
use crate::intern;
use crate::intern::rodeo_ref;
use crate::model;
use crate::model::{
    DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, EntityIdValue, RequestedEntityKey, Feature, FeatureType,
    FeatureView, GetOnlineFeatureResponse, GetOnlineFeaturesRequest, JoinKeyValue,
    RequestedFeatures,
};
use crate::onlinestore::OnlineStore;
use crate::registry::FeatureRegistryService;
use anyhow::{Result, anyhow};
use lasso::Spur;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::hash_map::Entry;
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

        let GetOnlineFeaturesRequest {
            entities,
            feature_service,
            features,
            full_feature_names,
        } = request;
        let rodeo = intern::rodeo_ref();
        let entities: HashMap<Spur, Vec<EntityIdValue>> = entities
            .into_iter()
            .map(|(e, v)| (rodeo.get_or_intern(&e), v))
            .collect();
        let feature_to_view: HashMap<Feature, Arc<FeatureView>> = self
            .registry
            .request_to_view_keys(requested_features)
            .await?;

        let lookup_mapping =
            build_lookup_key_mapping(&feature_to_view, entities.keys().collect::<Vec<_>>());
        // feature view name to feature view
        let view_name_to_view: HashMap<Spur, Arc<FeatureView>> = feature_to_view
            .values()
            .map(|view| (view.name, view.clone()))
            .collect();

        let features_with_keys: Vec<FeatureWithKeys> =
            feature_views_to_keys(&feature_to_view, &entities, &lookup_mapping)?;

        let mut features: HashMap<RequestedEntityKey, Vec<Feature>> = HashMap::default();

        for feature in features_with_keys.iter() {
            for entity_key in feature.entity_keys.iter() {
                features
                    .entry(entity_key.clone())
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
            entities,
            feature_rows,
            view_name_to_view,
            lookup_mapping,
            feature_set,
            full_feature_names.unwrap_or(false),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeatureWithKeys {
    pub feature: Feature,
    pub feature_type: FeatureType,
    pub entity_keys: Arc<Vec<RequestedEntityKey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EntityColumnRef {
    pub view_name: Spur,
    pub column_name: Spur,
}

impl EntityColumnRef {
    pub(crate) fn new(view_name: Spur, column_name: Spur) -> Self {
        Self {
            view_name,
            column_name,
        }
    }
}

static ENTITY_LESS_FEATURE_KEY: std::sync::LazyLock<Arc<Vec<RequestedEntityKey>>> =
    std::sync::LazyLock::new(|| {
        Arc::new(vec![RequestedEntityKey {
            join_keys: vec![JoinKeyValue {
                join_key: rodeo_ref().get_or_intern(DUMMY_ENTITY_ID),
                value: EntityIdValue::String(DUMMY_ENTITY_VAL.to_string()),
                value_type: value_type::Enum::String,
            }],
        }])
    });

struct LookupKey {
    origin_col_name: Spur,
    lookup: Spur,
    value_type: value_type::Enum,
}

fn build_lookup_key_mapping(
    feature_to_view: &HashMap<Feature, Arc<FeatureView>>,
    entities_from_request: Vec<&Spur>,
) -> HashMap<EntityColumnRef, Spur> {
    let mut mapping = HashMap::with_capacity_and_hasher(feature_to_view.len(), Default::default());
    let rodeo = intern::rodeo_ref();

    for (feature, view) in feature_to_view {
        if view.is_entity_less() {
            continue;
        }
        for col in &view.entity_columns {
            let lookup_name = if let Some(join_key_map) = &view.join_key_map {
                join_key_map
                    .get(&col.name)
                    .filter(|col_name| entities_from_request.contains(col_name))
                    .cloned()
                    .unwrap_or(col.name)
            } else {
                col.name
            };
            let key = EntityColumnRef::new(view.name, col.name);
            mapping.insert(key, lookup_name);
        }
    }
    mapping
}

/// Extract entity keys for each feature view from requested entity keys.
/// Returns a mapping from requested features to shared entity key vectors.
fn feature_views_to_keys(
    feature_to_view: &HashMap<Feature, Arc<FeatureView>>,
    requested_entity_keys: &HashMap<Spur, Vec<EntityIdValue>>,
    lookup_mapping: &HashMap<EntityColumnRef, Spur>,
) -> Result<Vec<FeatureWithKeys>> {
    let mut result = vec![];
    let mut key_cache: HashMap<Vec<Spur>, Arc<Vec<RequestedEntityKey>>> = HashMap::default();
    let rodeo = intern::rodeo_ref();
    for (feature, view) in feature_to_view {
        if view.is_entity_less() {
            result.push(FeatureWithKeys {
                feature: feature.clone(),
                feature_type: FeatureType::EntityLess,
                entity_keys: ENTITY_LESS_FEATURE_KEY.clone(),
            });
        } else {
            let lookup_keys: Vec<LookupKey> = view
                .entity_columns
                .iter()
                .map(|col| {
                    let entity_col_ref = EntityColumnRef::new(view.name, col.name);
                    lookup_mapping
                        .get(&entity_col_ref)
                        .map(|lookup| LookupKey {
                            origin_col_name: col.name,
                            lookup: *lookup,
                            value_type: col.value_type,
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "Missing entity column mapping for column {} in feature view {}",
                                rodeo.resolve(&col.name),
                                rodeo.resolve(&view.name)
                            )
                        })
                })
                .collect::<Result<Vec<LookupKey>>>()?;
            if lookup_keys.is_empty() {
                return Err(anyhow!(
                    "Feature view {} has no entity columns",
                    rodeo.resolve(&view.name)
                ));
            }
            for lookup_key in &lookup_keys {
                if !requested_entity_keys.contains_key(&lookup_key.lookup) {
                    return Err(anyhow!(
                        "Missing entity key: {} for requested feature {}",
                        rodeo.resolve(&lookup_key.lookup),
                        rodeo.resolve(&feature.feature_name)
                    ));
                }
            }

            let cache_key = lookup_keys
                .iter()
                .map(|lookup_key| lookup_key.origin_col_name)
                .collect::<Vec<Spur>>();
            let entity_keys = match key_cache.entry(cache_key) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    let first_lookup_key = lookup_keys
                        .first()
                        .expect("lookup_keys should not be empty")
                        .lookup;
                    let num_entities = requested_entity_keys[&first_lookup_key].len();

                    let lookup_values_vec: Vec<_> = lookup_keys
                        .iter()
                        .map(|lookup_key| &requested_entity_keys[&lookup_key.lookup])
                        .collect();

                    let mut entity_keys_vec = Vec::with_capacity(num_entities);
                    for i in 0..num_entities {
                        let join_key_vals = lookup_keys
                            .iter()
                            .zip(lookup_values_vec.iter())
                            .map(|(lookup_key, values)| {
                                JoinKeyValue {
                                    join_key: lookup_key.origin_col_name,
                                    //
                                    value: values[i].clone(),
                                    value_type: lookup_key.value_type,
                                }
                            })
                            .collect::<Vec<JoinKeyValue>>();
                        let entity_key_spur = RequestedEntityKey {
                            join_keys: join_key_vals,
                        };
                        entity_keys_vec.push(entity_key_spur);
                    }
                    Arc::clone(entry.insert(Arc::new(entity_keys_vec)))
                }
            };
            result.push(FeatureWithKeys {
                feature: feature.clone(),
                feature_type: FeatureType::Base,
                entity_keys,
            });
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feast::types::value_type::Enum::{Int32, Int64};
    use crate::feast::types::{value, value_type};
    use crate::intern::rodeo;
    use crate::model::{EntityIdValue, Field, GetOnlineFeaturesRequest};
    use chrono::Duration;
    use rustc_hash::FxHashMap as HashMap;
    use std::sync::Arc;

    trait ToValue {
        fn to_values(&self) -> Vec<EntityIdValue>;
    }

    impl ToValue for i32 {
        fn to_values(&self) -> Vec<EntityIdValue> {
            vec![EntityIdValue::Int(i64::from(*self))]
        }
    }

    impl<T> ToValue for (T, T)
    where
        T: ToValue,
    {
        fn to_values(&self) -> Vec<EntityIdValue> {
            let (first, second) = self;
            vec![first.to_values()[0].clone(), second.to_values()[0].clone()]
        }
    }

    fn build_entity_keys<T: ToValue>(
        join_keys: &[&str],
        entity_values: &[T],
    ) -> Vec<RequestedEntityKey> {
        entity_values
            .iter()
            .map(|values| {
                let join_keys = values
                    .to_values()
                    .iter()
                    .zip(join_keys)
                    .map(|(val, join_key)| JoinKeyValue {
                        join_key: rodeo_ref().get_or_intern(join_key),
                        value: val.clone(),
                        value_type: Int32,
                    })
                    .collect::<Vec<_>>();
                RequestedEntityKey { join_keys }
            })
            .collect()
    }

    fn get_features_views() -> Vec<FeatureView> {
        let feature_view_1 = FeatureView::new(
            "feature_view1",
            vec![],
            Duration::seconds(1),
            vec![rodeo().get_or_intern("entity_1")],
            vec![Field::new("entity_col_1", Int32)],
            None,
        );
        let feature_view_2 = FeatureView::new(
            "feature_view2",
            vec![],
            Duration::seconds(1),
            vec![
                rodeo().get_or_intern("entity_1"),
                rodeo().get_or_intern("entity_2"),
            ],
            vec![
                Field::new("entity_col_1", Int32),
                Field::new("entity_col_2", Int32),
            ],
            None,
        );
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
        let feature_1 = Feature::from_names("feature_view1", "col1");
        let feature_2 = Feature::from_names("feature_view2", "col2");
        let features = HashMap::from_iter([
            (feature_1.clone(), Arc::new(feature_view_1)),
            (feature_2.clone(), Arc::new(feature_view_2)),
        ]);
        let requested_entity_keys = HashMap::from_iter([
            (
                rodeo().get_or_intern("entity_col_1"),
                vec![
                    EntityIdValue::Int(12),
                    EntityIdValue::Int(14),
                    EntityIdValue::Int(16),
                ],
            ),
            (
                rodeo().get_or_intern("entity_col_2"),
                vec![
                    EntityIdValue::Int(22),
                    EntityIdValue::Int(24),
                    EntityIdValue::Int(26),
                ],
            ),
        ]);
        let lookup_mapping =
            build_lookup_key_mapping(&features, requested_entity_keys.keys().collect::<Vec<_>>());
        let mut result = feature_views_to_keys(&features, &requested_entity_keys, &lookup_mapping)?;
        result.sort_by_key(|f| (f.feature.feature_view_name, f.feature.feature_name));
        assert_eq!(result.len(), 2);
        let feature_1 = Feature::from_names("feature_view1", "col1");
        let feature_2 = Feature::from_names("feature_view2", "col2");

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);
        let entity_values_2 = build_entity_keys(
            &vec!["entity_col_1", "entity_col_2"],
            &[(12, 22), (14, 24), (16, 26)],
        );

        let mut expected = vec![
            FeatureWithKeys {
                feature: feature_1,
                feature_type: FeatureType::Base,
                entity_keys: Arc::new(entity_values_1),
            },
            FeatureWithKeys {
                feature: feature_2,
                feature_type: FeatureType::Base,
                entity_keys: Arc::new(entity_values_2),
            },
        ];

        expected.sort_by_key(|f| (f.feature.feature_view_name, f.feature.feature_name));
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
            rodeo().get_or_intern("entity_col_1"),
            rodeo().get_or_intern("alias_1"),
        )]));
        let feature_1 = Feature::from_names("feature_view1", "col1");
        let features = HashMap::from_iter([(feature_1.clone(), Arc::from(feature_view_1))]);
        let requested_entity_keys = HashMap::from_iter([(
            rodeo().get_or_intern("alias_1"),
            vec![
                EntityIdValue::Int(12),
                EntityIdValue::Int(14),
                EntityIdValue::Int(16),
            ],
        )]);
        let lookup_mapping =
            build_lookup_key_mapping(&features, requested_entity_keys.keys().collect::<Vec<_>>());
        let result = feature_views_to_keys(&features, &requested_entity_keys, &lookup_mapping)?;
        assert_eq!(result.len(), 1);
        let feature_1 = Feature::from_names("feature_view1", "col1");

        let entity_values_1 = build_entity_keys(&vec!["entity_col_1"], &[12, 14, 16]);

        let expected = vec![FeatureWithKeys {
            feature: feature_1,
            feature_type: FeatureType::Base,
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
            "driver_id".to_string(),
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
                "truck_id".to_string(),
                vec![EntityIdValue::Int(1002), EntityIdValue::Int(2003)],
            ),
            (
                "driver_id".to_string(),
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
