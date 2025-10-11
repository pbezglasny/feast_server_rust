use crate::config::EntityKeySerializationVersion;
use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value};
use crate::key_serialization::deserialize_key;
use crate::model::{
    EntityId, Feature, FeatureResults, FeatureStatus, FeatureType, FeatureView,
    GetOnlineFeatureResponse, TypedFeature, ValueWrapper,
};
use crate::onlinestore::OnlineStoreRow;
use anyhow::{Error, Result, anyhow};
use chrono::{DateTime, SubsecRound};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
struct ResponseFeatureRow(Value, FeatureStatus, SystemTime);

impl GetOnlineFeatureResponse {
    /// Build GetOnlineFeatureResponse from entity keys of request data,
    /// online store rows and feature view to ttl mapping.
    ///
    /// Parameters:
    /// `entity_keys` - passed by user entity key for requested features
    /// `rows` - data return by onlinestore
    /// `feature_views` - mapping feature_view name to its declaration
    /// `typed_features` - list of requested features with types
    /// `full_feature_names` - use full feature names in result object
    pub fn try_from(
        entity_keys: HashMap<String, Vec<EntityId>>,
        rows: Vec<OnlineStoreRow>,
        feature_views: HashMap<String, FeatureView>,
        feature_list: Vec<TypedFeature>,
        full_feature_names: bool,
    ) -> Result<Self> {
        // feature name to mapping where key is entity id value from request and values are
        // associated values for that feature
        let mut feature_values: HashMap<
            String,
            HashMap<EntityId, HashMap<Feature, ResponseFeatureRow>>,
        > = HashMap::new();

        // entity key name to set of features from views where this entity is used
        let mut entity_to_features: HashMap<String, HashSet<Feature>> = HashMap::new();

        let mut entity_less_features: Vec<(Feature, ResponseFeatureRow)> = vec![];
        let entity_less_features_set: HashSet<Feature> = feature_list
            .iter()
            .filter(|f| f.feature_type == FeatureType::EntityLess)
            .map(|f| Feature {
                feature_view_name: f.feature.feature_view_name.clone(),
                feature_name: f.feature.feature_name.clone(),
            })
            .collect();

        for row in rows.into_iter() {
            let EntityKey {
                mut join_keys,
                mut entity_values,
            } = deserialize_key(row.entity_key, EntityKeySerializationVersion::V3)?;
            if join_keys.len() != 1 {
                return Err(anyhow!("Len of key is greater than 1"));
            }
            let key_name = join_keys
                .pop()
                .ok_or(anyhow!("Incorrect format of join key"))?;
            let key_value = EntityId::try_from(
                entity_values
                    .pop()
                    .unwrap()
                    .val
                    .ok_or(anyhow!("empty key value"))?,
            )?;
            entity_to_features
                .entry(key_name.clone())
                .or_default()
                .insert(Feature::new(
                    row.feature_view_name.clone(),
                    row.feature_name.clone(),
                ));
            let mut entity_key_entry = feature_values.entry(key_name).or_default();
            let mut entry_values = entity_key_entry.entry(key_value).or_default();
            let value = ValueWrapper::from_bytes(&row.value)?;
            let feature_view_opt = feature_views.get(&row.feature_view_name);
            let status: FeatureStatus = {
                if value.0.val.is_none() {
                    FeatureStatus::NullValue
                } else if let Some(feature_view) = feature_view_opt {
                    let expiration_time = row.event_ts + feature_view.ttl;
                    if SystemTime::now() > expiration_time {
                        FeatureStatus::OutsideMaxAge
                    } else {
                        FeatureStatus::Present
                    }
                } else {
                    FeatureStatus::Present
                }
            };
            let feature = Feature::new(row.feature_view_name.clone(), row.feature_name.clone());
            if entity_less_features_set.contains(&feature) {
                entity_less_features
                    .push((feature, ResponseFeatureRow(value.0, status, row.event_ts)));
                continue;
            }
            entry_values.insert(feature, ResponseFeatureRow(value.0, status, row.event_ts));
        }

        let mut alias_to_original_map: HashMap<String, Vec<String>> = feature_views
            .values()
            .filter_map(|fv| fv.join_key_map.as_ref())
            .fold(HashMap::new(), |mut acc, join_key_mapping| {
                for (original_name, alias_name) in join_key_mapping {
                    acc.entry(alias_name.clone())
                        .or_insert(Vec::new())
                        .push(original_name.clone());
                }
                acc
            })
            .into_iter()
            .collect();

        let mut result = GetOnlineFeatureResponse::default();
        let mut processed_features: HashSet<Feature> = HashSet::new();

        for (entity_key_name, values) in entity_keys {
            let mut lookup_keys: Vec<String> = alias_to_original_map
                .remove(&entity_key_name)
                .unwrap_or_default();
            lookup_keys.push(entity_key_name.clone());
            for lookup_key in &lookup_keys {
                let entity_feature = Feature::entity_feature(lookup_key.clone());
                if processed_features.contains(&entity_feature) {
                    continue;
                }
                processed_features.insert(entity_feature.clone());
                let mut associated_values_map =
                    feature_values.remove(lookup_key).unwrap_or_default();
                let associated_features: HashSet<Feature> = entity_to_features
                    .remove(lookup_key)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|f| !processed_features.contains(f))
                    .collect();
                let mut features: HashMap<Feature, FeatureResults> = HashMap::new();
                for entity_val in &values {
                    let mut values = associated_values_map.remove(entity_val).unwrap_or_default();
                    {
                        let mut entity_result = features.entry(entity_feature.clone()).or_default();
                        entity_result
                            .values
                            .push(ValueWrapper::from(entity_val.clone()));
                        entity_result.statuses.push(FeatureStatus::Present);
                        entity_result.event_timestamps.push(DateTime::UNIX_EPOCH);
                    }
                    for associate_feature in &associated_features {
                        let value_opt = values.remove(associate_feature);
                        let feature_result = features.entry(associate_feature.clone()).or_default();
                        match value_opt {
                            None => {
                                feature_result
                                    .values
                                    .push(ValueWrapper(Value { val: None }));
                                feature_result.statuses.push(FeatureStatus::NotFound);
                                feature_result
                                    .event_timestamps
                                    .push(DateTime::from(SystemTime::UNIX_EPOCH).round_subsecs(0));
                            }
                            Some(ResponseFeatureRow(value, status, event_ts)) => {
                                feature_result.values.push(ValueWrapper(value));
                                feature_result.statuses.push(status);
                                feature_result
                                    .event_timestamps
                                    .push(DateTime::from(event_ts));
                            }
                        }
                    }
                }

                result
                    .metadata
                    .feature_names
                    .push(entity_feature.feature_name.clone());
                result.results.push(
                    features
                        .remove(&Feature::entity_feature(
                            entity_feature.feature_name.clone(),
                        ))
                        .ok_or(anyhow!("Missing values for entity {}", entity_key_name))?,
                );

                for feature in associated_features {
                    result.results.push(features.remove(&feature).ok_or(anyhow!(
                        "Missing values for feature {}",
                        feature.full_name()
                    ))?);
                    let feature_name = if full_feature_names {
                        feature.full_name()
                    } else {
                        feature.feature_name.clone()
                    };
                    result.metadata.feature_names.push(feature_name);
                    processed_features.insert(feature);
                }
            }
        }
        for (feature, ResponseFeatureRow(value, status, event_ts)) in entity_less_features {
            if processed_features.contains(&feature) {
                continue;
            }
            processed_features.insert(feature.clone());
            let size = result.results.get(0).map(|r| r.values.len()).unwrap_or(1);
            let mut feature_result = FeatureResults::default();
            feature_result.values = vec![ValueWrapper(value); size];
            feature_result.statuses = vec![status; size];
            feature_result.event_timestamps = vec![DateTime::from(event_ts); size];
            result.results.push(feature_result);
            let feature_name = if full_feature_names {
                feature.full_name()
            } else {
                feature.feature_name.clone()
            };
            result.metadata.feature_names.push(feature_name);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EntityKeySerializationVersion;
    use crate::feast::types::value::Val;
    use crate::feast::types::{EntityKey, Value};
    use crate::key_serialization::serialize_key;
    use anyhow::Result;
    use chrono::{SubsecRound, Utc};
    use prost::Message;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    #[test]
    fn try_from_builds_response_with_missing_values() -> Result<()> {
        let mut entity_keys = HashMap::new();
        entity_keys.insert(
            "driver_id".to_string(),
            vec![EntityId::Int(1001), EntityId::Int(1002)],
        );

        let event_ts = SystemTime::now();
        let feature_value = Value {
            val: Some(Val::Int64Val(42)),
        };
        let entity_key_bytes = serialize_key(
            &EntityKey {
                join_keys: vec!["driver_id".to_string()],
                entity_values: vec![Value {
                    val: Some(Val::Int64Val(1001)),
                }],
            },
            EntityKeySerializationVersion::V3,
        )?;

        let row = OnlineStoreRow {
            feature_view_name: "driver_hourly_stats".to_string(),
            entity_key: entity_key_bytes,
            feature_name: "acc_rate".to_string(),
            value: feature_value.encode_to_vec(),
            event_ts,
            created_ts: event_ts,
        };

        let mut feature_view = FeatureView::default();
        feature_view.name = "driver_hourly_stats".to_string();
        feature_view.ttl = Duration::from_secs(3600);
        feature_view.entity_names = vec!["driver_id".to_string()];

        let mut feature_views = HashMap::new();
        feature_views.insert(feature_view.name.clone(), feature_view);

        let features = vec![TypedFeature {
            feature: Feature::new("".to_string(), "acc_rate".to_string()),
            feature_type: FeatureType::Plain,
        }];

        let response = GetOnlineFeatureResponse::try_from(
            entity_keys,
            vec![row],
            feature_views,
            features,
            false,
        )?;

        let mut expected = GetOnlineFeatureResponse::default();
        expected.metadata.feature_names = vec!["driver_id".to_string(), "acc_rate".to_string()];
        expected.results.push(FeatureResults {
            values: vec![
                ValueWrapper::from(EntityId::Int(1001)),
                ValueWrapper::from(EntityId::Int(1002)),
            ],
            statuses: vec![FeatureStatus::Present, FeatureStatus::Present],
            event_timestamps: vec![
                chrono::DateTime::<Utc>::UNIX_EPOCH,
                chrono::DateTime::<Utc>::UNIX_EPOCH,
            ],
        });

        expected.results.push(FeatureResults {
            values: vec![
                ValueWrapper(feature_value),
                ValueWrapper(Value { val: None }),
            ],
            statuses: vec![FeatureStatus::Present, FeatureStatus::NotFound],
            event_timestamps: vec![
                chrono::DateTime::<Utc>::from(event_ts),
                chrono::DateTime::<Utc>::from(SystemTime::UNIX_EPOCH).round_subsecs(0),
            ],
        });

        assert_eq!(response, expected);
        Ok(())
    }
}
