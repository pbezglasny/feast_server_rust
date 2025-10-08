use crate::config::EntityKeySerializationVersion;
use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value};
use crate::key_serialization::deserialize_key;
use crate::model::{
    EntityId, FeatureResults, FeatureStatus, FeatureView, GetOnlineFeatureResponse, ValueWrapper,
};
use crate::onlinestore::OnlineStoreRow;
use anyhow::{Error, Result, anyhow};
use chrono::{DateTime, SubsecRound};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime};

struct ResponseBuilder {
    entity_keys: HashMap<String, Vec<EntityId>>,
    feature_names: Vec<String>,
}

impl GetOnlineFeatureResponse {
    /// Build GetOnlineFeatureResponse from entity keys of request data,
    /// online store rows and feature view to ttl mapping.
    pub fn try_from(
        entity_keys: HashMap<String, Vec<EntityId>>,
        rows: Vec<OnlineStoreRow>,
        feature_views: HashMap<String, FeatureView>,
    ) -> Result<Self> {
        let mut feature_values: HashMap<
            String,
            HashMap<EntityId, HashMap<String, (Val, FeatureStatus, SystemTime)>>,
        > = HashMap::new();

        let mut entity_to_features: HashMap<String, HashSet<String>> = HashMap::new();

        for row in rows.into_iter() {
            let EntityKey {
                mut join_keys,
                mut entity_values,
            } = deserialize_key(row.entity_key, EntityKeySerializationVersion::V3)?;
            if join_keys.len() != 1 {
                return Err(anyhow!("Len of key is greater than 1"));
            }
            let key_name = join_keys.pop().unwrap();
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
                .insert(row.feature_name.clone());
            let mut entity_key_entry = feature_values.entry(key_name).or_default();
            let mut entry_values = entity_key_entry.entry(key_value).or_default();
            let value = ValueWrapper::from_bytes(&row.value)?;
            let ttl = feature_view_to_ttl.get(&row.feature_view_name);
            let status = if let Some(feature_view) = ttl {
                let expiration_time = row.event_ts + feature_view.ttl;
                if SystemTime::now() > expiration_time {
                    FeatureStatus::OutsideMaxAge
                } else {
                    FeatureStatus::Present
                }
            } else {
                FeatureStatus::Present
            };
            entry_values.insert(
                row.feature_name,
                (value.0.val.unwrap(), status, row.event_ts),
            );
        }

        let mut result = GetOnlineFeatureResponse::default();

        for (entity_key_name, values) in entity_keys {
            let mut associated_values_map =
                feature_values.remove(&entity_key_name).unwrap_or_default();
            let associated_features = entity_to_features
                .remove(&entity_key_name)
                .unwrap_or_default();
            let mut features: HashMap<&str, FeatureResults> = HashMap::new();
            for entity_val in values.into_iter() {
                let mut values = associated_values_map
                    .remove(&entity_val)
                    .unwrap_or_default();
                {
                    let mut entity_result = features.entry(&entity_key_name).or_default();
                    entity_result.values.push(ValueWrapper::from(entity_val));
                    entity_result.statuses.push(FeatureStatus::Present);
                    entity_result.event_timestamps.push(DateTime::UNIX_EPOCH);
                }
                for associate_feature in &associated_features {
                    let value_opt = values.remove(associate_feature);
                    let feature_result = features.entry(associate_feature).or_default();
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
                        Some((val, status, event_ts)) => {
                            feature_result
                                .values
                                .push(ValueWrapper(Value { val: Some(val) }));
                            feature_result.statuses.push(FeatureStatus::Present);
                            feature_result
                                .event_timestamps
                                .push(DateTime::from(event_ts));
                        }
                    }
                }
            }

            result.metadata.feature_names.push(entity_key_name.clone());
            result.results.push(
                features
                    .remove(entity_key_name.as_str())
                    .ok_or(anyhow!("Missing values for feature {}", entity_key_name))?,
            );

            for feature in &associated_features {
                result.results.push(
                    features
                        .remove(feature.as_str())
                        .ok_or(anyhow!("Missing values for feature {}", feature))?,
                );
                result.metadata.feature_names.push(feature.clone());
            }
        }
        Ok(result)
    }
}
