use crate::feast::types::value::Val;
use crate::feast::types::{EntityKey, Value};
use crate::feature_store::feature_store_impl::{EntityColumnRef, FeatureWithKeys};
use crate::model::FeatureStatus::Present;
use crate::model::{
    DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, EntityIdValue, Feature, FeatureResults, FeatureStatus,
    FeatureType, FeatureView, GetOnlineFeatureResponse, ValueWrapper,
};
use crate::onlinestore::OnlineStoreRow;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Duration, SubsecRound, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct ResponseFeatureRow(Feature, Value, FeatureStatus, DateTime<Utc>);

#[derive(Debug, Clone, PartialEq)]
pub struct TypedFeature {
    pub feature: Feature,
    pub feature_type: FeatureType,
}

impl From<FeatureWithKeys> for TypedFeature {
    fn from(fk: FeatureWithKeys) -> Self {
        Self {
            feature: fk.feature,
            feature_type: fk.feature_type,
        }
    }
}

#[derive(Hash, PartialEq, Eq)]
struct FeatureRef<'a> {
    feature: &'a Feature,
}

impl<'a> From<&'a Feature> for FeatureRef<'a> {
    fn from(feature: &'a Feature) -> Self {
        Self { feature }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestEntityIdKey {
    pub name: Arc<str>,
    pub value: EntityIdValue,
}

fn get_feature_status(
    value: &Value,
    feature_view: Option<Arc<FeatureView>>,
    event_ts: &DateTime<Utc>,
) -> FeatureStatus {
    if value.val.is_none() {
        FeatureStatus::NullValue
    } else if let Some(feature_view) = feature_view {
        if let Some(expiration_time) = event_ts.checked_add_signed(feature_view.ttl) {
            if Utc::now() > expiration_time {
                FeatureStatus::OutsideMaxAge
            } else {
                Present
            }
        } else {
            Present
        }
    } else {
        Present
    }
}

fn val_to_entity_id_value(value: &Val) -> Result<EntityIdValue> {
    match value {
        Val::Int32Val(i) => Ok(EntityIdValue::Int(*i as i64)),
        Val::Int64Val(i) => Ok(EntityIdValue::Int(*i)),
        Val::StringVal(s) => Ok(EntityIdValue::String(s.clone())),
        other => Err(anyhow!("Unsupported entity value type: {:?}", other)),
    }
}

fn entity_less_request_entity_key() -> RequestEntityIdKey {
    RequestEntityIdKey {
        name: Arc::<str>::from(DUMMY_ENTITY_ID),
        value: EntityIdValue::String(DUMMY_ENTITY_VAL.to_string()),
    }
}

fn group_rows(
    rows: Vec<OnlineStoreRow>,
    feature_views: &HashMap<&str, Arc<FeatureView>>,
    lookup_mapping: &HashMap<EntityColumnRef, Arc<str>>,
) -> Result<HashMap<RequestEntityIdKey, Vec<ResponseFeatureRow>>> {
    let mut result: HashMap<RequestEntityIdKey, Vec<ResponseFeatureRow>> = HashMap::new();
    for row in rows.into_iter() {
        let OnlineStoreRow {
            feature_view_name,
            entity_key,
            feature_name,
            value,
            event_ts,
            created_ts,
        } = row;
        if entity_key.0.join_keys.len() != 1 || entity_key.0.entity_values.len() != 1 {
            return Err(anyhow!(
                "Invalid entity key with multiple join keys or entity values"
            ));
        }
        let entity_key_name = entity_key.0.join_keys[0].clone();
        let entity_col_ref =
            // todo replace returned row with reference to avoid allocation
            EntityColumnRef::new(Arc::from(feature_view_name), Arc::from(entity_key_name));
        let lookup_key = lookup_mapping
            .get(&entity_col_ref)
            .expect("programming error: lookup_mapping should contain all entity columns");
        let entity_id_value = entity_key.0.entity_values[0]
            .val
            .as_ref()
            .map(val_to_entity_id_value)
            .transpose()?
            .ok_or(anyhow!("Empty entity id value"))?;
        let request_entity_key = RequestEntityIdKey {
            name: lookup_key.clone(),
            value: entity_id_value.clone(),
        };
        let status: FeatureStatus = get_feature_status(
            &value,
            feature_views
                .get(entity_col_ref.view_name.as_ref())
                .cloned(),
            &event_ts,
        );
        result
            .entry(request_entity_key)
            .or_default()
            .push(ResponseFeatureRow(
                Feature::new(entity_col_ref.view_name.clone(), feature_name),
                value,
                status,
                event_ts,
            ));
    }
    Ok(result)
}

struct GetOnlineFeatureResponseBuilder {
    full_feature_names: bool,
    num_features: usize,
    num_values: usize,
    next_feature_idx: usize,
    feature_to_idx: HashMap<Feature, usize>,
    current_entity_idx: usize,
    current_feature_value_idx: usize,
    features: Vec<Arc<str>>,
    results: Vec<FeatureResults>,
}

impl GetOnlineFeatureResponseBuilder {
    fn with_capacity(num_features: usize, num_values: usize) -> Self {
        Self {
            full_feature_names: false,
            num_features,
            num_values,
            next_feature_idx: 1,
            current_entity_idx: 0,
            current_feature_value_idx: 0,
            feature_to_idx: HashMap::new(),
            features: Vec::with_capacity(num_features),
            results: Vec::with_capacity(num_features),
        }
    }

    fn set_full_feature_names(mut self, full_feature_names: bool) -> Self {
        self.full_feature_names = full_feature_names;
        self
    }

    fn with_entity_key_name(mut self, entity_key_name: Arc<str>) -> Self {
        if self.features.len() <= self.current_entity_idx {
            self.features.push(entity_key_name);
        } else {
            self.features[self.current_entity_idx] = entity_key_name;
        }
        self
    }

    fn next_feature_value_idx(mut self) -> Self {
        self.current_feature_value_idx += 1;
        self
    }

    fn add_entity_value(mut self, entity_id_value: EntityIdValue) -> Self {
        let value = match entity_id_value {
            EntityIdValue::Int(i) => Value {
                val: Some(Val::Int64Val(i)),
            },
            EntityIdValue::String(s) => Value {
                val: Some(Val::StringVal(s)),
            },
        };
        if self.results.len() <= self.current_entity_idx {
            self.results.push(FeatureResults {
                values: Vec::with_capacity(self.num_values),
                statuses: Vec::with_capacity(self.num_values),
                event_timestamps: Vec::with_capacity(self.num_values),
            });
        }
        self.results[self.current_entity_idx]
            .values
            .push(ValueWrapper(value));
        self.results[self.current_entity_idx].statuses.push(Present);
        self.results[self.current_entity_idx]
            .event_timestamps
            .push(DateTime::<Utc>::UNIX_EPOCH.round_subsecs(0));
        self
    }

    fn add_feature_value(
        mut self,
        feature: Feature,
        value: Value,
        status: FeatureStatus,
        event_ts: DateTime<Utc>,
    ) -> Self {
        let feature_idx = if let Some(idx) = self.feature_to_idx.get(&feature) {
            *idx
        } else {
            let next_idx = self.next_feature_idx;
            self.next_feature_idx += 1;
            self.feature_to_idx.insert(feature.clone(), next_idx);
            next_idx
        };

        if self.results.len() <= feature_idx {
            self.results.push(FeatureResults {
                values: Vec::with_capacity(self.num_values),
                statuses: Vec::with_capacity(self.num_values),
                event_timestamps: Vec::with_capacity(self.num_values),
            });
            let feature_name = if self.full_feature_names {
                Arc::from(format!(
                    "{}.{}",
                    feature.feature_view_name, feature.feature_name
                ))
            } else {
                feature.feature_name
            };
            self.features.push(feature_name);
        }

        self.results[feature_idx].values.push(ValueWrapper(value));
        self.results[feature_idx].statuses.push(status);
        self.results[feature_idx].event_timestamps.push(event_ts);
        self
    }

    fn add_missing_keys(mut self) -> Self {
        for results in self.results.iter_mut() {
            while results.values.len() <= self.current_feature_value_idx {
                results.values.push(ValueWrapper(Value { val: None }));
                results.statuses.push(FeatureStatus::NotFound);
                results
                    .event_timestamps
                    .push(DateTime::<Utc>::UNIX_EPOCH.round_subsecs(0));
            }
        }
        self
    }

    fn add_missing_features(mut self, features: HashSet<Feature>) -> Self {
        for feature in features {
            let feature_name = if self.full_feature_names {
                Arc::from(format!(
                    "{}__{}",
                    feature.feature_view_name, feature.feature_name
                ))
            } else {
                feature.feature_name
            };
            self.features.push(feature_name);
            self.results.push(FeatureResults {
                values: vec![ValueWrapper(Value { val: None }); self.num_values],
                statuses: vec![FeatureStatus::NotFound; self.num_values],
                event_timestamps: vec![DateTime::<Utc>::UNIX_EPOCH; self.num_values],
            });
            self.next_feature_idx += 1;
        }
        self
    }

    fn add_entity_less_features(mut self, rows: Vec<ResponseFeatureRow>) -> Self {
        if rows.is_empty() {
            return self;
        }
        for row in rows {
            let ResponseFeatureRow(feature, value, status, event_ts) = row;
            let Feature {
                feature_view_name,
                feature_name,
            } = feature;
            let feature_name = if self.full_feature_names {
                Arc::from(format!("{}__{}", feature_view_name, feature_name))
            } else {
                feature_name
            };
            self.features.push(feature_name);
            self.results.push(FeatureResults {
                values: vec![ValueWrapper(value); self.num_values],
                statuses: vec![status; self.num_values],
                event_timestamps: vec![event_ts; self.num_values],
            });
            self.next_feature_idx += 1;
        }
        self
    }

    fn next_entity(mut self) -> Self {
        self.current_entity_idx = self.next_feature_idx;
        self.next_feature_idx += 1;
        self.current_feature_value_idx = 0;
        self
    }

    fn build(self) -> GetOnlineFeatureResponse {
        GetOnlineFeatureResponse {
            metadata: crate::model::GetOnlineFeatureResponseMetadata {
                feature_names: self.features,
            },
            results: self.results,
        }
    }
}

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
    pub(crate) fn try_from(
        entity_keys: HashMap<Arc<str>, Vec<EntityIdValue>>,
        rows: Vec<OnlineStoreRow>,
        feature_views: HashMap<&str, Arc<FeatureView>>,
        lookup_mapping: &HashMap<EntityColumnRef, Arc<str>>,
        mut feature_set: HashSet<Feature>,
        full_feature_names: bool,
    ) -> Result<Self> {
        let mut grouped_rows = group_rows(rows, &feature_views, lookup_mapping)?;
        let result_capacity = entity_keys.values().map(|v| v.len()).max().unwrap_or(0);
        let mut response_builder = GetOnlineFeatureResponseBuilder::with_capacity(
            entity_keys.len() + feature_set.len(),
            result_capacity,
        )
        .set_full_feature_names(full_feature_names);

        for (entity_key_name, keys) in entity_keys {
            response_builder = response_builder.with_entity_key_name(entity_key_name.clone());
            for key in keys {
                let request_entity_key = RequestEntityIdKey {
                    name: entity_key_name.clone(),
                    value: key.clone(),
                };
                let feature_values = grouped_rows.remove(&request_entity_key).unwrap_or_default();
                response_builder = response_builder.add_entity_value(key);
                for response_row in feature_values {
                    let ResponseFeatureRow(feature, value, status, event_ts) = response_row;
                    feature_set.remove(&feature);
                    response_builder =
                        response_builder.add_feature_value(feature, value, status, event_ts);
                }
                response_builder = response_builder.add_missing_keys().next_feature_value_idx();
            }
            response_builder = response_builder.next_entity();
        }

        let entity_less_key = entity_less_request_entity_key();
        let rows = grouped_rows.remove(&entity_less_key).unwrap_or_default();
        for row in &rows {
            feature_set.remove(&row.0);
        }
        response_builder = response_builder
            .add_entity_less_features(rows)
            .add_missing_features(feature_set);
        Ok(response_builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feast::types::value::Val;
    use crate::feast::types::{EntityKey, Value};
    use crate::model::HashEntityKey;
    use anyhow::Result;
    use chrono::{Duration, SubsecRound, Utc};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn try_from_builds_response_with_missing_values() -> Result<()> {
        let mut entity_keys = HashMap::new();
        entity_keys.insert(
            Arc::<str>::from("driver_id"),
            vec![EntityIdValue::Int(1001), EntityIdValue::Int(1002)],
        );

        let event_ts = Utc::now().round_subsecs(0);
        let feature_value = Value {
            val: Some(Val::Int64Val(42)),
        };
        let entity_key = Arc::new(EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1001)),
            }],
        });
        let row = OnlineStoreRow {
            feature_view_name: "driver_hourly_stats".to_string(),
            entity_key: HashEntityKey(entity_key),
            feature_name: "acc_rate".to_string(),
            value: feature_value.clone(),
            event_ts,
            created_ts: None,
        };

        let mut feature_view = FeatureView::default();
        feature_view.name = Arc::<str>::from("driver_hourly_stats");
        feature_view.ttl = Duration::seconds(3600);
        feature_view.entity_names = vec![Arc::<str>::from("driver_id")];

        let mut feature_views = HashMap::new();
        let feature = Arc::from(feature_view);
        let feature_name = feature.name.to_string();
        feature_views.insert(feature_name.as_ref(), feature);

        let features: HashSet<Feature> = vec![Feature::new("driver_hourly_stats", "acc_rate")]
            .into_iter()
            .collect();

        let lookup_mapping: HashMap<EntityColumnRef, Arc<str>> = vec![(
            EntityColumnRef::new(
                Arc::<str>::from("driver_hourly_stats"),
                Arc::<str>::from("driver_id"),
            ),
            Arc::<str>::from("driver_id"),
        )]
        .into_iter()
        .collect();

        let response = GetOnlineFeatureResponse::try_from(
            entity_keys,
            vec![row],
            feature_views,
            &lookup_mapping,
            features,
            false,
        )?;

        let mut expected = GetOnlineFeatureResponse::default();
        expected.metadata.feature_names =
            vec![Arc::<str>::from("driver_id"), Arc::<str>::from("acc_rate")];
        expected.results.push(FeatureResults {
            values: vec![
                ValueWrapper::from(EntityIdValue::Int(1001)),
                ValueWrapper::from(EntityIdValue::Int(1002)),
            ],
            statuses: vec![Present, Present],
            event_timestamps: vec![DateTime::<Utc>::UNIX_EPOCH, DateTime::<Utc>::UNIX_EPOCH],
        });

        expected.results.push(FeatureResults {
            values: vec![
                ValueWrapper(feature_value),
                ValueWrapper(Value { val: None }),
            ],
            statuses: vec![Present, FeatureStatus::NotFound],
            event_timestamps: vec![event_ts, DateTime::<Utc>::UNIX_EPOCH.round_subsecs(0)],
        });

        assert_eq!(response, expected);
        Ok(())
    }
}
