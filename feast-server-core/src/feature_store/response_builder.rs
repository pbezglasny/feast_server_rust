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
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
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

#[derive(Clone, Copy)]
struct EntityPosition {
    entity_idx: usize,
    value_idx: usize,
}

struct GetOnlineFeatureResponseBuilder {
    full_feature_names: bool,
    num_values: usize,
    features: Vec<Arc<str>>,
    results: Vec<FeatureResults>,
    feature_to_idx: HashMap<Feature, usize>,
}

impl GetOnlineFeatureResponseBuilder {
    fn new(full_feature_names: bool, num_values: usize, capacity: usize) -> Self {
        Self {
            full_feature_names,
            num_values,
            features: Vec::with_capacity(capacity),
            results: Vec::with_capacity(capacity),
            feature_to_idx: HashMap::default(),
        }
    }

    fn push_entity(&mut self, entity_key_name: Arc<str>, capacity: usize) -> usize {
        let idx = self.features.len();
        self.features.push(entity_key_name);
        self.results.push(FeatureResults {
            values: Vec::with_capacity(capacity),
            statuses: Vec::with_capacity(capacity),
            event_timestamps: Vec::with_capacity(capacity),
        });
        idx
    }

    fn push_entity_value(&mut self, entity_idx: usize, entity_id_value: EntityIdValue) {
        let value = match entity_id_value {
            EntityIdValue::Int(i) => Value {
                val: Some(Val::Int64Val(i)),
            },
            EntityIdValue::String(s) => Value {
                val: Some(Val::StringVal(s)),
            },
        };
        self.results[entity_idx].values.push(ValueWrapper(value));
        self.results[entity_idx].statuses.push(Present);
        self.results[entity_idx]
            .event_timestamps
            .push(DateTime::<Utc>::UNIX_EPOCH.round_subsecs(0));
    }

    fn ensure_feature_slot(
        &mut self,
        feature: &Feature,
        value_count: usize,
        is_entity_less: bool,
        is_missing: bool,
    ) -> usize {
        if let Some(&idx) = self.feature_to_idx.get(feature) {
            return idx;
        }
        let feature_name = self.format_feature_name(feature, is_entity_less, is_missing);
        let idx = self.features.len();
        self.features.push(feature_name);
        self.results.push(FeatureResults {
            values: vec![ValueWrapper(Value { val: None }); value_count],
            statuses: vec![FeatureStatus::NotFound; value_count],
            event_timestamps: vec![DateTime::<Utc>::UNIX_EPOCH; value_count],
        });
        self.feature_to_idx.insert(feature.clone(), idx);
        idx
    }

    fn set_feature_value(
        &mut self,
        feature_idx: usize,
        value_idx: usize,
        value: Value,
        status: FeatureStatus,
        event_ts: DateTime<Utc>,
    ) {
        if let Some(slot) = self.results.get_mut(feature_idx) {
            if value_idx < slot.values.len() {
                slot.values[value_idx] = ValueWrapper(value);
                slot.statuses[value_idx] = status;
                slot.event_timestamps[value_idx] = event_ts;
            }
        }
    }

    fn add_entity_less_feature(
        &mut self,
        feature: Feature,
        value: Value,
        status: FeatureStatus,
        event_ts: DateTime<Utc>,
    ) {
        let feature_name = self.format_feature_name(&feature, true, false);
        self.features.push(feature_name);
        self.results.push(FeatureResults {
            values: vec![ValueWrapper(value); self.num_values],
            statuses: vec![status; self.num_values],
            event_timestamps: vec![event_ts; self.num_values],
        });
    }

    fn add_missing_feature(&mut self, feature: Feature, value_count: usize, is_entity_less: bool) {
        let feature_name = self.format_feature_name(&feature, is_entity_less, true);
        self.features.push(feature_name);
        self.results.push(FeatureResults {
            values: vec![ValueWrapper(Value { val: None }); value_count],
            statuses: vec![FeatureStatus::NotFound; value_count],
            event_timestamps: vec![DateTime::<Utc>::UNIX_EPOCH; value_count],
        });
    }

    fn format_feature_name(
        &self,
        feature: &Feature,
        is_entity_less: bool,
        is_missing: bool,
    ) -> Arc<str> {
        if self.full_feature_names {
            if is_entity_less || is_missing {
                Arc::from(format!(
                    "{}__{}",
                    feature.feature_view_name, feature.feature_name
                ))
            } else {
                Arc::from(format!(
                    "{}.{}",
                    feature.feature_view_name, feature.feature_name
                ))
            }
        } else {
            feature.feature_name.clone()
        }
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
    pub(crate) fn try_from<'a>(
        entity_keys: HashMap<Arc<str>, Vec<EntityIdValue>>,
        rows: Vec<OnlineStoreRow>,
        feature_views: HashMap<&str, Arc<FeatureView>>,
        lookup_mapping: &'a HashMap<EntityColumnRef<'a>, Arc<str>>,
        mut feature_set: HashSet<Feature>,
        full_feature_names: bool,
    ) -> Result<Self> {
        let mut ordered_entities: Vec<(Arc<str>, Vec<EntityIdValue>)> =
            entity_keys.into_iter().collect();
        let entity_count = ordered_entities.len();
        let max_value_count = ordered_entities
            .iter()
            .map(|(_, values)| values.len())
            .max()
            .unwrap_or(0);

        let mut entity_name_to_index: HashMap<Arc<str>, usize> =
            HashMap::with_capacity_and_hasher(entity_count, Default::default());
        for (idx, (name, _)) in ordered_entities.iter().enumerate() {
            entity_name_to_index.insert(name.clone(), idx);
        }

        let mut positions: Vec<EntityPosition> = Vec::new();
        let mut key_index: HashMap<RequestEntityIdKey, usize> = HashMap::default();
        for (entity_idx, (entity_name, values)) in ordered_entities.iter().enumerate() {
            for (value_idx, key) in values.iter().enumerate() {
                let request_key = RequestEntityIdKey {
                    name: entity_name.clone(),
                    value: key.clone(),
                };
                let slot = positions.len();
                positions.push(EntityPosition {
                    entity_idx,
                    value_idx,
                });
                key_index.insert(request_key, slot);
            }
        }

        let mut entity_lengths: Vec<usize> = Vec::with_capacity(entity_count);
        let mut response_builder = GetOnlineFeatureResponseBuilder::new(
            full_feature_names,
            max_value_count,
            entity_count + feature_set.len(),
        );
        for (entity_name, values) in ordered_entities.into_iter() {
            let expected_len = values.len();
            let entity_idx = response_builder.push_entity(entity_name.clone(), expected_len);
            for value in values {
                response_builder.push_entity_value(entity_idx, value);
            }
            entity_lengths.push(expected_len);
        }

        for row in rows {
            let OnlineStoreRow {
                feature_view_name,
                entity_key,
                feature_name,
                value,
                event_ts,
                created_ts: _,
            } = row;

            if entity_key.0.join_keys.len() != 1 || entity_key.0.entity_values.len() != 1 {
                return Err(anyhow!(
                    "Invalid entity key with multiple join keys or entity values"
                ));
            }

            let entity_key_name = entity_key.0.join_keys[0].clone();
            let entity_col_ref = EntityColumnRef::new(feature_view_name.as_ref(), &entity_key_name);
            let lookup_key = lookup_mapping
                .get(&entity_col_ref)
                .expect("programming error: lookup_mapping should contain all entity columns");
            let entity_id_value = entity_key.0.entity_values[0]
                .val
                .as_ref()
                .map(val_to_entity_id_value)
                .transpose()?
                .ok_or(anyhow!("Empty entity id value"))?;
            let request_key = RequestEntityIdKey {
                name: lookup_key.clone(),
                value: entity_id_value.clone(),
            };

            let feature = Feature::new(entity_col_ref.view_name, feature_name);
            let status = get_feature_status(
                &value,
                feature_views.get(entity_col_ref.view_name).cloned(),
                &event_ts,
            );

            if let Some(&slot) = key_index.get(&request_key) {
                let position = positions[slot];
                let value_count = entity_lengths
                    .get(position.entity_idx)
                    .copied()
                    .unwrap_or(0);
                let feature_idx =
                    response_builder.ensure_feature_slot(&feature, value_count, false, false);
                response_builder.set_feature_value(
                    feature_idx,
                    position.value_idx,
                    value,
                    status,
                    event_ts,
                );
                feature_set.remove(&feature);
            } else if lookup_key.as_ref() == DUMMY_ENTITY_ID {
                feature_set.remove(&feature);
                response_builder.add_entity_less_feature(feature, value, status, event_ts);
            } else {
                // Row does not correspond to requested entity keys; ignore it.
            }
        }

        for feature in feature_set.into_iter() {
            if let Some(view_arc) = feature_views.get(feature.feature_view_name.as_ref()) {
                let view = view_arc.as_ref();
                if view.is_entity_less() {
                    response_builder.add_missing_feature(feature, max_value_count, true);
                    continue;
                }

                if let Some(column) = view.entity_columns.first() {
                    let entity_col_ref =
                        EntityColumnRef::new(view.name.as_ref(), column.name.as_ref());
                    if let Some(request_key) = lookup_mapping.get(&entity_col_ref) {
                        if let Some(&entity_idx) = entity_name_to_index.get(request_key) {
                            let len = entity_lengths.get(entity_idx).copied().unwrap_or(0);
                            response_builder.add_missing_feature(feature, len, false);
                            continue;
                        }
                    }
                }
            }
            response_builder.add_missing_feature(feature, max_value_count, false);
        }

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
    use rustc_hash::FxHashMap as HashMap;
    use std::sync::Arc;

    #[test]
    fn try_from_builds_response_with_missing_values() -> Result<()> {
        let mut entity_keys = HashMap::default();
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
            feature_view_name: Arc::from("driver_hourly_stats"),
            entity_key: HashEntityKey(entity_key),
            feature_name: Arc::from("acc_rate"),
            value: feature_value.clone(),
            event_ts,
            created_ts: None,
        };

        let mut feature_view = FeatureView::default();
        feature_view.name = Arc::<str>::from("driver_hourly_stats");
        feature_view.ttl = Duration::seconds(3600);
        feature_view.entity_names = vec![Arc::<str>::from("driver_id")];

        let mut feature_views = HashMap::default();
        let feature = Arc::from(feature_view);
        feature_views.insert("driver_hourly_stats", feature);

        let features: HashSet<Feature> = vec![Feature::new("driver_hourly_stats", "acc_rate")]
            .into_iter()
            .collect();

        let lookup_mapping: HashMap<EntityColumnRef, Arc<str>> = vec![(
            EntityColumnRef::new("driver_hourly_stats", "driver_id"),
            Arc::from("driver_id"),
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
