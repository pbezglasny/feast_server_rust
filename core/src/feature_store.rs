use crate::feast::types::{EntityKey, value_type};
use crate::model::{EntityId, FeatureView};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestedFeature {
    pub feature_view_name: String,
    pub feature_name: String,
}

#[derive(Debug, Clone)]
pub struct RequestedFeatureWithTTL<'a> {
    pub requested_feature: &'a RequestedFeature,
    ttl: Duration,
}

impl<'a> PartialEq for RequestedFeatureWithTTL<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.requested_feature == other.requested_feature
    }
}

impl<'a> Eq for RequestedFeatureWithTTL<'a> {}

impl<'a> Hash for RequestedFeatureWithTTL<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.requested_feature.hash(state);
    }
}

impl RequestedFeature {
    pub fn from_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(anyhow!("Empty feature string"));
        }
        if let Some(idx) = s.find(':') {
            let (fv_name, f_name) = s.split_at(idx);
            Ok(Self {
                feature_view_name: fv_name.to_string(),
                feature_name: f_name[1..].to_string(),
            })
        } else {
            Ok(Self {
                feature_view_name: "".to_string(),
                feature_name: s.to_string(),
            })
        }
    }
}

fn feature_views_to_keys<'a>(
    feature_views: &'a HashMap<&RequestedFeature, &FeatureView>,
    requested_entity_keys: HashMap<String, Vec<EntityId>>,
) -> HashMap<&'a RequestedFeature, Result<Vec<EntityKey>>> {
    // (feature_view, entity_col_name) -> type
    let mut entity_key_type: HashMap<(&str, &str), value_type::Enum> = HashMap::new();
    let mut entity_to_view: HashMap<&str, Vec<&str>> = HashMap::new();
    for feature_view in feature_views.values() {
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
    for (entity_id, entity_keys) in &requested_entity_keys {
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
    for (requested_feature, feature_view) in feature_views {
        result.insert(
            *requested_feature,
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
    use crate::model::Field;

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
        let features =
            HashMap::from([(&feature_1, &feature_view_1), (&feature_2, &feature_view_2)]);
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
        let result = feature_views_to_keys(&features, requested_entity_keys);
        println!("{:?}", result);
    }
}
