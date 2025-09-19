use crate::feast::core::FeatureService as FeatureServiceProto;
use crate::feast::core::FeatureSpecV2 as FeatureSpecV2Proto;
use crate::feast::core::FeatureView as FeatureViewProto;
use crate::feast::core::FeatureViewProjection as FeatureViewProjectionProto;
use crate::feast::types::value_type::Enum as ValueTypeEnum;
use crate::util::prost_duration_to_std;
use crate::util::prost_timestamp_to_system_time;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOnlineFeatureRequest {
    pub entities: HashMap<String, Vec<String>>,
    pub feature_service: Option<String>,
    pub features: Vec<String>,
    pub full_feature_names: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOnlineFeatureResponse {
    pub field_values: HashMap<String, Vec<Option<String>>>,
}

impl Serialize for ValueTypeEnum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for ValueTypeEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = i32::deserialize(deserializer)?;
        ValueTypeEnum::try_from(v)
            .map_err(|e| serde::de::Error::custom(format!("Invalid ValueTypeEnum: {}", e)))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub value_type: ValueTypeEnum,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureProjection {
    pub name: String,
    pub name_alias: Option<String>,
    pub features: Vec<Field>,
    join_key_map: HashMap<String, String>,
}

// TODO Think about using references for entity names and columns instead of cloning
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureView {
    pub name: String,
    pub features: Vec<Field>,
    pub ttl: Duration,
    pub entity_names: Vec<String>,
    pub entity_columns: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub sample_rate: f32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureService {
    pub name: String,
    pub project: String,
    pub created_timestamp: Option<SystemTime>,
    pub last_updated_timestamp: Option<SystemTime>,
    pub projections: Vec<FeatureProjection>,
    pub logging_config: Option<LoggingConfig>,
}

impl TryFrom<&FeatureSpecV2Proto> for Field {
    type Error = String;

    fn try_from(feature_spec_proto: &FeatureSpecV2Proto) -> Result<Self, String> {
        let value_type = ValueTypeEnum::try_from(feature_spec_proto.value_type).map_err(|e| {
            format!(
                "Invalid value type {} for feature {}: {}",
                feature_spec_proto.value_type, feature_spec_proto.name, e
            )
        })?;
        Ok(Field {
            name: feature_spec_proto.name.clone(),
            value_type,
        })
    }
}

impl TryFrom<&FeatureViewProjectionProto> for FeatureProjection {
    type Error = String;
    fn try_from(projection_proto: &FeatureViewProjectionProto) -> Result<Self, String> {
        let features: Result<Vec<Field>, String> = projection_proto
            .feature_columns
            .iter()
            .map(|f| Field::try_from(f))
            .collect();
        Ok(FeatureProjection {
            name: projection_proto.feature_view_name.clone(),
            name_alias: Some(projection_proto.feature_view_name_alias.clone()),
            features: features?,
            join_key_map: projection_proto.join_key_map.clone(),
        })
    }
}

impl TryFrom<&FeatureViewProto> for FeatureView {
    type Error = String;
    fn try_from(feature_view_proto: &FeatureViewProto) -> Result<Self, String> {
        let spec = feature_view_proto
            .spec
            .as_ref()
            .ok_or("Missing feature view value")?;
        let features: Result<Vec<Field>, String> =
            spec.features.iter().map(|f| Field::try_from(f)).collect();
        Ok(FeatureView {
            name: spec.name.clone(),
            features: features?,
            ttl: spec
                .ttl
                .as_ref()
                .map(|d| prost_duration_to_std(d))
                .unwrap_or(Duration::from_secs(0)),
            entity_names: spec.entities.clone(),
            entity_columns: spec
                .entity_columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
        })
    }
}

impl TryFrom<&FeatureServiceProto> for FeatureService {
    type Error = String;
    fn try_from(feature_service_proto: &FeatureServiceProto) -> Result<Self, String> {
        let spec = feature_service_proto
            .spec
            .as_ref()
            .ok_or("Missing feature service specs")?;
        let metadata = feature_service_proto
            .meta
            .as_ref()
            .ok_or("Missing feature service metadata")?;
        let projections: Result<Vec<FeatureProjection>, String> = spec
            .features
            .iter()
            .map(|p| FeatureProjection::try_from(p))
            .collect();
        Ok(FeatureService {
            name: spec.name.clone(),
            project: spec.project.clone(),
            created_timestamp: metadata
                .created_timestamp
                .map(|ts| prost_timestamp_to_system_time(&ts)),
            last_updated_timestamp: metadata
                .last_updated_timestamp
                .map(|ts| prost_timestamp_to_system_time(&ts)),
            projections: projections?,
            logging_config: None,
        })
    }
}
