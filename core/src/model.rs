use crate::feast::core::Entity as EntityProto;
use crate::feast::core::FeatureService as FeatureServiceProto;
use crate::feast::core::FeatureSpecV2 as FeatureSpecV2Proto;
use crate::feast::core::FeatureView as FeatureViewProto;
use crate::feast::core::FeatureViewProjection as FeatureViewProjectionProto;
use crate::feast::core::Registry as RegistryProto;
use crate::feast::types::value_type::Enum as ValueTypeEnum;
use crate::util::prost_duration_to_std;
use crate::util::prost_timestamp_to_system_time;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityId {
    String(String),
    Int(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOnlineFeatureRequest {
    pub entities: HashMap<String, Vec<EntityId>>,
    pub feature_service: Option<String>,
    pub features: Vec<String>,
    pub full_feature_names: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOnlineFeatureResponse {
    pub field_values: HashMap<String, Vec<Option<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub name: String,
    pub join_key: String,
    pub value_type: ValueTypeEnum,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeatureRegistry {
    pub entities: HashMap<String, Entity>,
    pub feature_views: HashMap<String, FeatureView>,
    pub feature_services: HashMap<String, FeatureService>,
}

#[derive(Debug, Clone)]
pub enum RequestedFeatures<'a> {
    FeatureNames(&'a Vec<String>),
    FeatureService(&'a str),
}

impl<'a> RequestedFeatures<'a> {
    pub fn from_request(get_online_feature_request: &'a GetOnlineFeatureRequest) -> Self {
        if let Some(feature_service) = &get_online_feature_request.feature_service {
            RequestedFeatures::FeatureService(feature_service)
        } else {
            RequestedFeatures::FeatureNames(&get_online_feature_request.features)
        }
    }
}

impl TryFrom<EntityProto> for Entity {
    type Error = String;

    fn try_from(entity_proto: EntityProto) -> Result<Self, String> {
        let specs = entity_proto.spec.ok_or("Missing entity specs")?;
        let value_type = ValueTypeEnum::try_from(specs.value_type).map_err(|e| {
            format!(
                "Invalid value type {} for entity {}: {}",
                specs.value_type, specs.name, e
            )
        })?;
        Ok(Entity {
            name: specs.name,
            join_key: specs.join_key,
            value_type,
        })
    }
}

impl TryFrom<FeatureSpecV2Proto> for Field {
    type Error = String;

    fn try_from(feature_spec_proto: FeatureSpecV2Proto) -> Result<Self, String> {
        let value_type = ValueTypeEnum::try_from(feature_spec_proto.value_type).map_err(|e| {
            format!(
                "Invalid value type {} for feature {}: {}",
                feature_spec_proto.value_type, feature_spec_proto.name, e
            )
        })?;
        Ok(Field {
            name: feature_spec_proto.name,
            value_type,
        })
    }
}

impl TryFrom<FeatureViewProjectionProto> for FeatureProjection {
    type Error = String;
    fn try_from(projection_proto: FeatureViewProjectionProto) -> Result<Self, String> {
        let features: Result<Vec<Field>, String> = projection_proto
            .feature_columns
            .into_iter()
            .map(|f| Field::try_from(f))
            .collect();
        Ok(FeatureProjection {
            name: projection_proto.feature_view_name,
            name_alias: Some(projection_proto.feature_view_name_alias),
            features: features?,
            join_key_map: projection_proto.join_key_map,
        })
    }
}

impl TryFrom<FeatureViewProto> for FeatureView {
    type Error = String;
    fn try_from(feature_view_proto: FeatureViewProto) -> Result<Self, String> {
        let spec = feature_view_proto
            .spec
            .ok_or("Missing feature view value")?;
        let features: Result<Vec<Field>, String> = spec
            .features
            .into_iter()
            .map(|f| Field::try_from(f))
            .collect();
        Ok(FeatureView {
            name: spec.name,
            features: features?,
            ttl: spec
                .ttl
                .as_ref()
                .map(|d| prost_duration_to_std(d))
                .unwrap_or(Duration::from_secs(0)),
            entity_names: spec.entities,
            entity_columns: spec
                .entity_columns
                .into_iter()
                .map(|col| col.name)
                .collect(),
        })
    }
}

impl TryFrom<FeatureServiceProto> for FeatureService {
    type Error = String;
    fn try_from(feature_service_proto: FeatureServiceProto) -> Result<Self, String> {
        let spec = feature_service_proto
            .spec
            .ok_or("Missing feature service specs")?;
        let metadata = feature_service_proto
            .meta
            .ok_or("Missing feature service metadata")?;
        let projections: Result<Vec<FeatureProjection>, String> = spec
            .features
            .into_iter()
            .map(|p| FeatureProjection::try_from(p))
            .collect();
        Ok(FeatureService {
            name: spec.name,
            project: spec.project,
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

impl TryFrom<RegistryProto> for FeatureRegistry {
    type Error = String;
    fn try_from(registry_proto: RegistryProto) -> Result<Self, String> {
        let entities: Result<HashMap<String, Entity>, String> = registry_proto
            .entities
            .into_iter()
            .map(|e| {
                let entity = Entity::try_from(e)?;
                Ok((entity.name.clone(), entity))
            })
            .collect();
        let feature_views: Result<HashMap<String, FeatureView>, String> = registry_proto
            .feature_views
            .into_iter()
            .map(|fv| {
                let feature_view = FeatureView::try_from(fv)?;
                Ok((feature_view.name.clone(), feature_view))
            })
            .collect();
        let feature_services: Result<HashMap<String, FeatureService>, String> = registry_proto
            .feature_services
            .into_iter()
            .map(|fs| {
                let feature_service = FeatureService::try_from(fs)?;
                Ok((feature_service.name.clone(), feature_service))
            })
            .collect();
        Ok(FeatureRegistry {
            entities: entities?,
            feature_views: feature_views?,
            feature_services: feature_services?,
        })
    }
}
