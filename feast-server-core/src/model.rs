use crate::feast::core::Entity as EntityProto;
use crate::feast::core::FeatureService as FeatureServiceProto;
use crate::feast::core::FeatureSpecV2 as FeatureSpecV2Proto;
use crate::feast::core::FeatureView as FeatureViewProto;
use crate::feast::core::FeatureViewProjection as FeatureViewProjectionProto;
use crate::feast::core::Registry as RegistryProto;
use crate::feast::types::value::Val;
use crate::feast::types::value_type::Enum as ValueTypeEnum;
use crate::feast::types::{value_type, Value};
use crate::util::prost_duration_to_std;
use crate::util::prost_timestamp_to_system_time;
use anyhow::Result;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use prost::Message;
use serde::ser::Error as SerdeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::time::SystemTime;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum EntityId {
    String(String),
    Int(i64),
}

impl EntityId {
    pub fn to_proto_value(&self, output_type: value_type::Enum) -> Result<Value> {
        match self {
            EntityId::String(s) => Ok(Value {
                val: Some(Val::StringVal(s.clone())),
            }),
            EntityId::Int(i) => match output_type {
                value_type::Enum::Int32 => Ok(Value {
                    val: Some(Val::Int32Val(*i as i32)),
                }),
                value_type::Enum::Int64 => Ok(Value {
                    val: Some(Val::Int64Val(*i)),
                }),
                value_type::Enum::String => Ok(Value {
                    val: Some(Val::StringVal(i.to_string())),
                }),
                _ => Err(anyhow!("Unsupported type convertion for number type")),
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetOnlineFeatureRequest {
    pub entities: HashMap<String, Vec<EntityId>>,
    pub feature_service: Option<String>,
    pub features: Vec<String>,
    pub full_feature_names: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetOnlineFeatureResponseMetadata {
    pub feature_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeatureStatus {
    Invalid,
    Present,
    NullValue,
    NotFound,
    OutsideMaxAge,
}

pub struct ValueWrapper(pub Value);

impl ValueWrapper {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let val = Value::decode(bytes)?;
        Ok(Self(val))
    }
}

impl From<EntityId> for ValueWrapper {
    fn from(value: EntityId) -> Self {
        match value {
            EntityId::Int(v) => Self(Value {
                val: Some(Val::Int64Val(v)),
            }),
            EntityId::String(v) => Self(Value {
                val: Some(Val::StringVal(v)),
            }),
        }
    }
}

impl Serialize for ValueWrapper {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0.val {
            None => serializer.serialize_none(),
            Some(v) => match v {
                Val::Int32Val(i) => serializer.serialize_i32(*i),
                Val::Int64Val(i) => serializer.serialize_i64(*i),
                Val::FloatVal(f) => serializer.serialize_f32(*f),
                Val::DoubleVal(d) => serializer.serialize_f64(*d),
                Val::StringVal(s) => serializer.serialize_str(s),
                Val::BytesVal(b) => serializer.serialize_bytes(b),
                Val::BoolVal(b) => serializer.serialize_bool(*b),
                Val::UnixTimestampVal(ts) => serializer.serialize_i64(*ts),
                other => Err(S::Error::custom(format!(
                    "unsupported value variant: {:?}",
                    other
                ))),
            },
        }
    }
}

impl fmt::Debug for ValueWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct FeatureResults {
    pub values: Vec<ValueWrapper>,
    pub statuses: Vec<FeatureStatus>,
    // #[serde(with = "chrono::serde::ts_seconds")]
    pub event_timestamps: Vec<DateTime<Utc>>,
}

#[derive(Debug, Default, Serialize)]
pub struct GetOnlineFeatureResponse {
    pub metadata: GetOnlineFeatureResponseMetadata,
    pub results: Vec<FeatureResults>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Default)]
pub struct Field {
    pub name: String,
    pub value_type: ValueTypeEnum,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureProjection {
    pub name: String,
    pub name_alias: Option<String>,
    pub features: Vec<Field>,
    join_key_map: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureView {
    pub name: String,
    pub features: Vec<Field>,
    pub ttl: Duration,
    pub entity_names: Vec<String>,
    pub entity_columns: Vec<Field>,
}

#[derive(Debug, Clone, Default)]
pub struct LoggingConfig {
    pub sample_rate: f32,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureService {
    pub name: String,
    pub project: String,
    pub created_timestamp: Option<SystemTime>,
    pub last_updated_timestamp: Option<SystemTime>,
    pub projections: Vec<FeatureProjection>,
    pub logging_config: Option<LoggingConfig>,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureRegistry {
    pub entities: HashMap<String, Entity>,
    pub feature_views: HashMap<String, FeatureView>,
    pub feature_services: HashMap<String, FeatureService>,
}

#[derive(Debug, Clone)]
pub enum RequestedFeatures {
    FeatureNames(Vec<String>),
    FeatureService(String),
}

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

impl TryFrom<&str> for RequestedFeature {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
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

impl From<&GetOnlineFeatureRequest> for RequestedFeatures {
    fn from(get_online_feature_request: &GetOnlineFeatureRequest) -> Self {
        if let Some(feature_service) = &get_online_feature_request.feature_service {
            RequestedFeatures::FeatureService(feature_service.clone())
        } else {
            RequestedFeatures::FeatureNames(get_online_feature_request.features.clone())
        }
    }
}

impl TryFrom<EntityProto> for Entity {
    type Error = Error;

    fn try_from(entity_proto: EntityProto) -> Result<Self> {
        let specs = entity_proto.spec.ok_or(anyhow!("Missing entity specs"))?;
        let value_type = ValueTypeEnum::try_from(specs.value_type).map_err(|e| {
            anyhow!(
                "Invalid value type {} for entity {}: {}",
                specs.value_type,
                specs.name,
                e
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
    type Error = Error;

    fn try_from(feature_spec_proto: FeatureSpecV2Proto) -> Result<Self> {
        let value_type = ValueTypeEnum::try_from(feature_spec_proto.value_type).map_err(|e| {
            anyhow!(
                "Invalid value type {} for feature {}: {}",
                feature_spec_proto.value_type,
                feature_spec_proto.name,
                e
            )
        })?;
        Ok(Field {
            name: feature_spec_proto.name,
            value_type,
        })
    }
}

impl TryFrom<FeatureViewProjectionProto> for FeatureProjection {
    type Error = Error;
    fn try_from(projection_proto: FeatureViewProjectionProto) -> Result<Self> {
        let features: Result<Vec<Field>> = projection_proto
            .feature_columns
            .into_iter()
            .map(Field::try_from)
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
    type Error = Error;
    fn try_from(feature_view_proto: FeatureViewProto) -> Result<Self> {
        let spec = feature_view_proto
            .spec
            .ok_or(anyhow!("Missing feature view value"))?;
        let features: Result<Vec<Field>> = spec.features.into_iter().map(Field::try_from).collect();
        Ok(FeatureView {
            name: spec.name,
            features: features?,
            ttl: spec
                .ttl
                .as_ref()
                .map(prost_duration_to_std)
                .unwrap_or(Duration::from_secs(0)),
            entity_names: spec.entities,
            entity_columns: spec
                .entity_columns
                .into_iter()
                .map(|col| Field {
                    name: col.name,
                    value_type: ValueTypeEnum::try_from(col.value_type).unwrap(),
                })
                .collect(),
        })
    }
}

impl TryFrom<FeatureServiceProto> for FeatureService {
    type Error = Error;
    fn try_from(feature_service_proto: FeatureServiceProto) -> Result<Self> {
        let spec = feature_service_proto
            .spec
            .ok_or(anyhow!("Missing feature service specs"))?;
        let metadata = feature_service_proto
            .meta
            .ok_or(anyhow!("Missing feature service metadata"))?;
        let projections: Result<Vec<FeatureProjection>> = spec
            .features
            .into_iter()
            .map(FeatureProjection::try_from)
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
    type Error = Error;
    fn try_from(registry_proto: RegistryProto) -> Result<Self> {
        let entities: Result<HashMap<String, Entity>> = registry_proto
            .entities
            .into_iter()
            .map(|e| {
                let entity = Entity::try_from(e)?;
                Ok((entity.name.clone(), entity))
            })
            .collect();
        let feature_views: Result<HashMap<String, FeatureView>> = registry_proto
            .feature_views
            .into_iter()
            .map(|fv| {
                let feature_view = FeatureView::try_from(fv)?;
                Ok((feature_view.name.clone(), feature_view))
            })
            .collect();
        let feature_services: Result<HashMap<String, FeatureService>> = registry_proto
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
