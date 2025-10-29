use crate::feast::core::Entity as EntityProto;
use crate::feast::core::FeatureService as FeatureServiceProto;
use crate::feast::core::FeatureSpecV2 as FeatureSpecV2Proto;
use crate::feast::core::FeatureView as FeatureViewProto;
use crate::feast::core::FeatureViewProjection as FeatureViewProjectionProto;
use crate::feast::core::OnDemandFeatureView as OnDemandFeatureViewProto;
use crate::feast::core::Registry as RegistryProto;
use crate::feast::types::value::Val;
use crate::feast::types::value_type::Enum as ValueTypeEnum;
use crate::feast::types::{EntityKey, Value, value_type};
use crate::util::prost_duration_to_duration;
use crate::util::prost_timestamp_to_datetime;
use anyhow::{Context, Result};
use anyhow::{Error, anyhow};
use chrono::{DateTime, Duration, Utc};
use prost::Message;
use serde::ser::Error as SerdeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub(crate) const DUMMY_ENTITY_ID: &str = "__dummy_id";
pub(crate) const DUMMY_ENTITY_NAME: &str = "__dummy";
pub(crate) const DUMMY_ENTITY_VAL: &str = "";
pub(crate) const DUMMY_ENTITY_VALUE_TYPE: ValueTypeEnum = ValueTypeEnum::String;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum EntityIdValue {
    String(String),
    Int(i64),
}

impl EntityIdValue {
    pub fn to_proto_value(&self, output_type: value_type::Enum) -> Result<Value> {
        match self {
            EntityIdValue::String(s) => Ok(Value {
                val: Some(Val::StringVal(s.clone())),
            }),
            EntityIdValue::Int(i) => match output_type {
                value_type::Enum::Int32 => Ok(Value {
                    val: Some(Val::Int32Val(*i as i32)),
                }),
                value_type::Enum::Int64 => Ok(Value {
                    val: Some(Val::Int64Val(*i)),
                }),
                value_type::Enum::String => Ok(Value {
                    val: Some(Val::StringVal(i.to_string())),
                }),
                _ => Err(anyhow!("Unsupported type conversion for number type")),
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GetOnlineFeaturesRequest {
    pub entities: HashMap<String, Vec<EntityIdValue>>,
    pub feature_service: Option<String>,
    pub features: Option<Vec<String>>,
    pub full_feature_names: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GetOnlineFeatureResponseMetadata {
    pub feature_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FeatureStatus {
    Invalid,
    Present,
    NullValue,
    NotFound,
    OutsideMaxAge,
}

#[derive(PartialEq, Clone)]
pub struct ValueWrapper(pub Value);

impl From<EntityIdValue> for ValueWrapper {
    fn from(value: EntityIdValue) -> Self {
        match value {
            EntityIdValue::Int(v) => Self(Value {
                val: Some(Val::Int64Val(v)),
            }),
            EntityIdValue::String(v) => Self(Value {
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

#[derive(Debug, Default, PartialEq, Serialize)]
pub struct FeatureResults {
    pub values: Vec<ValueWrapper>,
    pub statuses: Vec<FeatureStatus>,
    pub event_timestamps: Vec<DateTime<Utc>>,
}

#[derive(Debug, Default, PartialEq, Serialize)]
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

#[derive(Debug, Clone)]
pub struct FeatureProjection {
    pub feature_view_name: Arc<str>,
    pub feature_view_name_alias: Option<Arc<str>>,
    pub features: Vec<Field>,
    pub join_key_map: HashMap<String, String>,
}

impl Default for FeatureProjection {
    fn default() -> Self {
        Self {
            feature_view_name: Arc::<str>::from(""),
            feature_view_name_alias: None,
            features: Vec::new(),
            join_key_map: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeatureView {
    pub name: Arc<str>,
    pub features: Vec<Field>,
    pub ttl: Duration,
    pub entity_names: Vec<String>,
    pub entity_columns: Vec<Field>,
    pub join_key_map: Option<HashMap<String, String>>,
}

impl Default for FeatureView {
    fn default() -> Self {
        Self {
            name: Arc::<str>::from(""),
            features: Vec::new(),
            ttl: Duration::zero(),
            entity_names: Vec::new(),
            entity_columns: Vec::new(),
            join_key_map: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OnDemandFeatureView {
    pub name: String,
    pub project: String,
}

#[derive(Debug, Clone, Default)]
pub struct LoggingConfig {
    pub sample_rate: f32,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureService {
    pub name: String,
    pub project: String,
    pub created_timestamp: Option<DateTime<Utc>>,
    pub last_updated_timestamp: Option<DateTime<Utc>>,
    pub projections: Vec<FeatureProjection>,
    pub logging_config: Option<LoggingConfig>,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureRegistry {
    pub entities: HashMap<String, Entity>,
    pub feature_views: HashMap<String, FeatureView>,
    pub on_demand_feature_views: HashMap<String, OnDemandFeatureView>,
    pub feature_services: HashMap<String, FeatureService>,
}

#[derive(Debug, Clone)]
pub enum RequestedFeatures<'a> {
    FeatureNames(&'a [String]),
    FeatureService(&'a str),
}

/// Implement custom hashing for EntityKey to support using it as a key in HashMap,
struct HashValue<'a>(&'a Value);

/// Added hashing for float and double values by hashing their bit representation as workaround
/// to avoid panic
/// Supposed that entity keys won't contain float or double values
impl<'a> Hash for HashValue<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0.val {
            None => {
                0u8.hash(state);
            }
            Some(v) => match v {
                Val::Int32Val(i) => {
                    1u8.hash(state);
                    i.hash(state);
                }
                Val::Int64Val(i) => {
                    2u8.hash(state);
                    i.hash(state);
                }
                Val::StringVal(s) => {
                    5u8.hash(state);
                    s.hash(state);
                }
                Val::BytesVal(b) => {
                    6u8.hash(state);
                    b.hash(state);
                }
                Val::UnixTimestampVal(ts) => {
                    8u8.hash(state);
                    ts.hash(state);
                }
                Val::BoolVal(b) => {
                    7u8.hash(state);
                    b.hash(state);
                }
                Val::FloatVal(f) => {
                    9u8.hash(state);
                    f.to_bits().hash(state);
                }
                Val::DoubleVal(d) => {
                    10u8.hash(state);
                    d.to_bits().hash(state);
                }

                Val::BytesListVal(lv) => {
                    11u8.hash(state);
                    lv.val.hash(state);
                }
                Val::StringListVal(lv) => {
                    12u8.hash(state);
                    lv.val.hash(state);
                }
                Val::Int32ListVal(lv) => {
                    13u8.hash(state);
                    lv.val.hash(state);
                }
                Val::Int64ListVal(lv) => {
                    14u8.hash(state);
                    lv.val.hash(state);
                }
                Val::DoubleListVal(lv) => {
                    15u8.hash(state);
                    lv.val.iter().for_each(|d| d.to_bits().hash(state));
                }
                Val::FloatListVal(lv) => {
                    16u8.hash(state);
                    lv.val.iter().for_each(|f| f.to_bits().hash(state));
                }
                Val::BoolListVal(lv) => {
                    17u8.hash(state);
                    lv.val.hash(state);
                }
                Val::UnixTimestampListVal(lv) => {
                    18u8.hash(state);
                    lv.val.hash(state);
                }
                Val::NullVal(n) => {
                    19u8.hash(state);
                    n.hash(state);
                }
            },
        }
    }
}

/// Wrapper struct to implement custom hashing for EntityKey
/// Used as key in HashMap for result from online store
#[derive(Debug, Clone, PartialEq)]
pub struct HashEntityKey(pub Arc<EntityKey>);

impl Hash for HashEntityKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for join_key in &self.0.join_keys {
            join_key.hash(state);
        }
        for entity_value in &self.0.entity_values {
            HashValue(entity_value).hash(state);
        }
    }
}

impl Eq for HashEntityKey {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Feature {
    pub feature_view_name: Arc<str>,
    pub feature_name: Arc<str>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FeatureType {
    Plain,
    EntityLess,
}

impl Feature {
    pub fn new(feature_view_name: impl Into<Arc<str>>, feature_name: impl Into<Arc<str>>) -> Self {
        Self {
            feature_view_name: feature_view_name.into(),
            feature_name: feature_name.into(),
        }
    }

    pub fn entity_feature(feature_name: impl Into<Arc<str>>) -> Self {
        Self {
            feature_view_name: Arc::<str>::from(""),
            feature_name: feature_name.into(),
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}__{}", self.feature_view_name, self.feature_name)
    }
}

#[derive(Debug, Clone)]
pub struct RequestedFeatureWithTTL<'a> {
    pub requested_feature: &'a Feature,
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

impl TryFrom<&str> for Feature {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(anyhow!("Empty feature string"));
        }
        if let Some(idx) = s.find(':') {
            let (fv_name, f_name) = s.split_at(idx);
            Ok(Self::new(fv_name, &f_name[1..]))
        } else {
            Ok(Self::entity_feature(s))
        }
    }
}

impl<'a> From<&'a GetOnlineFeaturesRequest> for RequestedFeatures<'a> {
    fn from(get_online_feature_request: &'a GetOnlineFeaturesRequest) -> Self {
        if let Some(feature_service) = &get_online_feature_request.feature_service {
            RequestedFeatures::FeatureService(&feature_service)
        } else if let Some(features) = &get_online_feature_request.features {
            RequestedFeatures::FeatureNames(features)
        } else {
            RequestedFeatures::FeatureNames(&[])
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
            feature_view_name: projection_proto.feature_view_name.into(),
            feature_view_name_alias: Some(projection_proto.feature_view_name_alias.into()),
            features: features?,
            join_key_map: projection_proto.join_key_map,
        })
    }
}

impl FeatureView {
    pub fn is_entity_less(&self) -> bool {
        self.entity_names.len() == 1 && self.entity_names[0] == DUMMY_ENTITY_NAME
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
            name: spec.name.into(),
            features: features?,
            ttl: spec
                .ttl
                .as_ref()
                .map(prost_duration_to_duration)
                .unwrap_or_else(Duration::zero),
            entity_names: spec.entities,
            entity_columns: spec
                .entity_columns
                .into_iter()
                .map(|col| Field {
                    name: col.name,
                    value_type: ValueTypeEnum::try_from(col.value_type).unwrap(),
                })
                .collect(),
            join_key_map: None,
        })
    }
}

impl TryFrom<OnDemandFeatureViewProto> for OnDemandFeatureView {
    type Error = Error;
    fn try_from(odfv_proto: OnDemandFeatureViewProto) -> Result<Self> {
        let spec = odfv_proto
            .spec
            .ok_or(anyhow!("Missing on-demand feature view specs"))?;
        Ok(OnDemandFeatureView {
            name: spec.name,
            project: spec.project,
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
                .map(|ts| prost_timestamp_to_datetime(&ts)),
            last_updated_timestamp: metadata
                .last_updated_timestamp
                .map(|ts| prost_timestamp_to_datetime(&ts)),
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
                Ok((feature_view.name.as_ref().to_string(), feature_view))
            })
            .collect();
        let ondemand_feature_views: Result<HashMap<String, OnDemandFeatureView>> = registry_proto
            .on_demand_feature_views
            .into_iter()
            .map(|odfv| {
                let on_demand_feature_view = OnDemandFeatureView::try_from(odfv)?;
                Ok((on_demand_feature_view.name.clone(), on_demand_feature_view))
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
            on_demand_feature_views: ondemand_feature_views?,
            feature_services: feature_services?,
        })
    }
}

macro_rules! try_from_vec_u8 {
    ($target_type:ty, $proto_type:ty) => {
        impl TryFrom<Vec<u8>> for $target_type {
            type Error = Error;

            fn try_from(value: Vec<u8>) -> Result<Self> {
                let proto = <$proto_type>::decode(value.as_slice())?;
                <$target_type>::try_from(proto)
            }
        }
    };
}

try_from_vec_u8!(Entity, EntityProto);
try_from_vec_u8!(FeatureService, FeatureServiceProto);
try_from_vec_u8!(OnDemandFeatureView, OnDemandFeatureViewProto);
try_from_vec_u8!(FeatureView, FeatureViewProto);
