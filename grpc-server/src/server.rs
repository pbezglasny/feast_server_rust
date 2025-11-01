use crate::proto::feast::serving::serving_service_server::{ServingService, ServingServiceServer};
use crate::proto::feast::serving::{
    FeatureList, GetFeastServingInfoRequest, GetFeastServingInfoResponse,
    GetOnlineFeaturesRequest as GrpcGetOnlineFeaturesRequest, GetOnlineFeaturesResponse,
    GetOnlineFeaturesResponseMetadata, get_online_features_request, get_online_features_response,
};
use crate::proto::feast::types::{
    self as grpc_types, BoolList as GrpcBoolList, BytesList as GrpcBytesList,
    DoubleList as GrpcDoubleList, FloatList as GrpcFloatList, Int32List as GrpcInt32List,
    Int64List as GrpcInt64List, RepeatedValue as GrpcRepeatedValue, StringList as GrpcStringList,
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use feast_server_core::feast::types::{
    BoolList as CoreBoolList, BytesList as CoreBytesList, DoubleList as CoreDoubleList,
    FloatList as CoreFloatList, Int32List as CoreInt32List, Int64List as CoreInt64List,
    StringList as CoreStringList, Value as CoreValue, value::Val as CoreVal,
};
use feast_server_core::feature_store::FeatureStore;
use feast_server_core::model::{
    EntityIdValue, FeatureResults, FeatureStatus, GetOnlineFeatureResponse,
    GetOnlineFeaturesRequest, ValueWrapper,
};
use prost_types::Timestamp;
use rustc_hash::FxHashMap as HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::fs;
use tonic::Status as TonicStatus;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response};

type GrpcStatus = Box<TonicStatus>;

#[derive(Clone)]
pub struct FeastGrpcService {
    feature_store: Arc<FeatureStore>,
}

impl FeastGrpcService {
    pub fn new(feature_store: FeatureStore) -> Self {
        Self {
            feature_store: Arc::new(feature_store),
        }
    }

    fn from_request_proto(
        request: GrpcGetOnlineFeaturesRequest,
    ) -> Result<GetOnlineFeaturesRequest, GrpcStatus> {
        let mut entities: HashMap<String, Vec<EntityIdValue>> = HashMap::default();
        for (entity_name, values) in request.entities {
            entities.insert(
                entity_name.clone(),
                repeated_value_to_entity_ids(entity_name.as_str(), values)?,
            );
        }

        let (feature_service, features) = match request.kind {
            Some(get_online_features_request::Kind::FeatureService(name)) => (Some(name), None),
            Some(get_online_features_request::Kind::Features(list)) => (None, Some(list.val)),
            None => (None, None),
        };

        if !request.request_context.is_empty() {
            tracing::warn!("gRPC request context is currently ignored");
        }

        Ok(GetOnlineFeaturesRequest {
            entities,
            feature_service,
            features,
            full_feature_names: Some(request.full_feature_names),
        })
    }

    fn to_response_proto(
        response: GetOnlineFeatureResponse,
    ) -> Result<GetOnlineFeaturesResponse, GrpcStatus> {
        let metadata = Some(GetOnlineFeaturesResponseMetadata {
            feature_names: Some(FeatureList {
                val: response
                    .metadata
                    .feature_names
                    .iter()
                    .map(|x| x.to_string())
                    .collect(),
            }),
        });

        let mut results = Vec::with_capacity(response.results.len());
        for feature_result in response.results {
            results.push(feature_result_to_proto(feature_result)?);
        }

        Ok(GetOnlineFeaturesResponse {
            metadata,
            results,
            status: true,
        })
    }
}

#[tonic::async_trait]
impl ServingService for FeastGrpcService {
    async fn get_feast_serving_info(
        &self,
        _request: Request<GetFeastServingInfoRequest>,
    ) -> Result<Response<GetFeastServingInfoResponse>, TonicStatus> {
        let response = GetFeastServingInfoResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        Ok(Response::new(response))
    }

    async fn get_online_features(
        &self,
        request: Request<GrpcGetOnlineFeaturesRequest>,
    ) -> Result<Response<GetOnlineFeaturesResponse>, TonicStatus> {
        let inner = request.into_inner();
        let translated_request = Self::from_request_proto(inner).map_err(|status| *status)?;
        let response = self
            .feature_store
            .get_online_features(translated_request)
            .await
            .map_err(|err| {
                tracing::error!(error = ?err, "Failed to retrieve online features");
                TonicStatus::internal("failed to retrieve online features")
            })?;
        let response = Self::to_response_proto(response).map_err(|status| *status)?;
        Ok(Response::new(response))
    }
}

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6567,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

pub async fn start_server(server_config: ServerConfig, feature_store: FeatureStore) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", server_config.host, server_config.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("Cannot resolve host"))?;

    let service = FeastGrpcService::new(feature_store);
    let mut builder = Server::builder();

    if server_config.tls_enabled {
        let cert_path = server_config
            .tls_cert_path
            .ok_or_else(|| anyhow!("TLS is enabled but cert path is not provided"))?;
        let key_path = server_config
            .tls_key_path
            .ok_or_else(|| anyhow!("TLS is enabled but key path is not provided"))?;

        let (cert, key) = tokio::try_join!(fs::read(cert_path), fs::read(key_path))?;
        let identity = Identity::from_pem(cert, key);
        builder = builder
            .tls_config(ServerTlsConfig::new().identity(identity))
            .map_err(|err| anyhow!("Failed to configure TLS: {}", err))?;
    }

    tracing::info!(
        "gRPC server listening on {}:{}",
        server_config.host,
        server_config.port
    );

    builder
        .add_service(ServingServiceServer::new(service))
        .serve(addr)
        .await
        .map_err(|err| anyhow!("Failed to start gRPC server: {}", err))
}

fn repeated_value_to_entity_ids(
    entity_name: &str,
    repeated_value: GrpcRepeatedValue,
) -> Result<Vec<EntityIdValue>, GrpcStatus> {
    repeated_value
        .val
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let core_value = grpc_value_to_core(value)?;
            let val = core_value.val.ok_or_else(|| {
                Box::new(TonicStatus::invalid_argument(format!(
                    "Missing value for entity {} at index {}",
                    entity_name, index
                )))
            })?;
            EntityIdValue::try_from(val).map_err(|err| {
                Box::new(TonicStatus::invalid_argument(format!(
                    "Invalid value for entity {} at index {}: {}",
                    entity_name, index, err
                )))
            })
        })
        .collect()
}

fn feature_result_to_proto(
    result: FeatureResults,
) -> Result<get_online_features_response::FeatureVector, GrpcStatus> {
    let mut values = Vec::with_capacity(result.values.len());
    for ValueWrapper(value) in result.values {
        values.push(core_value_to_grpc(value)?);
    }
    let statuses: Vec<i32> = result
        .statuses
        .into_iter()
        .map(map_status_to_proto)
        .collect();
    let event_timestamps: Vec<Timestamp> = result
        .event_timestamps
        .into_iter()
        .map(datetime_to_timestamp)
        .collect();

    Ok(get_online_features_response::FeatureVector {
        values,
        statuses,
        event_timestamps,
    })
}

fn map_status_to_proto(status: FeatureStatus) -> i32 {
    match status {
        FeatureStatus::Invalid => 0,
        FeatureStatus::Present => 1,
        FeatureStatus::NullValue => 2,
        FeatureStatus::NotFound => 3,
        FeatureStatus::OutsideMaxAge => 4,
    }
}

fn datetime_to_timestamp(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: dt.timestamp_subsec_nanos() as i32,
    }
}

fn grpc_value_to_core(value: grpc_types::Value) -> Result<CoreValue, GrpcStatus> {
    let core_val = match value.val {
        Some(grpc_types::value::Val::BytesVal(v)) => Some(CoreVal::BytesVal(v)),
        Some(grpc_types::value::Val::StringVal(v)) => Some(CoreVal::StringVal(v)),
        Some(grpc_types::value::Val::Int32Val(v)) => Some(CoreVal::Int32Val(v)),
        Some(grpc_types::value::Val::Int64Val(v)) => Some(CoreVal::Int64Val(v)),
        Some(grpc_types::value::Val::DoubleVal(v)) => Some(CoreVal::DoubleVal(v)),
        Some(grpc_types::value::Val::FloatVal(v)) => Some(CoreVal::FloatVal(v)),
        Some(grpc_types::value::Val::BoolVal(v)) => Some(CoreVal::BoolVal(v)),
        Some(grpc_types::value::Val::UnixTimestampVal(v)) => Some(CoreVal::UnixTimestampVal(v)),
        Some(grpc_types::value::Val::BytesListVal(list)) => {
            Some(CoreVal::BytesListVal(CoreBytesList { val: list.val }))
        }
        Some(grpc_types::value::Val::StringListVal(list)) => {
            Some(CoreVal::StringListVal(CoreStringList { val: list.val }))
        }
        Some(grpc_types::value::Val::Int32ListVal(list)) => {
            Some(CoreVal::Int32ListVal(CoreInt32List { val: list.val }))
        }
        Some(grpc_types::value::Val::Int64ListVal(list)) => {
            Some(CoreVal::Int64ListVal(CoreInt64List { val: list.val }))
        }
        Some(grpc_types::value::Val::DoubleListVal(list)) => {
            Some(CoreVal::DoubleListVal(CoreDoubleList { val: list.val }))
        }
        Some(grpc_types::value::Val::FloatListVal(list)) => {
            Some(CoreVal::FloatListVal(CoreFloatList { val: list.val }))
        }
        Some(grpc_types::value::Val::BoolListVal(list)) => {
            Some(CoreVal::BoolListVal(CoreBoolList { val: list.val }))
        }
        Some(grpc_types::value::Val::UnixTimestampListVal(list)) => {
            Some(CoreVal::UnixTimestampListVal(CoreInt64List {
                val: list.val,
            }))
        }
        Some(grpc_types::value::Val::NullVal(v)) => Some(CoreVal::NullVal(v)),
        None => None,
    };
    Ok(CoreValue { val: core_val })
}

fn core_value_to_grpc(value: CoreValue) -> Result<grpc_types::Value, GrpcStatus> {
    let grpc_val = match value.val {
        Some(CoreVal::BytesVal(v)) => Some(grpc_types::value::Val::BytesVal(v)),
        Some(CoreVal::StringVal(v)) => Some(grpc_types::value::Val::StringVal(v)),
        Some(CoreVal::Int32Val(v)) => Some(grpc_types::value::Val::Int32Val(v)),
        Some(CoreVal::Int64Val(v)) => Some(grpc_types::value::Val::Int64Val(v)),
        Some(CoreVal::DoubleVal(v)) => Some(grpc_types::value::Val::DoubleVal(v)),
        Some(CoreVal::FloatVal(v)) => Some(grpc_types::value::Val::FloatVal(v)),
        Some(CoreVal::BoolVal(v)) => Some(grpc_types::value::Val::BoolVal(v)),
        Some(CoreVal::UnixTimestampVal(v)) => Some(grpc_types::value::Val::UnixTimestampVal(v)),
        Some(CoreVal::BytesListVal(list)) => {
            Some(grpc_types::value::Val::BytesListVal(GrpcBytesList {
                val: list.val,
            }))
        }
        Some(CoreVal::StringListVal(list)) => {
            Some(grpc_types::value::Val::StringListVal(GrpcStringList {
                val: list.val,
            }))
        }
        Some(CoreVal::Int32ListVal(list)) => {
            Some(grpc_types::value::Val::Int32ListVal(GrpcInt32List {
                val: list.val,
            }))
        }
        Some(CoreVal::Int64ListVal(list)) => {
            Some(grpc_types::value::Val::Int64ListVal(GrpcInt64List {
                val: list.val,
            }))
        }
        Some(CoreVal::DoubleListVal(list)) => {
            Some(grpc_types::value::Val::DoubleListVal(GrpcDoubleList {
                val: list.val,
            }))
        }
        Some(CoreVal::FloatListVal(list)) => {
            Some(grpc_types::value::Val::FloatListVal(GrpcFloatList {
                val: list.val,
            }))
        }
        Some(CoreVal::BoolListVal(list)) => {
            Some(grpc_types::value::Val::BoolListVal(GrpcBoolList {
                val: list.val,
            }))
        }
        Some(CoreVal::UnixTimestampListVal(list)) => Some(
            grpc_types::value::Val::UnixTimestampListVal(GrpcInt64List { val: list.val }),
        ),
        Some(CoreVal::NullVal(v)) => Some(grpc_types::value::Val::NullVal(v)),
        None => None,
    };

    Ok(grpc_types::Value { val: grpc_val })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn converts_repeated_value_to_entities() {
        let repeated = GrpcRepeatedValue {
            val: vec![
                grpc_types::Value {
                    val: Some(grpc_types::value::Val::StringVal("driver_1".to_string())),
                },
                grpc_types::Value {
                    val: Some(grpc_types::value::Val::Int64Val(42)),
                },
            ],
        };
        let entities = repeated_value_to_entity_ids("driver_id", repeated).unwrap();
        assert_eq!(
            entities,
            vec![
                EntityIdValue::String("driver_1".to_string()),
                EntityIdValue::Int(42)
            ]
        );
    }

    #[test]
    fn converts_feature_result() {
        let mut result = FeatureResults::default();
        result.values.push(ValueWrapper(CoreValue {
            val: Some(CoreVal::Int64Val(10)),
        }));
        result.statuses.push(FeatureStatus::Present);
        let timestamp = Utc
            .timestamp_opt(1_700_000_000, 123_000_000)
            .single()
            .unwrap();
        result.event_timestamps.push(timestamp);

        let vector = feature_result_to_proto(result).unwrap();
        assert_eq!(vector.values.len(), 1);
        assert_eq!(vector.statuses, vec![1]);
        assert_eq!(vector.event_timestamps.len(), 1);
        assert_eq!(vector.event_timestamps[0].seconds, 1_700_000_000);
        assert_eq!(vector.event_timestamps[0].nanos, 123_000_000);
    }

    #[test]
    fn maps_status_to_proto_enum() {
        assert_eq!(map_status_to_proto(FeatureStatus::Invalid), 0);
        assert_eq!(map_status_to_proto(FeatureStatus::Present), 1);
        assert_eq!(map_status_to_proto(FeatureStatus::NullValue), 2);
        assert_eq!(map_status_to_proto(FeatureStatus::NotFound), 3);
        assert_eq!(map_status_to_proto(FeatureStatus::OutsideMaxAge), 4);
    }
}
