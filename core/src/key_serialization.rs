use crate::feast::types::EntityKey;
use crate::feast::types::Value;
use crate::feast::types::value::Val;
use crate::feast::types::value_type::Enum;
use crate::model::EntityKeySerializationVersion;
use std::collections::HashMap;

fn serialize_value(value: &Value) -> Result<Vec<u8>, String> {
    let val = value.val.as_ref().ok_or("")?;
    match val {
        Val::Int32Val(v) => {
            let mut bytes = Vec::with_capacity(12);
            bytes.extend((Enum::Int32 as u32).to_le_bytes());
            bytes.extend(4u32.to_le_bytes());
            bytes.extend(v.to_le_bytes());
            Ok(bytes)
        }
        Val::Int64Val(v) => {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend((Enum::Int64 as u32).to_le_bytes());
            bytes.extend(8u32.to_le_bytes());
            bytes.extend(v.to_le_bytes());
            Ok(bytes)
        }
        Val::StringVal(v) => {
            let mut bytes = vec![];
            bytes.extend((Enum::String as u32).to_le_bytes());
            bytes.extend((v.len() as u32).to_le_bytes());
            bytes.extend(v.as_bytes());
            Ok(bytes)
        }
        Val::BytesVal(v) => {
            let mut bytes = vec![];
            bytes.extend((Enum::Bytes as u32).to_le_bytes());
            bytes.extend((v.len() as u32).to_le_bytes());
            bytes.extend(v);
            Ok(bytes)
        }
        _ => Err("Unsupported type".to_string()),
    }
}
pub fn serialize_key(
    entity_key: &EntityKey,
    serializer_version: EntityKeySerializationVersion,
) -> Result<Vec<u8>, String> {
    match serializer_version {
        EntityKeySerializationVersion::V1 => {
            return Err("Unsupported version of key serializer".to_string());
        }
        EntityKeySerializationVersion::V2 => {
            return Err("Unsupported version of key serializer".to_string());
        }
        _ => {}
    }
    let key_map: HashMap<&str, &Value> = entity_key
        .join_keys
        .iter()
        .map(|s| s.as_str())
        .zip(entity_key.entity_values.iter())
        .collect();
    let mut sorted_keys: Vec<&str> = key_map.keys().cloned().collect();
    sorted_keys.sort();
    let mut bytes: Vec<u8> = vec![];
    bytes.extend((sorted_keys.len() as u32).to_le_bytes());
    for key in &sorted_keys {
        bytes.extend((Enum::String as u32).to_le_bytes());
        bytes.extend((key.len() as u32).to_le_bytes());
        bytes.extend(key.bytes());
    }
    for key in &sorted_keys {
        let value = key_map.get(key).ok_or("Key not found in map")?;
        let value_bytes = serialize_value(value)?;
        bytes.extend(value_bytes);
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn std_hex(bytes: &[u8]) -> String {
        "0x".to_string()
            + &bytes
                .iter()
                .map(|b| format!("{:02x}", b).to_uppercase())
                .collect::<String>()
    }

    #[test]
    fn test_serialize_key() {
        let entity_key = EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1005)),
            }],
        };
        let serialized = serialize_key(&entity_key, EntityKeySerializationVersion::V3).unwrap();
        let serialized_str = std_hex(&serialized);
        let expected =
            "0x0100000002000000090000006472697665725F69640400000008000000ED03000000000000";
        assert_eq!(serialized_str, expected);
    }
}
