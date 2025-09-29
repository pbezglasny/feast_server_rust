use crate::config::EntityKeySerializationVersion;
use crate::feast::types::EntityKey;
use crate::feast::types::Value;
use crate::feast::types::value::Val;
use crate::feast::types::value_type::Enum;
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;

fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    let val = value.val.as_ref().ok_or(anyhow!("Missing value"))?;
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
        _ => Err(anyhow!("Unsupported type")),
    }
}

fn deserialize_val(bytes: &[u8], mut idx: usize) -> (Result<(Val, usize)>) {
    let value_type_int: i32 = i32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
    let value_type = Enum::try_from(value_type_int).with_context(|| {
        format!(
            "Failed to convert i32 value {} to value type",
            value_type_int
        )
    })?;
    idx += 4;
    match value_type {
        Enum::Int32 => {
            let size: u32 = u32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
            if size != 4 {
                return Err(anyhow!("Incorrect size of serialized int 32"));
            }
            idx += 4;
            let val_int = i32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
            idx += 4;
            Ok((Val::Int32Val(val_int), idx))
        }
        Enum::Int64 => {
            let size: u32 = u32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
            if size != 8 {
                return Err(anyhow!("Incorrect size of serialized int 64"));
            }
            idx += 4;
            let val_int = i64::from_le_bytes(bytes[idx..idx + 8].try_into()?);
            idx += 8;
            Ok((Val::Int64Val(val_int), idx))
        }
        Enum::String => {
            let size: u32 = u32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
            idx += 4;
            let val_str = String::from_utf8(bytes[idx..idx + size as usize].try_into()?)?;
            idx += size as usize;
            Ok((Val::StringVal(val_str), idx))
        }
        Enum::BytesList => {
            let size: u32 = u32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
            idx += 4;
            let val_bytes = bytes[idx..idx + size as usize].to_vec();
            idx += size as usize;
            Ok((Val::BytesVal(val_bytes), idx))
        }
        other => Err(anyhow!(
            "Unsupported serialized type {}",
            other.as_str_name()
        )),
    }
}

pub fn serialize_key(
    entity_key: &EntityKey,
    serializer_version: EntityKeySerializationVersion,
) -> Result<Vec<u8>> {
    if serializer_version != EntityKeySerializationVersion::V3 {
        return Err(anyhow!("Unsupported version of key serializer"));
    }
    let key_map: HashMap<&str, &Value> = entity_key
        .join_keys
        .iter()
        .map(|s| s.as_str())
        .zip(entity_key.entity_values.iter())
        .collect();
    let mut sorted_keys: Vec<&str> = key_map.keys().cloned().collect();
    sorted_keys.sort();
    let mut bytes: Vec<u8> = Vec::with_capacity(30);
    bytes.extend((sorted_keys.len() as u32).to_le_bytes());
    for key in &sorted_keys {
        bytes.extend((Enum::String as u32).to_le_bytes());
        bytes.extend((key.len() as u32).to_le_bytes());
        bytes.extend(key.bytes());
    }
    for key in &sorted_keys {
        let value = key_map.get(key).ok_or(anyhow!("Key not found in map"))?;
        let value_bytes = serialize_value(value)?;
        bytes.extend(value_bytes);
    }
    Ok(bytes)
}

pub fn deserialize_key(
    bytes: Vec<u8>,
    serializer_version: EntityKeySerializationVersion,
) -> Result<EntityKey> {
    if serializer_version != EntityKeySerializationVersion::V3 {
        return Err(anyhow!("Unsupported version of key serializer"));
    }
    let key_len: u32 = u32::from_le_bytes(bytes[0..4].try_into()?);
    let mut join_keys: Vec<String> = Vec::with_capacity(key_len as usize);
    let mut entity_values: Vec<Value> = Vec::with_capacity(key_len as usize);
    let mut idx: usize = 4;
    for i in 0..key_len {
        let string_type = u32::from_le_bytes(bytes[idx..idx + 4].try_into()?);
        if string_type != Enum::String as u32 {
            return Err(anyhow!("Incorrect format of key: incorrect key type"));
        }
        idx += 4;
        let key_len = u32::from_le_bytes(
            bytes[idx..idx + 4]
                .try_into()
                .with_context(|| format!("Cannot deserialize key len for key number {}", i))?,
        );
        idx += 4;
        let key_name = String::from_utf8(bytes[idx..idx + key_len as usize].try_into()?)
            .with_context(|| format!("Cannot deserialize key name for key number {}", i))?
            .to_string();
        join_keys.push(key_name);
        idx += key_len as usize;
    }
    for i in 0..key_len {
        let (val, new_val_start) = deserialize_val(&bytes, idx)?;
        idx = new_val_start;
        entity_values.push(Value { val: Some(val) });
    }
    Ok(EntityKey {
        join_keys,
        entity_values,
    })
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
    fn test_serialize_key() -> Result<()> {
        let entity_key = EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1005)),
            }],
        };
        let serialized = serialize_key(&entity_key, EntityKeySerializationVersion::V3)?;
        let serialized_str = std_hex(&serialized);
        let expected =
            "0x0100000002000000090000006472697665725F69640400000008000000ED03000000000000";
        assert_eq!(serialized_str, expected);
        Ok(())
    }

    #[test]
    fn test_deserialize_key() -> Result<()> {
        let bytes: Vec<u8> = Vec::new();
        let entity_key = EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(1005)),
            }],
        };
        let serialized = serialize_key(&entity_key, EntityKeySerializationVersion::V3)?;

        let deserialized_key = deserialize_key(serialized, EntityKeySerializationVersion::V3)?;
        assert_eq!(entity_key, deserialized_key);
        Ok(())
    }
}
