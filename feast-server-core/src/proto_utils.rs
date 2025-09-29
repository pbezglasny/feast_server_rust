use crate::feast::types::value::Val;
use crate::model::EntityId;
use anyhow::{Error, Result, anyhow};
use std::any::Any;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq)]
pub struct ValWrapper(pub Val);

impl Eq for ValWrapper {}

impl Hash for ValWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Val::BoolVal(v) => v.hash(state),
            Val::BytesVal(v) => v.hash(state),
            Val::Int32Val(v) => v.hash(state),
            Val::Int64Val(v) => v.hash(state),
            Val::BytesListVal(v) => v.val.hash(state),
            Val::DoubleVal(v) => state.write_u64(v.to_bits()),
            Val::FloatVal(v) => state.write_u32(v.to_bits()),
            Val::StringVal(v) => v.hash(state),
            Val::StringListVal(v) => v.val.hash(state),
            Val::Int32ListVal(v) => v.val.hash(state),
            Val::Int64ListVal(v) => v.val.hash(state),
            Val::UnixTimestampVal(v) => v.hash(state),
            Val::UnixTimestampListVal(v) => {
                for t in &v.val {
                    t.hash(state);
                }
            }
            Val::DoubleListVal(v) => {
                // f64 does not implement Hash, so we convert to bits
                for f in &v.val {
                    state.write_u64(f.to_bits());
                }
            }
            Val::FloatListVal(v) => {
                // f32 does not implement Hash, so we convert to bits
                for f in &v.val {
                    state.write_u32(f.to_bits());
                }
            }
            Val::BoolListVal(v) => v.val.hash(state),
            Val::NullVal(v) => v.hash(state),
        }
    }
}

impl TryFrom<Val> for EntityId {
    type Error = Error;

    fn try_from(value: Val) -> Result<Self> {
        match value {
            Val::Int32Val(v) => Ok(EntityId::Int(v as i64)),
            Val::Int64Val(v) => Ok(EntityId::Int(v)),
            Val::StringVal(v) => Ok(EntityId::String(v)),
            other => Err(anyhow!("Unsupported type convertion")),
        }
    }
}
