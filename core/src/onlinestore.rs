use std::time::{Duration, SystemTime};
use crate::feast::types::EntityKey;

pub struct OnlineStoreRow {
    pub entity_key: Vec<u8>,
    pub feature_name: String,
    pub value: Vec<u8>,
    pub event_ts: SystemTime,
    pub created_ts: SystemTime,
}
pub trait OnlineStore {
    fn get_feature_values(&self, keys: &Vec<EntityKey>, ttl: Option<Duration>) -> Vec<OnlineStoreRow>;
}
