use crate::feast::types::{EntityKey, Value};
use prost_types::Duration as ProstDuration;
use prost_types::Timestamp as ProstTimestamp;
use std::time::Duration;
use std::time::SystemTime;

pub fn prost_duration_to_std(prost_duration: &ProstDuration) -> Duration {
    let seconds = prost_duration.seconds.max(0) as u64;
    let nanos = prost_duration.nanos.max(0) as u32;
    Duration::new(seconds, nanos)
}

pub fn prost_timestamp_to_system_time(prost_timestamp: &ProstTimestamp) -> SystemTime {
    let duration = Duration::new(
        prost_timestamp.seconds.max(0) as u64,
        prost_timestamp.nanos.max(0) as u32,
    );
    SystemTime::UNIX_EPOCH + duration
}

#[derive(Debug)]
pub struct EntityKeyWrapper(pub EntityKey);

impl PartialEq for EntityKeyWrapper {
    fn eq(&self, other: &Self) -> bool {
        let mut self_values: Vec<(&String, &Value)> = self
            .0
            .join_keys
            .iter()
            .zip(self.0.entity_values.iter())
            .collect();
        self_values.sort_by(|a, b| a.0.cmp(b.0));
        let mut other_values: Vec<(&String, &Value)> = other
            .0
            .join_keys
            .iter()
            .zip(other.0.entity_values.iter())
            .collect();
        other_values.sort_by(|a, b| a.0.cmp(b.0));
        self_values == other_values
    }
}
