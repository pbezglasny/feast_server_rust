use crate::feast::types::{EntityKey, Value};
use chrono::{DateTime, Duration, Utc};
use prost_types::Duration as ProstDuration;
use prost_types::Timestamp as ProstTimestamp;

pub fn prost_duration_to_duration(prost_duration: &ProstDuration) -> Duration {
    let seconds = prost_duration.seconds.max(0);
    let nanos = prost_duration.nanos.max(0) as i64;
    Duration::seconds(seconds) + Duration::nanoseconds(nanos)
}

pub fn prost_timestamp_to_datetime(prost_timestamp: &ProstTimestamp) -> DateTime<Utc> {
    let seconds = prost_timestamp.seconds.max(0);
    let nanos = prost_timestamp.nanos.max(0) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
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
