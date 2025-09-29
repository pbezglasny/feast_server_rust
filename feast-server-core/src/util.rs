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
