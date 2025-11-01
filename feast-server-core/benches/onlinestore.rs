use criterion::{Criterion, criterion_group, criterion_main};
use rustc_hash::FxHashMap as HashMap;
use tokio::runtime::Runtime;

#[path = "common.rs"]
mod common;

use common::online_store;
use feast_server_core::feast::types::value_type::Enum::Int64;
use feast_server_core::intern::rodeo_ref;
use feast_server_core::model::EntityIdValue::Int;
use feast_server_core::model::{RequestedEntityKey, Feature, JoinKeyValue};

fn build_entity_keys() -> Vec<RequestedEntityKey> {
    [1005_i64, 1002, 2003]
        .into_iter()
        .map(|driver_id| RequestedEntityKey {
            join_keys: vec![JoinKeyValue {
                join_key: rodeo_ref().get_or_intern("driver_id"),
                value: Int(driver_id),
                value_type: Int64,
            }],
        })
        .collect()
}

fn bench_onlinestore(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let store = runtime
        .block_on(online_store())
        .expect("failed to create sqlite online store");
    let entity_keys = build_entity_keys();
    let feature_names = vec!["conv_rate", "acc_rate"];

    let arg: HashMap<RequestedEntityKey, Vec<Feature>> = entity_keys
        .into_iter()
        .map(|key| {
            (
                key,
                feature_names
                    .iter()
                    .map(|feature| Feature::from_names("driver_hourly_stats", feature))
                    .collect(),
            )
        })
        .collect();

    c.bench_function("onlinestore_get_feature_values", |b| {
        b.to_async(&runtime).iter(|| {
            let arg = arg.clone();
            let store = store.clone();
            async move {
                let result = store
                    .get_feature_values(arg)
                    .await
                    .expect("online store fetch failed");
                criterion::black_box(result);
            }
        });
    });
}

criterion_group!(onlinestore_benches, bench_onlinestore);
criterion_main!(onlinestore_benches);
