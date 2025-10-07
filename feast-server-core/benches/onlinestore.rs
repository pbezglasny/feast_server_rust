use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

#[path = "common.rs"]
mod common;

use common::online_store;
use feast_server_core::feast::types::value::Val;
use feast_server_core::feast::types::{EntityKey, Value};

fn build_entity_keys() -> Vec<EntityKey> {
    [1005_i64, 1002, 2003]
        .into_iter()
        .map(|driver_id| EntityKey {
            join_keys: vec!["driver_id".to_string()],
            entity_values: vec![Value {
                val: Some(Val::Int64Val(driver_id)),
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
    let feature_names = vec!["conv_rate".to_string(), "acc_rate".to_string()];

    c.bench_function("onlinestore_get_feature_values", |b| {
        b.to_async(&runtime).iter(|| {
            let store = store.clone();
            let entity_keys = entity_keys.clone();
            let feature_names = feature_names.clone();
            async move {
                let feature_refs: Vec<&str> =
                    feature_names.iter().map(|s| s.as_str()).collect();
                let result = store
                    .get_feature_values("driver_hourly_stats", &entity_keys, &feature_refs)
                    .await
                    .expect("online store fetch failed");
                criterion::black_box(result);
            }
        });
    });
}

criterion_group!(onlinestore_benches, bench_onlinestore);
criterion_main!(onlinestore_benches);
