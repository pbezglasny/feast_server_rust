use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

#[path = "common.rs"]
mod common;

use common::{feature_store, sample_request};

fn bench_feature_store(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let store = runtime
        .block_on(feature_store())
        .expect("failed to create feature store");
    let request = sample_request();

    c.bench_function("feature_store_get_online_features", |b| {
        b.to_async(&runtime).iter(|| {
            let store = store.clone();
            let request = request.clone();
            async move {
                let response = store
                    .get_online_features(request)
                    .await
                    .expect("feature store call failed");
                criterion::black_box(response);
            }
        });
    });
}

criterion_group!(feature_store_benches, bench_feature_store);
criterion_main!(feature_store_benches);
