use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

#[path = "common.rs"]
mod common;

use common::{registry_service, sample_request};
use feast_server_core::model::RequestedFeatures;

fn bench_registry(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let registry = registry_service();
    let request = sample_request();

    c.bench_function("registry_request_to_view_keys", |b| {
        b.to_async(&runtime).iter(|| {
            let registry = registry.clone();
            let request = request.clone();
            async move {
                let requested_features = RequestedFeatures::from(&request);
                let result = registry
                    .request_to_view_keys(requested_features)
                    .await
                    .expect("registry lookup failed");
                criterion::black_box(result);
            }
        });
    });
}

criterion_group!(registry_benches, bench_registry);
criterion_main!(registry_benches);
