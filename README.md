# Feast Feature Server (Rust)

Rust implementation of the Feast feature server.

## Workspace Layout
- `feast-server-core`: core feature-store domain logic (registry access, online store abstraction, protobuf helpers).
- `rest-server`: Axum-based HTTP server that exposes online feature retrieval endpoints.
- `grpc-server`: tonic-based gRPC server scaffolding.
- `cli`: command-line entrypoint that wires configuration, logging, and server startup.

## Prerequisites
- Rust toolchain (stable) with `cargo` and `rustfmt`.

## Run the HTTP Server
1. Point the CLI at a feature repository. Either change into the repo directory or pass it explicitly:
   ```bash
   FEATURE_REPO_DIR_ENV_VAR=dev/careful_tomcat/feature_repo \
   cargo run -p cli -- serve -n 0.0.0.0 -p 6566
   ```
   You can also use `--chdir <path>` or `--feature-store-yaml <file>` to override the repository root and `feature_store.yaml` filename.
2. Optional flags:
   - `--metrics` enables a `/metrics` endpoint backed by `axum-prometheus`.
   - `--key` and `--cert` must be provided together to serve over TLS.
   - `--type grpc` is accepted by the CLI, but the gRPC server is not implemented yet.

When the server starts it exposes:
- `POST /get-online-features` expecting a Feast `GetOnlineFeatureRequest` payload and returning the online feature vector.
- `GET /health` for readiness checks (HTTP 200 on success).
- `GET /metrics` when metrics are enabled.

## Development Workflow
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets --all-features -- -D warnings`
- Test: `cargo test --all`
