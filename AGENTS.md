# Repository Guidelines

## Project Structure & Module Organization
- `feast-server-core/` contains the domain logic shared across services (feature registry, online stores, protobuf helpers). Unit tests live alongside modules via `#[cfg(test)]` blocks.
- `rest-server/` exposes HTTP endpoints for online features; `rest-server/src/server.rs` hosts the Axum router.
- `cli/` wraps the server core with command-line options; `cli/src/main.rs` wires env configuration and service startup.
- `dev/` holds sample feature repositories used for local experimentation, while `Dockerfile` and `Makefile` support container builds.

## Build, Test, and Development Commands
- `cargo fmt --all` formats every crate; run before commits.
- `cargo clippy --all-targets --all-features -- -D warnings` enforces lint cleanliness.
- `cargo test --all` executes the unit tests embedded in each crate.
- `cargo run -p cli -- serve -n 0.0.0.0` starts the HTTP server with default settings for iterative testing.

## Coding Style & Naming Conventions
- Follow standard Rust style: four-space indentation, `snake_case` for functions and modules, `CamelCase` for types.
- Keep business logic in `feast-server-core` and leave transport-specific code in crate wrappers.
- Use module-level documentation and sparing inline comments to clarify non-obvious decisions.

## Testing Guidelines
- Prefer fast unit tests colocated with implementation; mirror naming such as `mod tests` within each module.
- When adding new feature-store logic, cover both happy paths and error propagation (e.g., invalid entity keys).
- Run `cargo test --all` before submitting changes and ensure tests pass without `--ignored` flags.

## Commit & Pull Request Guidelines
- Recent history uses the `<summary>` pattern; keep titles short and imperative (e.g., `Add redis connector`).
- Each commit should stay focused and formatted by `cargo fmt`; avoid bundling unrelated updates.
- Pull requests should describe the problem, outline the solution, and note validation steps (`cargo test`, manual requests). Link tracking issues when available and include screenshots/log snippets for HTTP changes.

## Configuration Tips
- Override the feature repo path with `FEATURE_REPO_DIR` or the CLI `--chdir` flag; point `FEAST_FS_YAML_FILE_PATH` to custom configs.
- For TLS, provide matching `--key` and `--cert` paths; the server refuses half-configured pairs.
