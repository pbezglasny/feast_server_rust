FROM rust:1.90-trixie AS builder

RUN mkdir -p /app/build

WORKDIR /app/build

RUN apt update \
	&& apt install protobuf-compiler -y \
	&& rm -rf /var/lib/apt/lists/*

# Compile dependencies first to leverage Docker cache

# Copy manifests to leverage Docker cache for dependencies
COPY Cargo.toml Cargo.lock ./
COPY cli/Cargo.toml ./cli/
COPY feast-server-core/Cargo.toml ./feast-server-core/
COPY rest-server/Cargo.toml ./rest-server/
COPY grpc-server/Cargo.toml ./grpc-server/

RUN mkdir -p cli/src && echo "fn main() {}" > cli/src/main.rs && \
    mkdir -p feast-server-core/src && echo "pub fn lib() {}" > feast-server-core/src/lib.rs && \
    mkdir -p rest-server/src && echo "pub fn lib() {}" > rest-server/src/lib.rs && \
    mkdir -p grpc-server/src && echo "pub fn lib() {}" > grpc-server/src/lib.rs  && \
    echo "fn main() {}" > grpc-server/build.rs && \
    mkdir -p feast-server-core/benches && echo "fn main() {}" > feast-server-core/benches/feature_store.rs && \
    echo "fn main() {}" > feast-server-core/benches/onlinestore.rs && \
    echo "fn main() {}" > feast-server-core/benches/registry.rs



# Build dependencies only. This layer will be cached if Cargo files don't change.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

COPY . .

# Build the application, reusing the cached dependencies and source code.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/build/target \
    cargo build --release && \
    cp target/release/feast /app/build

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/build/feast /

ENTRYPOINT ["./feast"]
