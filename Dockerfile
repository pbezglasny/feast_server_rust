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

# Create dummy source files to build dependencies without the full source code.
RUN mkdir -p cli/src && echo "fn main() {}" > cli/src/main.rs
RUN mkdir -p feast-server-core/src && echo "pub fn lib() {}" > feast-server-core/src/lib.rs
RUN mkdir -p rest-server/src && echo "pub fn lib() {}" > rest-server/src/lib.rs
RUN mkdir -p grpc-server/src && echo "pub fn lib() {}" > grpc-server/src/lib.rs

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/build/target \
    cargo build --release && \
    cp target/release/feast /app/build

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/build/feast /

ENTRYPOINT ["./feast"]
