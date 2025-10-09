FROM rust:1.90-trixie AS builder

RUN mkdir -p /app/build

WORKDIR /app/build

RUN apt update \
	&& apt install protobuf-compiler -y \
	&& rm -rf /var/lib/apt/lists/*

COPY . .

# Build the application, reusing the cached dependencies and source code.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/build/target \
    cargo build --release && \
    cp target/release/feast /app/build

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/build/feast /

ENTRYPOINT ["./feast"]
