FROM rust:1.90-trixie AS builder

RUN mkdir -p /app/build

WORKDIR /app/build

RUN apt update && apt install protobuf-compiler -y

COPY . .

RUN cargo build --release

FROM debian:trixie-slim

RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*

RUN mkdir /app

WORKDIR /app

COPY --from=builder /app/build/target/release/feast /app

ENTRYPOINT ["/app/feast"]
