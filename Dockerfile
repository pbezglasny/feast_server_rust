FROM rust:1.90-trixie AS builder

RUN mkdir -p /app/build

WORKDIR /app/build

RUN apt update \
	&& apt install protobuf-compiler -y \
	&& rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/build/target/release/feast /

ENTRYPOINT ["./feast"]
