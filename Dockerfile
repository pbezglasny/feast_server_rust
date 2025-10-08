FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

RUN mkdir /app

WORKDIR /app

COPY target/release/feast /app

ENTRYPOINT ["/app/feast"]