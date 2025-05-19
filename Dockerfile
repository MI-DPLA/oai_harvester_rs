FROM rust:bookworm AS builder

WORKDIR /usr/src/oai-harvester
COPY . .

RUN cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/oai_harvester /usr/local/bin/oai_harvester
CMD ["oai_harvester"]
