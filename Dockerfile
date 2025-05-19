FROM rust:bookworm

WORKDIR /usr/src/oai-harvester
COPY . .

RUN cargo install --path .

CMD ["oai_harvester"]
