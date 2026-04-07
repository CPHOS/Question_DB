ARG PG_MAJOR=16

FROM rust:1-bookworm AS builder

WORKDIR /app

COPY . .

RUN cargo build --locked --release

FROM postgres:${PG_MAJOR}-bookworm AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/qb_api /usr/local/bin/qb_api
COPY migrations /app/migrations
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && mkdir -p /var/lib/qb/exports

ENV QB_BIND_ADDR=0.0.0.0:8080
ENV QB_EXPORT_DIR=/var/lib/qb/exports

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["qb_api"]
