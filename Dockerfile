# syntax=docker/dockerfile:1.7
# felix-relay, every role in one image; RELAY_ROLES picks them. It also
# carries deploy/seed.sh and what it needs (curl, jq, openssl, busybox), so
# an install's `tokens` service runs from this image too.

FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY core core
COPY relay relay
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p felix-relay \
    && cp target/release/felix-relay /usr/local/bin/

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends busybox ca-certificates curl jq openssl tini \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/felix-relay /usr/local/bin/
COPY deploy/seed.sh /usr/local/bin/relay-seed
# Intake must be reachable from outside the container. Admin requests are
# signed in either way.
ENV RELAY_LISTEN=0.0.0.0:8090
USER 65532:65532
EXPOSE 8090/tcp
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=6 \
    CMD curl -fsS http://127.0.0.1:8090/healthz >/dev/null || exit 1
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/felix-relay"]
