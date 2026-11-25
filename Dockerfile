# syntax=docker/dockerfile:1
#
# kyu STOMP broker as a single container.
#
#   docker build -t kyu .
#   docker run -p 61613:61613 kyu
#
# Configuration (environment):
#   KYU_ADDR       listen address            (default 0.0.0.0:61613)
#   KYU_LOGIN      require this login ...
#   KYU_PASSCODE   ... and this passcode
#   KYU_HEARTBEAT  sx,sy heart-beat settings in ms (default 10000,10000)
#
# Messages are held in memory: they do not survive a container restart.

# Builder and runtime are both Debian 13 (trixie), so glibc matches.
FROM rust:1.96-slim-trixie AS build
WORKDIR /src
# Only what the server build needs (benches/ is listed in Cargo.toml).
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin kyu-server \
    && cp target/release/kyu-server /kyu-server

# Distroless: no shell or package manager, runs as an unprivileged user.
FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /kyu-server /usr/local/bin/kyu-server
ENV KYU_ADDR=0.0.0.0:61613 \
    KYU_HEARTBEAT=10000,10000
EXPOSE 61613
USER nonroot
# A real STOMP CONNECT to KYU_ADDR (connecting to 0.0.0.0 reaches the local
# host), using KYU_LOGIN/KYU_PASSCODE if set.
HEALTHCHECK --interval=10s --timeout=6s --start-period=2s --retries=3 \
    CMD ["kyu-server", "--check"]
ENTRYPOINT ["kyu-server"]
