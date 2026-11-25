# Changelog

All notable changes to kyu are recorded here. Versions follow
[Semantic Versioning](https://semver.org/); before 1.0, minor versions may
change the public API.

## 0.1.2 (2026-11-25)

### Performance

- **Codec**: cache parsed headers across partial reads, track the NULL scan
  offset when `content-length` is absent, and reserve exact frame capacity once
  `content-length` is known — eliminating repeated buffer reallocations and
  header re-scans on large/chunked frames (1 MiB throughput 4.3× higher,
  chunked 1 MiB decode 5.2× faster). Replaced `core::fmt` integer formatting
  with a stack-buffered decimal writer.
- **Server**: batch non-transactional `ACK`/`NACK` frames under a single shard
  lock with deferred refill; add a direct-delivery fast path in `enqueue` and
  eliminate redundant hash map lookups and `Arc` clones across queue dispatch
  and topic fanout; switch shard hashing from `SipHash` to `foldhash` and store
  subscription ids as `Str`.
- **Client**: lock-free fast path in `Client::send_frame`, burst reading in
  `IoTask`, `foldhash` for internal maps, and static/zero-copy headers.
- **Transport**: skip read-timestamp `clock_gettime` calls when incoming
  heart-beating is disabled, and reset the outgoing heart-beat timer immediately
  on send to avoid a redundant poll cycle.

## 0.1.1 (2026-11-25)

### Added

- Client examples in Node.js (`@stomp/stompjs` with `@stomp/tcp-wrapper`),
  Python (`stomp.py`) and Ruby (`stomp` gem), each sending, receiving and
  acknowledging a message against kyu. `devbox run examples` runs all three
  against a fresh server. `examples/README.md` notes the setting each library
  needs for STOMP 1.2 over TCP.

No changes to the library, server or Docker image behaviour.

## 0.1.0 (2026-11-25)

First release: a STOMP 1.2 client and server (message broker) on tokio.

### Protocol

- STOMP 1.2 frames: LF or CRLF line endings, header escaping (not for
  CONNECT/CONNECTED), `content-length` with binary bodies, first repeated
  header wins, configurable frame/header limits.
- Version negotiation, heart-beating in both directions, receipts,
  transactions (BEGIN/COMMIT/ABORT), and the `auto`, `client` and
  `client-individual` ack modes. Protocol errors send an ERROR frame (with
  `receipt-id` when the frame asked for a receipt) and close the connection.

### Server

- `/topic/` destinations are publish/subscribe; all others are queues
  (round-robin to subscribers, held until someone subscribes). Unacknowledged
  queue messages are redelivered when their subscription ends.
- Flow control: per-subscription prefetch, per-connection write limit,
  producer throttling when a queue holds 100,000 messages, bounded buffers
  for slow topic subscribers.
- Broker state sharded by destination; messages encoded once and shared by
  every delivery; SENDs from one socket read applied under one lock.
- `kyu-server` binary: configured with `KYU_ADDR`, `KYU_LOGIN` /
  `KYU_PASSCODE` and `KYU_HEARTBEAT`; exits cleanly on SIGTERM;
  `--check` health check.

### Client

- `Client` with `send`, `send_confirmed`, `subscribe`, `ack`/`nack`,
  transactions and graceful `disconnect`; bounded subscription buffers that
  pause reading (but not heart-beats) when the application falls behind.

### Deployment

- Docker image `ghcr.io/reklis/kyu` (distroless, non-root, about 45 MB) with a
  STOMP health check, built and published on every push.

### Performance

- Against RabbitMQ 4.3.1, ActiveMQ 6.2.4 and Artemis 2.44.0 on the same
  machine, kyu delivered 1.5–14× the messages per second of the fastest of the
  three in every scenario but 1 MiB messages, where RabbitMQ was slightly
  ahead. See the README for the full table and how to reproduce it.

### Known limitations

- Messages are held in memory only and do not survive a restart.
- The Docker image is built for x86_64 only.
- A connection that both publishes to a full queue and must acknowledge on
  that same connection to drain it can stall itself while throttled.
