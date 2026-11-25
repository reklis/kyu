# kyu

A [STOMP 1.2](https://stomp.github.io/stomp-specification-1.2.html) client and
server (message broker) in Rust, built on tokio.

- **Spec compliant**: frame format and escaping, `content-length` and binary
  bodies, heart-beating, version negotiation, receipts, transactions and all
  three ack modes. Built test-first; 179 tests.
- **Fast**: in the comparison below, kyu delivers 1.4–6.8× the messages per
  second of the fastest of RabbitMQ, ActiveMQ and Artemis in every scenario.
- **Bounded memory**: per-subscription prefetch, per-connection write limits,
  producer throttling when a queue is full, and bounded topic buffers.

## Use & Deployment

### Docker (GHCR)

kyu deploys as a single container: the `kyu-server` binary on a
[distroless](https://github.com/GoogleContainerTools/distroless) base image
(no shell or package manager, runs as a non-root user), about 45 MB.

Pre-built images are published to GitHub Container Registry on every push
(`.github/workflows/docker.yml`) after passing the container health check:

- `ghcr.io/reklis/kyu:0.1.2` / `ghcr.io/reklis/kyu:0.1` — release versions (recommended for production)
- `ghcr.io/reklis/kyu:latest` — default branch
- `ghcr.io/reklis/kyu:<branch>` / `ghcr.io/reklis/kyu:sha-<commit>` — branch and commit builds

```sh
# Run the latest release from GHCR
docker run -d --name kyu --restart unless-stopped \
  -p 61613:61613 \
  ghcr.io/reklis/kyu:0.1.2

# With authentication and custom heart-beats (ms)
docker run -d --name kyu --restart unless-stopped \
  -p 61613:61613 \
  -e KYU_LOGIN=app \
  -e KYU_PASSCODE=secret \
  -e KYU_HEARTBEAT=10000,10000 \
  ghcr.io/reklis/kyu:0.1.2

# Or build locally
docker build -t kyu .
```

#### Configuration

| Variable | Default in the image | Meaning |
|---|---|---|
| `KYU_ADDR` | `0.0.0.0:61613` | Listen address |
| `KYU_LOGIN`, `KYU_PASSCODE` | unset | Require these credentials on CONNECT (both must be set) |
| `KYU_HEARTBEAT` | `10000,10000` | Server heart-beat settings `sx,sy` in ms (used when clients ask for heart-beats) |

#### Docker Compose

```yaml
services:
  kyu:
    image: ghcr.io/reklis/kyu:0.1.2
    restart: unless-stopped
    ports:
      - "61613:61613"
    environment:
      KYU_LOGIN: app
      KYU_PASSCODE: secret
      KYU_HEARTBEAT: "10000,10000"
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
```

#### Kubernetes

kyu is a single-node, in-memory broker, so run a single replica (`replicas: 1`)
and use `kyu-server --check` for probes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kyu
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kyu
  template:
    metadata:
      labels:
        app: kyu
    spec:
      containers:
        - name: kyu
          image: ghcr.io/reklis/kyu:0.1.2
          ports:
            - containerPort: 61613
              name: stomp
          env:
            - name: KYU_HEARTBEAT
              value: "10000,10000"
          readinessProbe:
            exec:
              command: ["kyu-server", "--check"]
            initialDelaySeconds: 2
            periodSeconds: 10
          livenessProbe:
            exec:
              command: ["kyu-server", "--check"]
            initialDelaySeconds: 5
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: kyu
spec:
  selector:
    app: kyu
  ports:
    - name: stomp
      port: 61613
      targetPort: stomp
```

#### Operational notes

- **Health check**: the image's `HEALTHCHECK` runs `kyu-server --check`, which
  performs a real STOMP CONNECT (with `KYU_LOGIN`/`KYU_PASSCODE` if set), so
  "healthy" means the broker is answering, not just that the port is open.
- **Stopping**: `docker stop` / Kubernetes pod termination sends SIGTERM and
  kyu exits immediately with status 0.
- **State**: messages are held in memory and do not survive a restart, so
  there is nothing to mount as a volume.
- **File descriptors**: raise `nofile` (e.g. `--ulimit nofile=65536:65536`)
  when serving thousands of concurrent clients.
- **Throughput**: Docker's port mapping costs some throughput (1×1 queue
  about 200k/s through `-p` against 230k/s+ native); `--network host` avoids it.
- **Architecture**: the published GHCR image is currently built for `linux/amd64`.

### Server binary

```sh
cargo run --release --bin kyu-server -- 127.0.0.1:61613
kyu-server --check 127.0.0.1:61613   # exit 0 if a broker answers a STOMP CONNECT
```

The address defaults to `KYU_ADDR`, else `127.0.0.1:61613`; the other
variables are as in the table above. SIGTERM or Ctrl-C stops the server.

Or embed it:

```rust
use kyu::server::{Server, ServerConfig};

let server = Server::bind("127.0.0.1:61613", ServerConfig::default()).await?;
server.run().await; // accepts connections until dropped
```

Destinations starting with `/topic/` are publish/subscribe (every subscriber
gets a copy). Every other destination is a queue: each message goes to one
subscriber, round robin, and waits until someone subscribes. Unacknowledged
queue messages are redelivered when their subscription ends; NACKed messages
are discarded. Messages are held in memory only.

### Client

```rust
use kyu::client::{AckMode, Client, ConnectOptions, OutgoingMessage};

let client = Client::connect("127.0.0.1:61613", ConnectOptions::new("localhost")).await?;
let mut sub = client.subscribe("/queue/a", AckMode::ClientIndividual).await?;
client.send_confirmed(OutgoingMessage::new("/queue/a").body("hi")).await?;
if let Some(msg) = sub.recv().await {
    client.ack(&msg).await?;
}
client.disconnect().await?;
```

The client works with any STOMP 1.2 broker.

### Other languages

kyu works with standard STOMP 1.2 clients. [`examples/`](examples/) has
Node.js, Python and Ruby examples using each language's usual STOMP library;
`devbox run examples` runs all three against a fresh server.

## Performance

The same load generator (`examples/loadtest.rs`, using kyu's client) drives
each broker over STOMP on the same machine. Each broker is started with fresh
data just before its run and is the only broker running.

Deliveries per second, higher is better. The best result in each row is in
bold. "P×C" means producers × consumers on one queue.

| Scenario | kyu | RabbitMQ 4.3.1 | ActiveMQ 6.2.4 | Artemis 2.44.0 |
|---|---:|---:|---:|---:|
| Sequential round trips (p50 latency) | **65.2k (13µs)** | 26.7k (35µs) | 14.2k (56µs) | 13.6k (53µs) |
| Queue 1×1, 128 B | **230k** | 101k | 81k | 128k |
| Queue 4×4, 128 B | **359k** | 113k | 165k | 126k |
| Queue 64×64, 128 B | **574k** | 50k | 88k | 114k |
| Queue 1×1, 16 KiB | **179k** | 65k | 5.7k | 76k |
| Queue 1×1, 1 MiB | **4.5k** | 3.2k | 93 | 90 |
| 16 independent queues | **1.98M** | 133k | 145k | 302k |
| 1,000 queues, 32 producers | **2.47M** | 155k | 129k | 410k |
| Acknowledged queue 1×1 | **212k** | 39k | 61k | 141k |
| Acknowledged queue 4×4 | **249k** | 56k | 103k | 117k |
| Topic, 10 subscribers | **645k** | 205k | 258k | 409k |
| Topic, 100 subscribers | **2.23M** | 168k | 431k | 843k |
| Topic, 1,000 subscribers | **2.38M** | 145k | 292k | 1.01M |
| 10,000 clients: round trips | **173k** | 25.6k | timed out¹ | failed² |

CPU used by 4,000 idle clients heart-beating, measured at each broker's own
agreed interval (brokers may insist on a slower rate than clients ask for, so
kyu is measured at the same rate for each comparison):

| Interval | kyu | Other broker |
|---|---:|---:|
| 1000 ms | **6.0%** of a core | RabbitMQ 44.7% |
| 500 ms | **11.7%** | Artemis 31.7% |
| 100 ms | **67.3%** | ActiveMQ 98.0%, with 1,375 of the 4,000 connections dropped |

¹ ActiveMQ did not finish the 5,000-client step within 5 minutes.
² Artemis closed connections ("did not receive data within the 60000ms
connection TTL") during the 10,000-client step.

### Reading these numbers

- **Like for like, not feature for feature.** RabbitMQ, ActiveMQ and Artemis
  offer persistence, clustering and management that kyu does not; part of the
  gap is the cost of those features. kyu is a single node that keeps messages
  in memory.
- **Near-default settings.** Each broker was changed only where a default
  would break the test: STOMP enabled on its own port, Artemis's `/queue/`
  and `/topic/` prefixes mapped, ActiveMQ's 1,000-connection cap raised,
  RabbitMQ's listen backlog raised to 4096, and a 2 GB heap for both Java
  brokers. No message is marked persistent. Tuning, especially of the Java
  brokers' large-message handling, would change some rows.
- **Memory is not compared.** Under a slow consumer, RabbitMQ and ActiveMQ
  move non-persistent messages to disk, so their memory use is not comparable
  with kyu's in-memory queues (about 1.35 KB per queued 1 KiB message, with
  producers throttled at 100,000 queued messages).
- **One machine, one run each.** AMD Ryzen AI 9 HX 370 (24 threads), 61 GiB,
  Linux 6.18, Rust 1.96. The load generator shares the machine with the
  broker, and another process used one core throughout. Differences under
  about 20% are within run-to-run noise.

### Reproducing

Tools come from [devbox](https://www.jetify.com/devbox):

```sh
scripts/brokers.sh setup   # configure RabbitMQ and ActiveMQ, download Artemis
scripts/compare.sh         # all brokers, all scenarios; results in .brokers/results/
scripts/compare.sh kyu -- queue fanout   # a subset

devbox run loadtest            # kyu only, server in-process
devbox run loadtest -- fanout  # one scenario
```

## Development

```sh
devbox run check      # rustfmt, clippy (pedantic, with and without mimalloc), actionlint, complexity gate, tests
devbox run bench      # codec micro-benchmarks (criterion)
devbox run examples   # Node.js, Python and Ruby clients against a fresh server
```

`scripts/complexity.py` fails the check if any function exceeds cyclomatic
complexity 20 or cognitive complexity 15, measured with
[rust-code-analysis](https://github.com/mozilla/rust-code-analysis).

The server and load test use [mimalloc](https://github.com/microsoft/mimalloc)
by default (the `mimalloc` feature); the library itself never sets a global
allocator.

## License

Copyright (C) 2026 the kyu contributors.

kyu is free software: you can redistribute it and/or modify it under the
terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. See [LICENSE](LICENSE) for the full text.
