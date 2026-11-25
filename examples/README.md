# Examples

kyu speaks standard STOMP 1.2, so any STOMP 1.2 client works with it. Each
example below uses its language's most widely used STOMP library to send a
message to a queue (asking the broker for a receipt), receive it with a
`client-individual` subscription, acknowledge it, and disconnect.

| Language | Library | Run |
|---|---|---|
| Node.js | [`@stomp/stompjs`](https://github.com/stomp-js/stompjs) + [`@stomp/tcp-wrapper`](https://github.com/stomp-js/tcp-wrapper) | `cd node && npm install && npm start` |
| Python | [`stomp.py`](https://github.com/jasonrbriggs/stomp.py) | `cd python && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt && .venv/bin/python example.py` |
| Ruby | [`stomp`](https://github.com/stompgem/stomp) | `cd ruby && bundle install && bundle exec ruby example.rb` |

All of them read `KYU_ADDR` (default `127.0.0.1:61613`) and, if the broker
requires credentials, `KYU_LOGIN` / `KYU_PASSCODE`. Start a broker first, for
example `docker run -p 61613:61613 ghcr.io/reklis/kyu`.

To run all three against a fresh kyu server (Node.js, Python and Ruby come
from devbox):

```sh
devbox run examples
```

`loadtest.rs`, also in this folder, is kyu's own Rust load generator
(`devbox run loadtest`).

## Library notes

Each library needs one setting to talk STOMP 1.2 over plain TCP:

- **`@stomp/stompjs`** speaks WebSocket by default; `@stomp/tcp-wrapper`
  supplies a TCP socket. It also needs an explicit `host` connect header,
  which STOMP 1.2 requires and stompjs normally takes from the WebSocket URL.
- **`stomp.py`**: use `stomp.Connection12`. Plain `stomp.Connection` speaks
  STOMP 1.1, which kyu rejects (it supports 1.2 only).
- **`stomp` gem** defaults to STOMP 1.0, so the example sends
  `accept-version: 1.2`. The gem was last released in 2020 and calls
  `IO#ready?`, which Ruby 4.0 removed; the example restores it in a few
  lines.
