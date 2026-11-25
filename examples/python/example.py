"""Sends a message to a kyu queue and receives it back, acknowledging it.

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    .venv/bin/python example.py

Env: KYU_ADDR (default 127.0.0.1:61613), KYU_LOGIN / KYU_PASSCODE.
"""

import os
import sys
import threading

import stomp

QUEUE = "/queue/kyu-example-python"


class Listener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn
        self.received = threading.Event()
        self.confirmed = threading.Event()
        self.failed = None

    def on_message(self, frame):
        print(f"received: {frame.body}")
        # STOMP 1.2: acknowledge with the message's `ack` header.
        self.conn.ack(frame.headers["ack"])
        self.received.set()

    def on_receipt(self, frame):
        print("broker confirmed the message")
        self.confirmed.set()

    def on_error(self, frame):
        self.failed = frame.headers.get("message", "unknown error")
        self.received.set()
        self.confirmed.set()


def main():
    host, port = os.environ.get("KYU_ADDR", "127.0.0.1:61613").rsplit(":", 1)
    # Connection12: in stomp.py, plain `Connection` speaks STOMP 1.1.
    conn = stomp.Connection12([(host, int(port))], heartbeats=(10000, 10000))
    listener = Listener(conn)
    conn.set_listener("", listener)

    conn.connect(os.environ.get("KYU_LOGIN"), os.environ.get("KYU_PASSCODE"), wait=True)
    print("connected")

    conn.subscribe(QUEUE, id="1", ack="client-individual")
    # Ask for a receipt so we know the broker has the message.
    conn.send(QUEUE, "hello from Python", content_type="text/plain", headers={"receipt": "sent-1"})

    # The message and the receipt can arrive in either order.
    ok = listener.received.wait(10) and listener.confirmed.wait(10)
    conn.disconnect()
    if listener.failed or not ok:
        print(f"failed: {listener.failed or 'timed out'}")
        return 1
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
