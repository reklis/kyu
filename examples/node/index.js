// Sends a message to a kyu queue and receives it back, acknowledging it.
//
//   npm install && npm start
//
// Env: KYU_ADDR (default 127.0.0.1:61613), KYU_LOGIN / KYU_PASSCODE.

import { Client } from '@stomp/stompjs';
import { TCPWrapper } from '@stomp/tcp-wrapper';

const [host, port] = (process.env.KYU_ADDR ?? '127.0.0.1:61613').split(':');
const queue = '/queue/kyu-example-node';

const client = new Client({
  // @stomp/stompjs speaks WebSocket by default; this wrapper gives it TCP.
  webSocketFactory: () => new TCPWrapper(host, Number(port)),
  connectHeaders: {
    host: 'localhost', // required by STOMP 1.2 (normally taken from a WebSocket URL)
    ...(process.env.KYU_LOGIN && {
      login: process.env.KYU_LOGIN,
      passcode: process.env.KYU_PASSCODE ?? '',
    }),
  },
  heartbeatIncoming: 10000,
  heartbeatOutgoing: 10000,
  reconnectDelay: 0, // an example should fail, not retry forever
});

const done = (code, message) => {
  console.log(message);
  client.deactivate().finally(() => process.exit(code));
};
setTimeout(() => done(1, 'timed out'), 10000).unref();

// Finish once the message has come back and the broker has confirmed it
// (the two can arrive in either order).
let pending = 2;
const step = () => --pending === 0 && done(0, 'done');

client.onConnect = (frame) => {
  console.log(`connected to ${frame.headers.server}`);

  client.subscribe(
    queue,
    (message) => {
      console.log(`received: ${message.body}`);
      message.ack();
      step();
    },
    { ack: 'client-individual' },
  );

  // Ask for a receipt so we know the broker has the message.
  const receipt = `sent-${Date.now()}`;
  client.watchForReceipt(receipt, () => {
    console.log('broker confirmed the message');
    step();
  });
  client.publish({
    destination: queue,
    body: 'hello from Node.js',
    headers: { 'content-type': 'text/plain', receipt },
  });
};

client.onStompError = (frame) => done(1, `broker error: ${frame.headers.message}`);
client.onWebSocketError = (error) => done(1, `connection error: ${error.message ?? error}`);

client.activate();
