# Sends a message to a kyu queue and receives it back, acknowledging it.
#
#   bundle install && bundle exec ruby example.rb
#
# Env: KYU_ADDR (default 127.0.0.1:61613), KYU_LOGIN / KYU_PASSCODE.

require "io/wait"
require "stomp"

# The stomp gem (1.4.10, last released 2020) calls IO#ready?, which Ruby 4.0
# removed. Restore it using its replacement: readable without blocking.
unless IO.method_defined?(:ready?)
  class IO
    def ready? = wait_readable(0)
  end
end

QUEUE = "/queue/kyu-example-ruby"

host, port = ENV.fetch("KYU_ADDR", "127.0.0.1:61613").split(":")
client = Stomp::Client.new(
  hosts: [{ host: host, port: port.to_i, login: ENV["KYU_LOGIN"].to_s, passcode: ENV["KYU_PASSCODE"].to_s }],
  # Ask for STOMP 1.2 (the gem defaults to 1.0); kyu speaks 1.2 only.
  connect_headers: { "accept-version" => "1.2", "host" => "localhost", "heart-beat" => "10000,10000" },
  reliable: false # an example should fail, not reconnect forever
)
puts "connected to #{client.connection_frame.headers['server']}"

events = Thread::Queue.new
client.subscribe(QUEUE, ack: "client-individual", id: "1") do |message|
  puts "received: #{message.body}"
  client.ack(message) # uses the message's STOMP 1.2 `ack` header
  events << :received
end

# A block asks for a receipt, so we know the broker has the message.
client.publish(QUEUE, "hello from Ruby", "content-type" => "text/plain") do |_receipt|
  puts "broker confirmed the message"
  events << :confirmed
end

# The message and the receipt can arrive in either order.
seen = []
2.times { seen << events.pop(timeout: 10) }
client.close
if seen.compact.sort == %i[confirmed received]
  puts "done"
else
  puts "failed: timed out"
  exit 1
end
