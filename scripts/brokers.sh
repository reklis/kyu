#!/usr/bin/env bash
# Local brokers for scripts/compare.sh, kept under .brokers/ (git-ignored).
#
#   scripts/brokers.sh setup                 # once: configure every broker
#   scripts/brokers.sh start <broker>        # fresh data, waits for the port
#   scripts/brokers.sh stop  <broker>
#
# Brokers and STOMP ports: kyu 61613, rabbitmq 61614, activemq 61615,
# artemis 61617. RabbitMQ and ActiveMQ come from devbox; Artemis is not in
# nixpkgs, so its release is downloaded from Apache and checksum-verified.
# All use near-default settings, changed only where needed for a like-for-like
# test (see README.md).
set -euo pipefail
cd "$(dirname "$0")/.."
B=$PWD/.brokers

# Some devbox installs are a script with a #!/bin/bash shebang, which cannot
# be executed directly where /bin/bash does not exist (e.g. NixOS).
devbox() {
    local bin
    bin=$(type -P devbox)
    if head -c 2 "$bin" | grep -q '#!'; then bash "$bin" "$@"; else "$bin" "$@"; fi
}
ARTEMIS_VERSION=2.44.0
declare -A PORT=([kyu]=61613 [rabbitmq]=61614 [activemq]=61615 [artemis]=61617)

rabbit_env() {
    export RABBITMQ_NODENAME=kyubench@localhost
    export RABBITMQ_CONFIG_FILE=$B/rabbitmq/rabbitmq.conf
    export RABBITMQ_ENABLED_PLUGINS_FILE=$B/rabbitmq/enabled_plugins
    export RABBITMQ_MNESIA_BASE=$B/rabbitmq/mnesia
    export RABBITMQ_LOG_BASE=$B/rabbitmq/log
    export RABBITMQ_DIST_PORT=25673
}

activemq_env() {
    export ACTIVEMQ_CONF=$B/activemq/conf ACTIVEMQ_DATA=$B/activemq/data
    export ACTIVEMQ_TMP=$B/activemq/data/tmp ACTIVEMQ_OPTS_MEMORY="-Xms512M -Xmx2G"
}

setup_rabbitmq() {
    mkdir -p "$B/rabbitmq"
    cat > "$B/rabbitmq/rabbitmq.conf" <<'EOF'
listeners.tcp.default = 5673
loopback_users = none
stomp.listeners.tcp.1 = 127.0.0.1:61614
stomp.tcp_listen_options.backlog = 4096
stomp.default_user = guest
stomp.default_pass = guest
stomp.default_vhost = /
log.console = false
log.file.level = warning
EOF
    echo '[rabbitmq_stomp].' > "$B/rabbitmq/enabled_plugins"
}

setup_activemq() {
    local home
    home=$(dirname "$(dirname "$(devbox run -- sh -c 'readlink -f "$(which activemq)"' | tail -1)")")
    rm -rf "$B/activemq/conf"
    mkdir -p "$B/activemq"
    cp -r "$home/conf" "$B/activemq/"
    chmod -R u+w "$B/activemq/conf"
    # STOMP only, over NIO, with the default 1000-connection cap raised; no
    # web console.
    python3 - "$B/activemq/conf/activemq.xml" <<'EOF'
import sys
p = sys.argv[1]; s = open(p).read()
a = s.index('<transportConnectors>'); b = s.index('</transportConnectors>')
s = s[:a] + '''<transportConnectors>
            <transportConnector name="stomp" uri="stomp+nio://127.0.0.1:61615?maximumConnections=20000&amp;wireFormat.maxFrameSize=104857600"/>
        ''' + s[b:]
open(p, 'w').write(s.replace('<import resource="jetty.xml"/>', ''))
EOF
}

setup_artemis() {
    local tarball=apache-artemis-$ARTEMIS_VERSION-bin.tar.gz
    local url=https://archive.apache.org/dist/activemq/activemq-artemis/$ARTEMIS_VERSION/$tarball
    mkdir -p "$B"
    if [ ! -d "$B/apache-artemis-$ARTEMIS_VERSION" ]; then
        (cd "$B" && curl -sSfLO "$url" && curl -sSfLO "$url.sha512" &&
            echo "$(cut -d' ' -f1 "$tarball.sha512")  $tarball" | sha512sum -c - &&
            tar xzf "$tarball")
    fi
    rm -rf "$B/artemis-instance"
    "$B/apache-artemis-$ARTEMIS_VERSION/bin/artemis" create "$B/artemis-instance" \
        --user guest --password guest --allow-anonymous --silent --no-web --host 127.0.0.1 >/dev/null
    # STOMP only; /queue/ is anycast (queue) and /topic/ multicast (topic).
    python3 - "$B/artemis-instance/etc/broker.xml" <<'EOF'
import sys
p = sys.argv[1]; s = open(p).read()
a = s.index('<acceptors>'); b = s.index('</acceptors>')
s = s[:a] + '''<acceptors>
         <acceptor name="stomp">tcp://127.0.0.1:61617?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=STOMP;useEpoll=true;anycastPrefix=/queue/;multicastPrefix=/topic/</acceptor>
      ''' + s[b:]
open(p, 'w').write(s)
EOF
}

wait_port() {
    for _ in $(seq 1 90); do
        ss -ltnH "sport = :$1" | grep -q . && return 0
        sleep 1
    done
    echo "port $1 did not open" >&2
    return 1
}

pid_on() { ss -ltnpH "sport = :$1" | grep -oP 'pid=\K[0-9]+' | head -1; }

start() {
    local b=$1
    mkdir -p "$B/$b"
    case $b in
        kyu)
            cargo build --release -q --bin kyu-server
            KYU_HEARTBEAT=100,100 nohup target/release/kyu-server 127.0.0.1:61613 \
                > "$B/kyu/stdout.log" 2>&1 &
            ;;
        rabbitmq)
            rabbit_env
            rm -rf "$B/rabbitmq/mnesia" "$B/rabbitmq/log"
            nohup bash -c "$(declare -f devbox); devbox run -- rabbitmq-server" > "$B/rabbitmq/stdout.log" 2>&1 &
            ;;
        activemq)
            activemq_env
            rm -rf "$B/activemq/data"
            nohup bash -c "$(declare -f devbox); devbox run -- activemq console" > "$B/activemq/stdout.log" 2>&1 &
            ;;
        artemis)
            rm -rf "$B/artemis-instance/data"
            nohup "$B/artemis-instance/bin/artemis" run > "$B/artemis-instance/stdout.log" 2>&1 &
            ;;
        *) echo "unknown broker $b" >&2; exit 2 ;;
    esac
    wait_port "${PORT[$b]}"
    sleep 2 # let the JVM / Erlang node finish starting
    pid_on "${PORT[$b]}"
}

stop() {
    local b=$1 pid
    if [ "$b" = rabbitmq ]; then
        rabbit_env
        devbox run -- rabbitmqctl stop > /dev/null 2>&1 || true
    fi
    pid=$(pid_on "${PORT[$b]}" || true)
    if [ -n "$pid" ]; then
        kill "$pid"
        for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -9 "$pid" 2>/dev/null || true
    fi
}

case ${1:-} in
    setup) setup_rabbitmq; setup_activemq; setup_artemis ;;
    start) start "$2" ;;
    stop) stop "$2" ;;
    *) sed -n '2,12p' "$0"; exit 2 ;;
esac
