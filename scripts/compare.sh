#!/usr/bin/env bash
# Runs the load test against each STOMP broker in turn. Each broker is started
# with fresh data just before its run and stopped afterwards, so only one runs
# at a time and none carries state from another test. Output goes to
# .brokers/results/<broker>.txt.
#
#   scripts/brokers.sh setup                      # once
#   scripts/compare.sh [broker...] [-- scenario...]
#
# Set NO_RESTART=1 to test brokers that are already running instead.
set -u
cd "$(dirname "$0")/.."

declare -A PORT=([kyu]=61613 [rabbitmq]=61614 [activemq]=61615 [artemis]=61617)
declare -A EXTRA=([rabbitmq]="--host /" [artemis]="--login guest --passcode guest")
ORDER=(kyu rabbitmq activemq artemis)
SCENARIOS=(rtt queue parallel destinations ack fanout idle connections slowconsumer)
PER_SCENARIO_TIMEOUT=${PER_SCENARIO_TIMEOUT:-300}

brokers=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do brokers+=("$1"); shift; done
[ "${1:-}" = "--" ] && { shift; SCENARIOS=("$@"); }
[ ${#brokers[@]} -eq 0 ] && brokers=("${ORDER[@]}")

cargo build --release --example loadtest -q || exit 1
mkdir -p .brokers/results

for b in "${brokers[@]}"; do
    port=${PORT[$b]}
    if [ -z "${NO_RESTART:-}" ]; then
        scripts/brokers.sh stop "$b"
        scripts/brokers.sh start "$b" > /dev/null || { echo "skip $b: failed to start"; continue; }
    fi
    pid=$(ss -ltnpH "sport = :$port" | grep -oP 'pid=\K[0-9]+' | head -1)
    [ -z "$pid" ] && { echo "skip $b: nothing listening on $port"; continue; }
    out=.brokers/results/$b.txt
    echo "== $b (port $port, pid $pid)" | tee "$out"
    for s in "${SCENARIOS[@]}"; do
        # shellcheck disable=SC2086
        timeout "$PER_SCENARIO_TIMEOUT" target/release/examples/loadtest \
            --addr "127.0.0.1:$port" --server-pid "$pid" ${EXTRA[$b]:-} "$s" 2>&1 \
            | grep -v '^kyu load test against' | tee -a "$out"
        rc=${PIPESTATUS[0]}
        if [ "$rc" -eq 124 ]; then
            echo "  [$s] TIMED OUT after ${PER_SCENARIO_TIMEOUT}s" | tee -a "$out"
        elif [ "$rc" -ne 0 ]; then
            echo "  [$s] FAILED (exit $rc)" | tee -a "$out"
        fi
    done
    [ -z "${NO_RESTART:-}" ] && scripts/brokers.sh stop "$b"
done
