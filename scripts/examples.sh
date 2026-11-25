#!/usr/bin/env bash
# Runs the Node.js, Python and Ruby examples against a fresh kyu server.
# Each example sends a message, receives and acknowledges it, and exits 0.
#
#   devbox run examples          # node, ruby and python come from devbox
#   scripts/examples.sh node     # just one
set -uo pipefail
cd "$(dirname "$0")/.."

cargo build --release -q --bin kyu-server || exit 1

# Start kyu on a free port; it prints "kyu listening on ADDR" first.
log=$(mktemp)
KYU_ADDR=127.0.0.1:0 target/release/kyu-server 2> "$log" &
server=$!
trap 'kill "$server" 2>/dev/null; rm -f "$log"' EXIT
for _ in $(seq 1 50); do
    grep -q "listening on" "$log" && break
    sleep 0.1
done
KYU_ADDR=$(sed -n 's/^kyu listening on //p' "$log")
[ -n "$KYU_ADDR" ] || { echo "kyu-server did not start:"; cat "$log"; exit 1; }
export KYU_ADDR
unset KYU_LOGIN KYU_PASSCODE
echo "kyu-server on $KYU_ADDR"

node_example() {
    cd examples/node && { [ -d node_modules ] || npm ci --no-fund --no-audit --silent; } && npm start --silent
}
python_example() {
    cd examples/python && { [ -d .venv ] || { python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }; } &&
        .venv/bin/python example.py
}
ruby_example() {
    cd examples/ruby && bundle config set --local path vendor/bundle > /dev/null &&
        bundle install --quiet && bundle exec ruby example.rb
}

failed=()
for name in "${@:-node python ruby}"; do
    for lang in $name; do
        echo "--- $lang"
        if ! ("${lang}_example"); then failed+=("$lang"); fi
    done
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "FAILED: ${failed[*]}"
    exit 1
fi
echo "all examples passed"
