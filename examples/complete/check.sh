#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

work="$(mktemp -d)"
broker_pid=""
consumer_pid=""
distributed_pid=""
cleanup() {
  for pid in "$distributed_pid" "$consumer_pid" "$broker_pid"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 50); do
        kill -0 "$pid" 2>/dev/null || break
        sleep .1
      done
      if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid" 2>/dev/null || true; fi
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$work"
}
trap cleanup EXIT

go build -o "$work/complete-broker" ./complete/broker
go build -o "$work/complete-consumer" ./complete/consumer
go build -o "$work/complete-publisher" ./complete/publisher
go build -o "$work/complete" ./complete

addr="${ADDR:-127.0.0.1:18091}"
url="http://$addr"
token="complete-example-token"
"$work/complete-broker" -addr "$addr" -data "$work/data" -token "$token" >"$work/broker.log" 2>&1 &
broker_pid=$!
for _ in $(seq 1 100); do
  if curl -fsS "$url/readyz" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$broker_pid" 2>/dev/null; then tail -100 "$work/broker.log"; exit 1; fi
  sleep .1
done
curl -fsS "$url/readyz" >/dev/null

"$work/complete-consumer" -url "$url" -token "$token" >"$work/consumer.log" 2>&1 &
consumer_pid=$!
"$work/complete-publisher" -url "$url" -token "$token"
for expected in "consumer queue" "consumer topic" "consumer request" "consumer task"; do
  for _ in $(seq 1 50); do
    grep -q "$expected" "$work/consumer.log" && break
    sleep .1
  done
  grep -q "$expected" "$work/consumer.log"
done
echo "broker/consumer/publisher complete"

"$work/complete" embedded
"$work/complete" durable

# Cluster shutdown is intentionally bounded so a transport teardown defect
# fails the check instead of leaving CI hung indefinitely.
"$work/complete" distributed &
distributed_pid=$!
for _ in $(seq 1 300); do
  kill -0 "$distributed_pid" 2>/dev/null || break
  sleep .1
done
if kill -0 "$distributed_pid" 2>/dev/null; then
  kill -9 "$distributed_pid" 2>/dev/null || true
  wait "$distributed_pid" 2>/dev/null || true
  echo "distributed example timed out" >&2
  exit 1
fi
wait "$distributed_pid"

echo "all complete examples passed"
