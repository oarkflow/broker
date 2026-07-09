#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TMPDIR="$(mktemp -d)"
ADDR="${BROKER_ADDR:-:$((18090 + RANDOM % 1000))}"
PORT="${ADDR#*:}"
URL="${BROKER_URL:-http://127.0.0.1:$PORT}"
WAL_DIR="$TMPDIR/wal"

BPID=""
CPID=""
WPID=""
cleanup() {
  if [[ -n "${BPID:-}" ]]; then kill "$BPID" 2>/dev/null || true; fi
  if [[ -n "${CPID:-}" ]]; then kill "$CPID" 2>/dev/null || true; fi
  if [[ -n "${WPID:-}" ]]; then kill "$WPID" 2>/dev/null || true; fi
  if [[ -n "${BPID:-}" ]]; then wait "$BPID" 2>/dev/null || true; fi
  if [[ -n "${CPID:-}" ]]; then wait "$CPID" 2>/dev/null || true; fi
  if [[ -n "${WPID:-}" ]]; then wait "$WPID" 2>/dev/null || true; fi
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

go build -o "$TMPDIR/complete-broker" ./examples/complete_broker
go build -o "$TMPDIR/complete-consumer" ./examples/complete_consumer
go build -o "$TMPDIR/complete-worker" ./examples/complete_worker
go build -o "$TMPDIR/complete-publisher" ./examples/complete_publisher

BROKER_ADDR="$ADDR" BROKER_WAL="$WAL_DIR" BROKER_INSECURE_DEV=true BROKER_OBSERVER_JSONL="$TMPDIR/broker-events.jsonl" \
  "$TMPDIR/complete-broker" -config examples/complete_broker/broker.bcl > "$TMPDIR/broker.log" 2>&1 &
BPID=$!
sleep 1.5
if ! kill -0 "$BPID" 2>/dev/null; then
  cat "$TMPDIR/broker.log" >&2
  echo "broker did not start" >&2
  exit 1
fi

BROKER_URL="$URL" CONSUMER_OBSERVER_JSONL="$TMPDIR/consumer-events.jsonl" "$TMPDIR/complete-consumer" -config examples/complete_consumer/consumer.bcl > "$TMPDIR/consumer.log" 2>&1 &
CPID=$!
BROKER_URL="$URL" WORKER_OBSERVER_JSONL="$TMPDIR/worker-events.jsonl" "$TMPDIR/complete-worker" -config examples/complete_worker/worker.bcl > "$TMPDIR/worker.log" 2>&1 &
WPID=$!
sleep 1.5

BROKER_URL="$URL" PUBLISHER_OBSERVER_JSONL="$TMPDIR/publisher-events.jsonl" "$TMPDIR/complete-publisher" -config examples/complete_publisher/publisher.bcl | tee "$TMPDIR/publisher.log"

grep -q "task status=succeeded" "$TMPDIR/publisher.log"
grep -q "request response=" "$TMPDIR/publisher.log"
grep -q "command response=" "$TMPDIR/publisher.log"
grep -q "stream committed offset=" "$TMPDIR/publisher.log"
grep -q "HOOK before_publish" "$TMPDIR/broker.log"
grep -q "CALLBACK live-jsonl-audit notification=message.published" "$TMPDIR/broker.log"
grep -q "CALLBACK broker_event type=message.leased" "$TMPDIR/consumer.log"
grep -q "CALLBACK broker_event type=message.leased" "$TMPDIR/worker.log"
grep -q "task.progress" "$TMPDIR/broker.log"
test -s "$TMPDIR/broker-events.jsonl"
test -s "$TMPDIR/consumer-events.jsonl"
test -s "$TMPDIR/worker-events.jsonl"
test -s "$TMPDIR/publisher-events.jsonl"

echo "complete broker/consumer/worker/publisher lifecycle/hooks/notifications smoke test passed on $URL"
