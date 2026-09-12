#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-$ROOT/artifacts/soak/$RUN_ID}"
DURATION="${DURATION:-72h}"
CHUNK="${CHUNK:-5m}"
CONCURRENCY="${CONCURRENCY:-32}"
BATCH="${BATCH:-32}"
PAYLOAD="${PAYLOAD:-512}"
RESTART_EVERY="${RESTART_EVERY:-12}"
MAX_RSS_GROWTH_KB="${MAX_RSS_GROWTH_KB:-262144}"
ADDR="${ADDR:-127.0.0.1:18090}"
TOKEN="${TOKEN:-soak-local-token}"

mkdir -p "$OUT"
work="$(mktemp -d)"
broker_pid=""
cleanup() {
  if [[ -n "$broker_pid" ]]; then kill "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT

duration_seconds() {
  python3 - "$1" <<'PY'
import re,sys
s=sys.argv[1]; m=re.fullmatch(r'([0-9]+)(s|m|h)',s)
if not m: raise SystemExit("duration must use s, m, or h")
n=int(m.group(1)); print(n*{'s':1,'m':60,'h':3600}[m.group(2)])
PY
}

total_seconds="$(duration_seconds "$DURATION")"
chunk_seconds="$(duration_seconds "$CHUNK")"
cd "$ROOT/cmd"
env GOCACHE="${GOCACHE:-$work/go-cache}" go build -o "$work/brokerd" ./brokerd
env GOCACHE="${GOCACHE:-$work/go-cache}" go build -o "$work/loadgen" ./loadgen
cd "$ROOT"

start_broker() {
  # The harness binds loopback only and tests reliability, not transport
  # security. Deployment TLS/auth is covered by separate conformance checks.
  "$work/brokerd" -addr "$ADDR" -insecure-dev -store wal -wal "$OUT/wal" -profile durable -quiet >>"$OUT/broker.log" 2>&1 &
  broker_pid=$!
  for _ in $(seq 1 120); do
    if curl -fsS "http://$ADDR/readyz" >/dev/null 2>&1; then return; fi
    if ! kill -0 "$broker_pid" 2>/dev/null; then tail -100 "$OUT/broker.log" >&2; return 1; fi
    sleep .25
  done
  echo "broker readiness timeout" >&2
  return 1
}

start_broker
started="$(date +%s)"
deadline=$((started + total_seconds))
iteration=0
initial_rss=""
printf 'timestamp,iteration,rss_kb,wal_bytes\n' >"$OUT/resources.csv"
while (( $(date +%s) < deadline )); do
  iteration=$((iteration + 1))
  remaining=$((deadline - $(date +%s)))
  run_seconds=$chunk_seconds
  if (( remaining < run_seconds )); then run_seconds=$remaining; fi
  log="$OUT/loadgen-$iteration.log"
  "$work/loadgen" -addr "http://$ADDR" -token "$TOKEN" -queue soak.q -mode roundtrip -protocol frame \
    -c "$CONCURRENCY" -duration "${run_seconds}s" -payload "$PAYLOAD" -batch "$BATCH" -pull-limit "$BATCH" | tee "$log"
  grep -Eq 'errors[[:space:]]*:[[:space:]]*0 \(0\.00%\)' "$log"
  rss="$(ps -o rss= -p "$broker_pid" | tr -d ' ')"
  wal_bytes="$(du -sk "$OUT/wal" | awk '{print $1 * 1024}')"
  printf '%s,%d,%s,%s\n' "$(date -u +%FT%TZ)" "$iteration" "$rss" "$wal_bytes" >>"$OUT/resources.csv"
  if [[ -z "$initial_rss" ]]; then initial_rss="$rss"; fi
  if (( rss - initial_rss > MAX_RSS_GROWTH_KB )); then
    echo "RSS growth exceeded ${MAX_RSS_GROWTH_KB} KiB" >&2
    exit 1
  fi
  if (( RESTART_EVERY > 0 )); then
    if (( iteration % RESTART_EVERY == 0 && $(date +%s) < deadline )); then
      kill -9 "$broker_pid" 2>/dev/null || true
      wait "$broker_pid" 2>/dev/null || true
      broker_pid=""
      start_broker
    fi
  fi
done

curl -fsS -H "Authorization: Bearer $TOKEN" "http://$ADDR/stats" >"$OUT/final-stats.json"
printf '{"run_id":"%s","duration":"%s","iterations":%d,"status":"passed"}\n' "$RUN_ID" "$DURATION" "$iteration" >"$OUT/summary.json"
echo "soak passed; evidence: $OUT"
