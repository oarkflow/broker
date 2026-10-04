#!/usr/bin/env bash
# Manual verification for the oarkflow broker admin API and operator console.
#
# Starts a real brokerd, exercises every management surface over HTTP with curl,
# and asserts the responses. Everything here is plain HTTP so you can read what it
# does and run the same commands by hand.
#
# Usage:
#   bash scripts/manual_verify.sh                 # HTTP, no auth (fast)
#   bash scripts/manual_verify.sh --tls           # TLS + token, proves auth is enforced
#
# Nothing is written outside a temp dir, which is removed on exit.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
PORT_HTTP="${PORT_HTTP:-18090}"
PORT_TLS="${PORT_TLS:-18443}"
TOKEN="verify-token"
USE_TLS=0
[[ "${1:-}" == "--tls" ]] && USE_TLS=1

PASS=0
FAIL=0
BROKER_PID=""

cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null
  rm -rf "$TMP"
}
trap cleanup EXIT

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# check <label> <expected> <actual>
check() { if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 (want $2, got $3)"; fi; }
# contains <label> <needle> <haystack>
contains() { if [[ "$3" == *"$2"* ]]; then ok "$1"; else bad "$1 (missing '$2' in: ${3:0:200})"; fi; }

# build the binaries
say "building"
(cd "$ROOT/cmd" && go build -o "$TMP/brokerd" ./brokerd) || { echo "build failed"; exit 1; }

# base URL + curl auth args
if [[ $USE_TLS -eq 1 ]]; then
  command -v openssl >/dev/null || { echo "--tls needs openssl"; exit 1; }
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$TMP/ca.key" -out "$TMP/ca.pem" \
    -days 2 -subj "/CN=oarkflow verify CA" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "$TMP/srv.key" -out "$TMP/srv.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$TMP/ext.cnf"
  openssl x509 -req -in "$TMP/srv.csr" -CA "$TMP/ca.pem" -CAkey "$TMP/ca.key" \
    -CAcreateserial -out "$TMP/srv.pem" -days 2 -extfile "$TMP/ext.cnf" >/dev/null 2>&1
  ADDR="https://127.0.0.1:$PORT_TLS"
  BROKER_ARGS=(-token "$TOKEN" -addr "127.0.0.1:$PORT_TLS"
               -tls-cert "$TMP/srv.pem" -tls-key "$TMP/srv.key")
  CURL=(curl -s --cacert "$TMP/ca.pem")
  AUTH=(-H "Authorization: Bearer $TOKEN")
else
  ADDR="http://127.0.0.1:$PORT_HTTP"
  BROKER_ARGS=(--insecure-dev -addr "127.0.0.1:$PORT_HTTP")
  CURL=(curl -s)
  AUTH=()
fi

say "starting brokerd on $ADDR"
"$TMP/brokerd" "${BROKER_ARGS[@]}" -wal "$TMP/wal" -store wal -quiet > "$TMP/brokerd.log" 2>&1 &
BROKER_PID=$!

for _ in $(seq 1 50); do
  "${CURL[@]}" "${AUTH[@]+"${AUTH[@]}"}" -o /dev/null "$ADDR/healthz" 2>/dev/null && break
  sleep 0.2
done
if ! kill -0 "$BROKER_PID" 2>/dev/null; then
  echo "brokerd failed to start:"; cat "$TMP/brokerd.log"; exit 1
fi

g()   { "${CURL[@]}" "${AUTH[@]+"${AUTH[@]}"}" "$@"; }
code() { "${CURL[@]}" "${AUTH[@]+"${AUTH[@]}"}" -o /dev/null -w '%{http_code}' "$@"; }
# code_bare sends no credentials at all. Needed for the negative auth assertions:
# code() always attaches the token, so it cannot prove a request is rejected.
code_bare() { "${CURL[@]}" -o /dev/null -w '%{http_code}' "$@"; }

# ---------------------------------------------------------------- auth
say "auth"
if [[ $USE_TLS -eq 1 ]]; then
  # Probes are deliberately unauthenticated; everything else is not.
  check "healthz is an unauthenticated probe" 200 "$(code "$ADDR/healthz")"
  check "stats without a token is rejected"   401 "$(code_bare "$ADDR/stats")"
  check "stats with a bad token is rejected"  401 "$(code_bare -H 'Authorization: Bearer wrong' "$ADDR/stats")"
  check "stats with the right token works"    200 "$(code "${AUTH[@]}" "$ADDR/stats")"
  check "console without a token is rejected" 401 "$(code_bare "$ADDR/console/")"
  check "console with a token works"          200 "$(code "${AUTH[@]}" "$ADDR/console/")"
else
  ok "skipping auth assertions (--insecure-dev disables auth; rerun with --tls)"
fi

# ---------------------------------------------------------------- console
say "console assets"
check "shell served"        200 "$(code "$ADDR/console/")"
check "stylesheet served"   200 "$(code "$ADDR/console/app.css")"
check "script served"       200 "$(code "$ADDR/console/app.js")"
check "deep link gets shell" 200 "$(code "$ADDR/console/consumers/deep/link")"
check "missing asset 404s"  404 "$(code "$ADDR/console/nope.js")"
contains "CSP forbids inline script" "script-src 'self'" "$(g -I "$ADDR/console/")"
contains "CSP pins default-src"      "default-src 'self'" "$(g -I "$ADDR/console/")"
if [[ "$("${CURL[@]}" "${AUTH[@]+"${AUTH[@]}"}" -I "$ADDR/console/" | grep -i content-security-policy)" == *"unsafe-inline"* ]]; then
  bad "CSP allows unsafe-inline"
else
  ok "CSP has no unsafe-inline"
fi

# ---------------------------------------------------------------- core data
say "queues, topics, publishing"
g -X POST "$ADDR/queues" -d '{"name":"orders"}' > /dev/null
check "declare queue" 200 "$(code -X POST "$ADDR/queues" -d '{"name":"reports"}')"
for i in 1 2 3; do
  g -X POST "$ADDR/publish" -d "{\"queue\":\"orders\",\"payload\":\"order-$i\"}" > /dev/null
done
for i in 1 2 3 4 5; do
  g -X POST "$ADDR/publish" -d "{\"topic\":\"events\",\"payload\":\"evt-$i\"}" > /dev/null
done
contains "orders has depth 3" '"orders":3' "$(g "$ADDR/queues")"

# /publish/batch takes a bare array. The README once documented {"messages":[...]},
# which is rejected, so pin both the accepted and the rejected shape.
g -X POST "$ADDR/publish/batch" -d '[{"queue":"reports","payload":{"n":1}},{"queue":"reports","payload":{"n":2}}]' > /dev/null
contains "batch publish takes a bare array" '"reports":2' "$(g "$ADDR/queues")"
check "batch publish rejects an object wrapper" 400 \
  "$(code -X POST "$ADDR/publish/batch" -d '{"messages":[{"queue":"reports","payload":{"n":1}}]}')"
# Derive the expected total instead of hardcoding it, so adding checks above
# cannot silently invalidate this one.
check "stats message total equals queue+topic counts" ok \
  "$(g "$ADDR/stats" | python3 -c '
import sys, json
ss = json.load(sys.stdin)["store"]
total = sum(ss["queues"].values()) + sum(ss["topics"].values())
print("ok" if total == ss["messages"] else "%d != %d" % (total, ss["messages"]))')"

# ---------------------------------------------------------------- creation
# These are the exact bodies the console's create dialogs send.
say "creating queues, topics, streams, consumers, routes"
check "declare a queue"  200 "$(code -X POST "$ADDR/queues"  -d '{"name":"uiq","max_inflight":8,"visibility_timeout":30000000000,"ack_mode":"manual"}')"
check "declare a topic"  200 "$(code -X POST "$ADDR/topics"  -d '{"name":"uitopic","partitions":1,"retention":86400000000000}')"
check "create a stream"  200 "$(code -X POST "$ADDR/streams" -d '{"name":"uistream","partitions":1,"compact":true}')"
check "add a route"     200 "$(code -X POST "$ADDR/routes"   -d '{"name":"ui-route","match":"uitopic","mode":"publish","to":[{"type":"queue","name":"uiq"}],"enabled":true}')"
contains "the created queue exists"  '"uiq"'      "$(g "$ADDR/stats")"
contains "the created topic exists"  '"uitopic"'  "$(g "$ADDR/stats")"
contains "the created stream exists" '"uistream"' "$(g "$ADDR/streams")"
check "publish to the new queue" 200 "$(code -X POST "$ADDR/publish" -d '{"queue":"uiq","type":"demo","payload":{"hello":"world"}}')"
contains "the published message is readable" '"uiq"' "$(g "$ADDR/queues")"
NEWQ="$(curl -s "${CURL[@]}" "${AUTH[@]+"${AUTH[@]}"}" "$ADDR/queues" | python3 -c 'import sys,json;print(json.load(sys.stdin)["uiq"])')"
check "the new queue took the message" 1 "$NEWQ"
check "list routes" 200 "$(code "$ADDR/routes")"
contains "the route is listed" "ui-route" "$(g "$ADDR/routes")"
check "replay a dead letter queue" 200 "$(code -X POST "$ADDR/dlq/uiq.dlq/replay-all" -d '{}')"

# ---------------------------------------------------------------- broker lifecycle
say "broker pause and resume"
contains "broker starts running" '"state":"running"' "$(g "$ADDR/v1/broker")"
contains "pause takes effect"    '"state":"paused"'  "$(g -X POST "$ADDR/v1/broker" -d '{"action":"pause","reason":"verify"}')"
contains "paused state persists" '"state":"paused"'  "$(g "$ADDR/v1/broker")"
contains "resume takes effect"   '"state":"running"' "$(g -X POST "$ADDR/v1/broker" -d '{"action":"resume"}')"
check    "bad action is rejected" 400 "$(code -X POST "$ADDR/v1/broker" -d '{"action":"explode"}')"
contains "rejected action left state alone" '"state":"running"' "$(g "$ADDR/v1/broker")"

# ---------------------------------------------------------------- topics
say "topic retention read"
contains "topic read returns messages" '"topic":"events"' "$(g "$ADDR/v1/topics/events?limit=2")"
check "unknown topic is a 404" 404 "$(code "$ADDR/v1/topics/nope")"
# Cursor paging must not repeat or skip.
P1="$(g "$ADDR/v1/topics/events?limit=2")"
CUR="$(printf '%s' "$P1" | python3 -c 'import sys,json;print(json.load(sys.stdin)[-1]["id"])')"
P2="$(g "$ADDR/v1/topics/events?limit=2&after=$CUR")"
if [[ "$(printf '%s' "$P2" | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["id"])')" == "$CUR" ]]; then
  bad "cursor page repeated the cursor message"
else
  ok "cursor page advanced past the cursor"
fi
# limit=2 caps every page, so two pages hold 4 of the 5 retained messages. What
# matters is that they do not overlap, and that a third page yields the last one.
check "cursor pages never repeat a message" 4 "$(python3 - "$P1" "$P2" <<'PYEOF'
import json, sys
ids = [m["id"] for m in json.loads(sys.argv[1])] + [m["id"] for m in json.loads(sys.argv[2])]
print(len(ids) if len(set(ids)) == len(ids) else -1)
PYEOF
)"
CUR2="$(printf '%s' "$P2" | python3 -c 'import sys,json;print(json.load(sys.stdin)[-1]["id"])')"
P3="$(g "$ADDR/v1/topics/events?limit=2&after=$CUR2")"
check "third page returns the final message" 1 "$(printf '%s' "$P3" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))')"
check "past the end returns nothing" 0 "$(g "$ADDR/v1/topics/events?limit=2&after=$(printf '%s' "$P3" | python3 -c 'import sys,json;print(json.load(sys.stdin)[-1]["id"])')" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))')"
# Payloads arrive base64 encoded; confirm they decode.
check "payload decodes to the published value" "evt-1" "$(g "$ADDR/v1/topics/events?limit=1" | python3 -c '
import sys, json, base64
raw = base64.b64decode(json.load(sys.stdin)[0]["payload"]).decode()
try:
    print(json.loads(raw))   # published payload was {"payload":"evt-1"}, so a JSON string
except Exception:
    print(raw)')"
# Stats must agree with the topic read.
check "stats topic count matches ListTopic" 5 \
  "$(g "$ADDR/stats" | python3 -c 'import sys,json;print(json.load(sys.stdin)["store"]["topics"]["events"])')"

# ---------------------------------------------------------------- tasks
say "tasks addressable by id"
TASK="$(g -X POST "$ADDR/v1/tasks/submit" -d '{"name":"verify-task","queue":"orders"}' \
        | python3 -c 'import sys,json;print(json.load(sys.stdin).get("id",""))')"
if [[ -n "$TASK" ]]; then ok "submit returned a task id ($TASK)"; else bad "submit returned no task id"; fi
contains "task is listed"            "$TASK" "$(g "$ADDR/v1/tasks?limit=50")"
contains "task is fetchable by id"   "$TASK" "$(g "$ADDR/v1/tasks/$TASK")"
g -X POST "$ADDR/v1/tasks/$TASK/cancel" > /dev/null
check "cancel returns 200" 200 "$(code -X POST "$ADDR/v1/tasks/$TASK/cancel")"
contains "status becomes cancelled" '"status":"cancelled"' "$(g "$ADDR/v1/tasks/$TASK")"
check "unknown task is a 404" 404 "$(code "$ADDR/v1/tasks/does-not-exist")"

# ---------------------------------------------------------------- consumers
say "consumers"
REG="$(g -X POST "$ADDR/v1/consumers/register" -d '{"consumer_id":"verifier","queues":["orders"],"group":"g1"}')"
CID="$(printf '%s' "$REG" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("consumer_id",""))')"
if [[ -n "$CID" ]]; then ok "register returned consumer_id ($CID)"; else bad "register returned no consumer_id"; fi
contains "consumer is listed"    "$CID" "$(g "$ADDR/v1/consumers")"
contains "consumer is fetchable"  "$CID" "$(g "$ADDR/v1/consumers/$CID")"
for action in pause drain resume; do
  check "consumer $action" 200 "$(code -X POST "$ADDR/v1/consumers/$CID/$action")"
done
check "consumer ban"   200 "$(code -X POST "$ADDR/v1/consumers/$CID/ban" -d '{"reason":"verify"}')"
contains "banned flag set" '"banned":true' "$(g "$ADDR/v1/consumers/$CID")"
check "consumer unban" 200 "$(code -X POST "$ADDR/v1/consumers/$CID/unban")"

# ---------------------------------------------------------------- queue control
say "queue control"
check "queue pause"  200 "$(code -X POST "$ADDR/queues/orders/pause")"
check "queue resume" 200 "$(code -X POST "$ADDR/queues/orders/resume")"
check "queue messages readable" 200 "$(code "$ADDR/queues/orders/messages?limit=5")"

# ---------------------------------------------------------------- misc
say "remaining surfaces"
check "healthz"        200 "$(code "$ADDR/healthz")"
check "readyz"         200 "$(code "$ADDR/readyz")"
check "audit"          200 "$(code "$ADDR/audit?limit=10")"
check "cluster status" 200 "$(code "$ADDR/cluster/status")"
check "dlq"            200 "$(code "$ADDR/dlq")"
check "lag needs a stream" 400 "$(code "$ADDR/lag")"

# ---------------------------------------------------------------- console/UI parity
say "console renders the data the API returns"
contains "console references /v1/broker"    "/v1/broker"    "$(g "$ADDR/console/app.js")"
contains "console reads store stats"        "storeStats"    "$(g "$ADDR/console/app.js")"
contains "console uses consumer_id"         "consumer_id"   "$(g "$ADDR/console/app.js")"
contains "console decodes base64 payloads"  "decodePayload" "$(g "$ADDR/console/app.js")"
# Ignore comments: the file documents the wrong shape on purpose.
if grep -v '^[[:space:]]*//' "$ROOT/console/assets/app.js" | grep -q 'st\.queues\|st\.topics\|st\.streams'; then
  bad "console reads a flat st.queues/st.topics path, but /stats nests them under store"
else
  ok "console reads stats through storeStats(), not a flat path"
fi

say "result"
printf '  %d passed, %d failed\n\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]] || exit 1
