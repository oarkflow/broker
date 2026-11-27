#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-benchmark-results/native-$RUN_ID}"
NUM_RECORDS="${NUM_RECORDS:-1000000}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-256}"
PARTITIONS="${PARTITIONS:-8}"
PUBLISH_BATCH="${PUBLISH_BATCH:-1000}"
PRODUCER_CONCURRENCY="${PRODUCER_CONCURRENCY:-16}"
CONSUMER_CONCURRENCY="${CONSUMER_CONCURRENCY:-8}"
FETCH_BATCH="${FETCH_BATCH:-4000}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837}"
KAFKA_BENCH_IMAGE="broker-kafka-bench:$RUN_ID"
mkdir -p "$OUT"

cleanup(){ docker compose -f docker-compose.native-bench.yml --profile tools down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka-bench kafka-producer kafka-consumer >/dev/null 2>&1 || true; docker network rm broker-native-bench >/dev/null 2>&1 || true; docker image rm -f "$KAFKA_BENCH_IMAGE" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "[$(date +%T)] Building native broker and benchmark clients"
docker compose -f docker-compose.native-bench.yml --profile tools build

echo "[$(date +%T)] Starting durable BST1 broker"
PARTITIONS="$PARTITIONS" docker compose -f docker-compose.native-bench.yml up -d broker
ready=0
for _ in $(seq 1 120); do
 if nc -z 127.0.0.1 8091; then ready=1; break; fi
 sleep .5
done
if [[ "$ready" != 1 ]]; then
 echo "Broker did not become ready; container logs follow:" >&2
 docker compose -f docker-compose.native-bench.yml logs broker >&2 || true
 exit 1
fi
sleep 1 # The TCP listener opens just before the native handshake handler is ready.

echo "[$(date +%T)] Native publish: $NUM_RECORDS records"
NUM_RECORDS="$NUM_RECORDS" PAYLOAD_SIZE="$PAYLOAD_SIZE" PARTITIONS="$PARTITIONS" PUBLISH_BATCH="$PUBLISH_BATCH" PRODUCER_CONCURRENCY="$PRODUCER_CONCURRENCY" \
 docker compose -f docker-compose.native-bench.yml --profile tools run --rm native-publisher | tee "$OUT/native-publish.txt"

echo "[$(date +%T)] Restarting broker and recovering segmented logs"
docker compose -f docker-compose.native-bench.yml restart broker
sleep 2

echo "[$(date +%T)] Native consume: exact drain"
NUM_RECORDS="$NUM_RECORDS" PARTITIONS="$PARTITIONS" CONSUMER_CONCURRENCY="$CONSUMER_CONCURRENCY" FETCH_BATCH="$FETCH_BATCH" GROUP="bench-$RUN_ID" \
 docker compose -f docker-compose.native-bench.yml --profile tools run --rm native-consumer | tee "$OUT/native-consume.txt"

grep -q 'errors=0' "$OUT/native-publish.txt"
grep -q "records=$NUM_RECORDS" "$OUT/native-publish.txt"
grep -q 'errors=0' "$OUT/native-consume.txt"
grep -q "records=$NUM_RECORDS" "$OUT/native-consume.txt"

# Kafka single-node comparison: match record count, size, partitions,
# producer count, batch size and CPU/memory caps; force disk flushes at batch scale.
docker network create broker-native-bench >/dev/null
docker run -d --name kafka-bench --network broker-native-bench \
 --cpus="${BROKER_CPUS:-4}" --memory="${BROKER_MEMORY:-4g}" \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-bench:9092 \
 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-bench:9093 \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_LOG_FLUSH_INTERVAL_MESSAGES="${PUBLISH_BATCH}" "$KAFKA_IMAGE" >/dev/null
kafka_ready=0
for _ in $(seq 1 240); do
 if docker exec kafka-bench /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  kafka_ready=1
  break
 fi
 sleep .5
done
if [[ "$kafka_ready" != 1 ]]; then
 echo "Kafka did not become ready; container logs follow:" >&2
 docker logs kafka-bench >&2 || true
 exit 1
fi
TOPIC="bench-$RUN_ID"
docker exec kafka-bench /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null

echo "[$(date +%T)] Kafka publish: $NUM_RECORDS records, $PRODUCER_CONCURRENCY concurrent producers"
docker build -f scripts/Dockerfile.kafka-bench --build-arg "KAFKA_IMAGE=$KAFKA_IMAGE" \
 -t "$KAFKA_BENCH_IMAGE" .
docker run --rm --name kafka-producer --network broker-native-bench \
 --cpus="${BENCH_CPUS:-4}" --memory="${BENCH_MEMORY:-4g}" \
 -e KAFKA_HEAP_OPTS='-Xms128m -Xmx1g' \
 "$KAFKA_BENCH_IMAGE" "$TOPIC" "$NUM_RECORDS" "$PAYLOAD_SIZE" \
 "$PARTITIONS" "$PRODUCER_CONCURRENCY" "$PUBLISH_BATCH" | tee "$OUT/kafka-publish.txt"

echo "[$(date +%T)] Kafka fetch baseline (one consumer; offset commit semantics differ)"
docker run --rm --name kafka-consumer --network broker-native-bench "$KAFKA_IMAGE" \
 /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-bench:9092 --topic "$TOPIC" --num-records "$NUM_RECORDS" --group "bench-$RUN_ID" --timeout 120000 | tee "$OUT/kafka-consume.txt"

python3 - "$OUT" "$NUM_RECORDS" "$KAFKA_IMAGE" "$PARTITIONS" "$PAYLOAD_SIZE" "$PRODUCER_CONCURRENCY" "$PUBLISH_BATCH" "${BROKER_CPUS:-4}" "${BROKER_MEMORY:-4g}" "${CONSUMER_CONCURRENCY:-8}" <<'PY'
import csv,json,re,sys,pathlib
out=pathlib.Path(sys.argv[1]); target=int(sys.argv[2])
def native(name):
 s=(out/name).read_text();m=re.search(r'RESULT_JSON=(\{.*\})',s)
 if not m: raise SystemExit(f"missing RESULT_JSON in {name}")
 return json.loads(m.group(1))
def kafka_pub():
 s=(out/'kafka-publish.txt').read_text()
 m=re.search(r'records=(\d+) errors=(\d+) elapsed_seconds=([0-9.]+) msg/s=([0-9.]+)',s)
 if not m: raise SystemExit("unable to parse Kafka publish result")
 return int(m.group(1)), int(m.group(2)), float(m.group(4))
def kafka_cons():
 lines=[ln.strip() for ln in (out/'kafka-consume.txt').read_text().splitlines() if ln.strip()]
 # Non-detailed ConsumerPerformance output has:
 # start.time,end.time,MB,MB.sec,nMsg,nMsg.sec,rebalance.ms,fetch.ms,fetch.MB.sec,fetch.nMsg.sec
 for line in reversed(lines):
  row=next(csv.reader([line]))
  if len(row) < 10: continue
  try:
   consumed=int(float(row[4].strip()))
   total_rate=float(row[5].strip())
   fetch_rate=float(row[9].strip())
   return consumed,total_rate,fetch_rate
  except ValueError:
   continue
 raise SystemExit("unable to parse Kafka consumer CSV output")
p=native('native-publish.txt');c=native('native-consume.txt');kpubcount,kperrors,kp=kafka_pub();kcount,kc_total,kc_fetch=kafka_cons()
correct=(p['errors']==0 and c['errors']==0 and p['records']==target and c['records']==target and kpubcount==target and kperrors==0 and kcount==target)
summary={
 'native_publish_msg_s':p['messages_per_second'],
 'native_fetch_and_durable_commit_msg_s':c['messages_per_second'],
 'kafka_publish_msg_s':kp,
 'kafka_consume_total_msg_s_including_rebalance':kc_total,
 'kafka_consume_fetch_msg_s':kc_fetch,
 'kafka_consumed':kcount,
 'native_publish_ratio':p['messages_per_second']/kp,
 'settings':{
  'kafka_image':sys.argv[3], 'partitions':int(sys.argv[4]), 'payload_bytes':int(sys.argv[5]),
  'producers_per_system':int(sys.argv[6]), 'records_per_produce_batch':int(sys.argv[7]),
  'kafka_log_flush_interval_messages':int(sys.argv[7]), 'broker_cpu_limit':sys.argv[8],
  'broker_memory_limit':sys.argv[9], 'native_consumer_workers':int(sys.argv[10]),
  'kafka_consumer_workers':1, 'native_sync':True, 'kafka_acks':'all', 'kafka_replication_factor':1,
 },
 'records':target,
 'correct':correct,
}
(out/'summary.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2))
if not correct:
 raise SystemExit(f"benchmark invalid: expected {target} records; Kafka consumed {kcount}")
PY
echo "Results: $OUT"
