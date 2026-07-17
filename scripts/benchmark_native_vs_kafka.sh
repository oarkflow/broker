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
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
mkdir -p "$OUT"

cleanup(){ docker compose -f docker-compose.native-bench.yml --profile tools down -v --remove-orphans >/dev/null 2>&1 || true; docker rm -f kafka-bench kafka-producer kafka-consumer >/dev/null 2>&1 || true; docker network rm broker-native-bench >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "[$(date +%T)] Building native broker and benchmark clients"
docker compose -f docker-compose.native-bench.yml --profile tools build

echo "[$(date +%T)] Starting durable BST1 broker"
PARTITIONS="$PARTITIONS" docker compose -f docker-compose.native-bench.yml up -d broker
until docker compose -f docker-compose.native-bench.yml exec -T broker /brokerd --help >/dev/null 2>&1 || nc -z 127.0.0.1 8091; do sleep .25; done

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

# Kafka standard single-node comparison: same payload, records and partitions.
docker network create broker-native-bench >/dev/null
docker run -d --name kafka-bench --network broker-native-bench \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-bench:9092 \
 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka-bench:9093 \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 "$KAFKA_IMAGE" >/dev/null
until docker exec kafka-bench /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep .5; done
TOPIC="bench-$RUN_ID"
docker exec kafka-bench /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null

echo "[$(date +%T)] Kafka standard publish"
docker run --rm --name kafka-producer --network broker-native-bench "$KAFKA_IMAGE" \
 /opt/kafka/bin/kafka-producer-perf-test.sh --topic "$TOPIC" --num-records "$NUM_RECORDS" --record-size "$PAYLOAD_SIZE" --throughput -1 \
 --command-property bootstrap.servers=kafka-bench:9092 acks=all linger.ms=1 batch.size=1048576 compression.type=none | tee "$OUT/kafka-publish.txt"

echo "[$(date +%T)] Kafka standard consume"
docker run --rm --name kafka-consumer --network broker-native-bench "$KAFKA_IMAGE" \
 /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-bench:9092 --topic "$TOPIC" --num-records "$NUM_RECORDS" --group "bench-$RUN_ID" --timeout 120000 | tee "$OUT/kafka-consume.txt"

python3 - "$OUT" "$NUM_RECORDS" <<'PY'
import csv,json,re,sys,pathlib
out=pathlib.Path(sys.argv[1]); target=int(sys.argv[2])
def native(name):
 s=(out/name).read_text();m=re.search(r'RESULT_JSON=(\{.*\})',s)
 if not m: raise SystemExit(f"missing RESULT_JSON in {name}")
 return json.loads(m.group(1))
def kafka_pub():
 s=(out/'kafka-publish.txt').read_text();m=re.findall(r'([0-9.]+) records/sec',s)
 if not m: raise SystemExit("unable to parse Kafka publish throughput")
 return float(m[-1])
def kafka_cons():
 lines=[ln.strip() for ln in (out/'kafka-consume.txt').read_text().splitlines() if ln.strip()]
 # Non-detailed ConsumerPerformance output has:
 # start.time,end.time,MB,MB.sec,nMsg,nMsg.sec,rebalance.ms,fetch.ms,fetch.MB.sec,fetch.nMsg.sec
 for line in reversed(lines):
  row=next(csv.reader([line]))
  if len(row) < 10: continue
  try:
   consumed=int(float(row[4].strip()))
   rate=float(row[5].strip())
   return consumed,rate
  except ValueError:
   continue
 raise SystemExit("unable to parse Kafka consumer CSV output")
p=native('native-publish.txt');c=native('native-consume.txt');kp=kafka_pub();kcount,kc=kafka_cons()
correct=(p['errors']==0 and c['errors']==0 and p['records']==target and c['records']==target and kcount==target)
summary={
 'native_publish_msg_s':p['messages_per_second'],
 'native_consume_msg_s':c['messages_per_second'],
 'kafka_publish_msg_s':kp,
 'kafka_consume_msg_s':kc,
 'kafka_consumed':kcount,
 'native_publish_ratio':p['messages_per_second']/kp,
 'native_consume_ratio':c['messages_per_second']/kc,
 'records':target,
 'correct':correct,
}
(out/'summary.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2))
if not correct:
 raise SystemExit(f"benchmark invalid: expected {target} records; Kafka consumed {kcount}")
PY
echo "Results: $OUT"
