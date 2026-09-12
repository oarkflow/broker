#!/usr/bin/env bash
set -Eeuo pipefail

# Every published number must come from a command executed in the current run.
# The implemented harness currently covers the durable native broker and Kafka
# with aligned record count, payload, partitions, and exact-drain validation.
# NATS and RabbitMQ remain release-gate work until equivalent measured clients
# exist; this script deliberately makes no claims for them.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Running measured native-broker/Kafka parity benchmark"
echo "NATS JetStream and RabbitMQ parity are not yet implemented."
exec "$ROOT/scripts/benchmark_native_vs_kafka.sh"
