#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
example_data_dir="$(mktemp -d "${TMPDIR:-/tmp}/broker-example-check.XXXXXX")"
trap 'rm -rf "$example_data_dir"' EXIT

cd "$repo_dir/examples"
go test -timeout=2m ./...
go vet ./...
go run -race ./enhancements -scenario all
go run ./stream_transaction -dir "$example_data_dir/stream"
go run ./stream_transaction -dir "$example_data_dir/stream"
go run ./transactional_outbox -dir "$example_data_dir/outbox"
