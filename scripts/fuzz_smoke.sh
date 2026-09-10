#!/usr/bin/env bash
set -euo pipefail

duration="${1:-30s}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repo_dir"
go test -run='^$' -fuzz='^FuzzWALRecordDecode$' -fuzztime="$duration" .
go test -run='^$' -fuzz='^FuzzBinaryMessageDecode$' -fuzztime="$duration" .
