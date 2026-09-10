#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
first_dir="$(mktemp -d)"
second_dir="$(mktemp -d)"
trap 'rm -rf "$first_dir" "$second_dir"' EXIT

commands=(brokerd brokerctl broker-worker loadgen native-bench broker-wal)
for command in "${commands[@]}"; do
  (cd "$repo_dir/cmd" && CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$first_dir/$command" "./$command")
  (cd "$repo_dir/cmd" && CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$second_dir/$command" "./$command")
  cmp "$first_dir/$command" "$second_dir/$command"
done

echo "reproducible command builds verified"
