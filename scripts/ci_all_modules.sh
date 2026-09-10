#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
modules=("." "cmd" "contrib" "examples")

for module in "${modules[@]}"; do
  echo "==> ${mode}: ${module}"
  case "$mode" in
    test)
      (cd "$repo_dir/$module" && go test -count=1 -timeout=3m ./...)
      ;;
    race)
      (cd "$repo_dir/$module" && go test -race -count=1 -timeout=5m ./...)
      ;;
    vet)
      (cd "$repo_dir/$module" && go vet ./...)
      ;;
    *)
      echo "unknown mode: $mode" >&2
      exit 2
      ;;
  esac
done
