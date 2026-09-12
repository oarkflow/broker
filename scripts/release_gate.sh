#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT="${OUT:-$ROOT/artifacts/release-gates/$RUN_ID}"
mkdir -p "$OUT"
cd "$ROOT"

run() {
  name="$1"; shift
  echo "==> $name"
  "$@" 2>&1 | tee "$OUT/$name.log"
}

run test bash scripts/ci_all_modules.sh test
run race bash scripts/ci_all_modules.sh race
run vet bash scripts/ci_all_modules.sh vet
run vuln bash scripts/ci_all_modules.sh vuln
run fuzz bash scripts/fuzz_smoke.sh "${FUZZ_DURATION:-30s}"
run builds bash scripts/reproducible_build.sh

commit="$(git rev-parse HEAD)"
dirty=false
if ! git diff --quiet || ! git diff --cached --quiet; then dirty=true; fi
printf '{"run_id":"%s","commit":"%s","dirty":%s,"timestamp":"%s","automated_gates":"passed"}\n' \
  "$RUN_ID" "$commit" "$dirty" "$(date -u +%FT%TZ)" >"$OUT/summary.json"
echo "automated release gates passed; evidence: $OUT"
