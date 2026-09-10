#!/usr/bin/env bash
set -euo pipefail

if ! command -v syft >/dev/null 2>&1; then
  echo "syft is required to generate a CycloneDX SBOM" >&2
  exit 1
fi

source_ref="${1:-dir:.}"
output="${2:-broker.sbom.cdx.json}"
syft scan "$source_ref" -o "cyclonedx-json=$output"
echo "CycloneDX SBOM written to $output"
