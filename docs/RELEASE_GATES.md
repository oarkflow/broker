# Release Gates and Production Readiness

This project is pre-v1. A green unit-test run is necessary, but it is not a
production-readiness sign-off. Release claims must be backed by reproducible
artifacts from the exact commit being released.

## Current gate status

| Gate | Status | Required evidence |
| :--- | :---: | :--- |
| All-module tests | Automated | `bash scripts/ci_all_modules.sh test` |
| Race detector | Automated | `bash scripts/ci_all_modules.sh race` |
| Static analysis | Automated | `bash scripts/ci_all_modules.sh vet` |
| Dependency vulnerabilities | Automated | `bash scripts/ci_all_modules.sh vuln`; triage every reachable finding |
| WAL crash recovery and migrations | Covered, expanding | WAL-first fault tests, stable move replay, corrupt/torn recovery, and backup-preserving migration tests |
| Distributed consensus | Covered, expanding | Durable restart, real state snapshots/log compaction, quorum loss, leader replacement, healing, and membership tests |
| Protocol conformance | Incomplete | Compatibility scope is documented in `PROTOCOL_COMPATIBILITY.md`; named third-party client evidence is still required |
| Security | Incomplete | Threat model, fuzzing, dependency scan, and secure defaults exist; independent assessment remains required |
| Performance | Incomplete | Reproducible native benchmark output on named hardware; no synthetic comparison values |
| Operations | Implemented, unverified | `OPERATIONS.md` covers backup/restore, alerts, rolling upgrade and rollback; a release drill is still required |
| Reliability | Harness available | `DURATION=72h scripts/soak.sh`; generated evidence must complete successfully |

## Supported deployment statement

There is currently no generally supported production release. Embedded and
standalone modes are suitable for evaluation with the documented security and
durability settings. Distributed mode uses durable etcd/raft state,
deterministic mutations, and complete broker-state snapshots, but remains
pre-v1 until the external and elapsed-time gates above are demonstrated and a
release candidate is signed off.

Do not label a build production-ready solely because the automated test, race,
and vet gates pass. Record the commit, environment, commands, logs, benchmark
hardware, and reviewer for every manual gate.

## Verify tagged release assets

Download every asset from the tagged GitHub release into one directory,
including `checksums.txt` and `cosign.bundle`. Verify the checksum manifest
against the exact GitHub Actions release workflow identity, then verify every
listed archive and CycloneDX SBOM:

```bash
cosign verify-blob checksums.txt \
  --bundle cosign.bundle \
  --certificate-identity-regexp '^https://github\.com/oarkflow/broker/\.github/workflows/release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
sha256sum -c checksums.txt
```

The checksum manifest covers the downloadable release archives and per-binary
SBOMs, the digest-pinned container image reference, and the per-platform
container SBOMs. The Cosign bundle is the signature evidence for the manifest
and is not listed inside that manifest. Do not use `--ignore-missing`: all
listed files must be present for the check to pass.

The release workflow also publishes `linux/amd64` and `linux/arm64` broker
images to GHCR, signs the manifest-list digest, attaches a CycloneDX
attestation for each platform, and verifies the signature and attestation
identity before publishing the GitHub release. After verifying the release
checksums, verify the image signature and its CycloneDX attestation using the
digest reference in `container-image.txt`:

```bash
IMAGE_REF="$(cat container-image.txt)"
cosign verify "$IMAGE_REF" \
  --certificate-identity-regexp '^https://github\.com/oarkflow/broker/\.github/workflows/release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
cosign verify-attestation "$IMAGE_REF" \
  --type cyclonedx \
  --certificate-identity-regexp '^https://github\.com/oarkflow/broker/\.github/workflows/release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
```

## Release sign-off

A stable release requires all rows above to be complete, no unresolved critical
or high-severity security findings, documented upgrade and rollback procedures,
and explicit maintainer approval. The repository includes the Apache License
2.0; confirm its use under the deploying organization's governance policy.
