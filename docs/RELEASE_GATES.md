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

## Release sign-off

A stable release requires all rows above to be complete, no unresolved critical
or high-severity security findings, documented upgrade and rollback procedures,
and explicit maintainer approval. The repository includes the Apache License
2.0; confirm its use under the deploying organization's governance policy.
