# Security Threat Model

## Assets and trust boundaries

Protected assets are message payloads, credentials, tenant boundaries, consumer
offsets, broker configuration, WAL/snapshots, and cluster leadership. Trust
boundaries exist at every HTTP, AMQP, MQTT, gRPC, WebSocket, native, admin, and
inter-node listener, plus the filesystem and observability exporters.

## Primary threats and controls

| Threat | Required controls |
| :--- | :--- |
| Unauthenticated access | TLS/mTLS, authenticated handshakes, no production insecure mode |
| Cross-tenant disclosure or mutation | Tenant-derived server context, authorization on reads and writes, isolation tests |
| Replay/duplicate publication | Idempotency keys and durable producer epoch/sequence fencing |
| Message tampering | TLS in transit; HMAC/AEAD framing and checksummed WAL/snapshots at rest |
| Resource exhaustion | Frame/body/message bounds, queue depth, quotas, rate limits, bounded concurrency |
| Malicious/corrupt persistence | Strict decoding, checksums, fail-closed migration, immutable backups |
| Split brain or stale leader writes | etcd/raft quorum, CheckQuorum, leader fencing, replicated mutations |
| Secret leakage | Protected environment/file injection, redacted logs, no CLI/history secrets |
| Dependency compromise | Pinned modules, reproducible builds, SBOM, vulnerability scanning |

## Explicitly untrusted inputs

Treat payloads, headers, routing keys, filters, schema identifiers, compressed
frames, WAL imports, snapshot files, peer messages, and configuration files as
hostile. Parsing must be bounded before allocation and errors must not expose
credentials or payload contents.

## Release review checklist

- Run tests, race detection, fuzzing, `go vet`, and `govulncheck` on the release commit.
- Review every listener for authentication, TLS, size/time limits, and shutdown behavior.
- Verify authorization for every control-plane and data-plane operation.
- Exercise corrupt/truncated/encrypted WAL and snapshot recovery.
- Review logs, metrics, traces, crash output, and generated artifacts for secrets.
- Obtain an independent assessment before declaring a stable production release.

Security reports follow `SECURITY.md`. This document defines engineering scope;
it is not an assertion that an independent review has occurred.
