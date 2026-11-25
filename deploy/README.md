# Kubernetes deployment tooling for fastworker Broker

**Status: new and unvalidated.** Everything under `deploy/` was written from
reading the repository (`Dockerfile`, `cmd/brokerd/main.go`, `README.md`,
`docs/OPERATIONS.md`, `Tasks.md`) rather than tested against a running
Kubernetes cluster. Treat it as a well-researched starting point, not a
certified deployment path. Please validate in a disposable/staging cluster
before using it for anything that matters, and expect to tune resource
sizing, probe timings, and network policy for your environment.

## What the project itself says about readiness

Per `Tasks.md` (the project's own authoritative readiness tracker, current as
of this writing):

- Version is `0.1.0-dev`.
- **Embedded/single-node mode:** functional pre-v1, not yet
  production-supported.
- **Distributed broker mode:** **experimental, not production-safe.**
- Public stable release is blocked on a documented list of gates
  (`docs/RELEASE_GATES.md`), including a 72-hour soak test, fuzzing,
  independent security review, and more.

**This Kubernetes/Helm tooling does not change any of that.** The Helm chart
now has opt-in wiring for the broker's experimental distributed mode. It does
not make distributed mode production-safe or certify the resulting cluster.

### Clustered Helm mode is opt-in

The Helm chart defaults to `cluster.enabled: false`; with that setting, each
StatefulSet replica is a separate standalone broker. To opt into distributed
mode, set `cluster.enabled: true`, keep `persistence.enabled: true`, use an odd
`replicaCount` of at least three, and provide a `tls.existingSecret` containing
`tls.crt`, `tls.key`, and `ca.crt`. The chart rejects clustered mode with
ephemeral `emptyDir` storage because broker, Raft, and partition WALs all live
under `/data`. It also requires a PodDisruptionBudget with integer
`maxUnavailable` no greater than `floor(replicaCount/2)`, and hard topology
spreading across the configured `topologyKey`; pods remain pending if enough
eligible topology domains are unavailable. Set `image.digest` to the same
`sha256:<64 lowercase hex characters>` digest for every replica; clustered
mode rejects mutable tags so a rollout cannot mix image contents.
The certificate must validate each pod's headless-service DNS name. The chart
generates stable Raft IDs and peer addresses from StatefulSet ordinals and
stores broker, Raft, and partition WAL data in separate directories on each
persistent volume.

This configuration bootstraps a fixed-size initial cluster. Do not change
`replicaCount` as a scaling mechanism: node additions and removals must follow
the authenticated Raft membership workflow. The chart and cluster mode remain
unvalidated against a live Kubernetes cluster and are not production-certified.
The raw manifests under `deploy/kubernetes/` still describe standalone mode.

### Native BST1 stream (port 8091)

BST1 supports bearer-token authentication or a verified client certificate
mapped by the broker ACL. In production, `brokerd` requires TLS and a usable
ACL credential; anonymous access and token bypass require
`--insecure-dev`/`BROKER_INSECURE_DEV`. The raw manifests and Helm chart leave
the listener disabled by default. With the Helm chart, enabling it adds the
native port to both Services. When `networkPolicy.enabled=true`, the chart
continues to block BST1 until `networkPolicy.nativeIngressFrom` explicitly
lists allowed peers. Configure production TLS and authentication, and restrict
external access if `service.type` is NodePort or LoadBalancer. The raw manifests
still require manual Service and NetworkPolicy changes. This listener and the
deployment manifests remain pre-v1 and unvalidated against a live cluster.

For example, permit BST1 only from a client namespace by adding this to a
values file:

```yaml
networkPolicy:
  enabled: true
  nativeIngressFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: broker-clients
```

## Contents

```
deploy/
├── kubernetes/        # Raw manifests, apply with kubectl
│   ├── 00-namespace.yaml
│   ├── 01-serviceaccount.yaml
│   ├── 02-configmap.yaml
│   ├── 03-secret.yaml            # TEMPLATE — placeholder values only
│   ├── 04-service-headless.yaml  # peer discovery / stable DNS
│   ├── 05-service.yaml           # client traffic (ClusterIP)
│   ├── 06-poddisruptionbudget.yaml
│   ├── 07-networkpolicy.yaml
│   └── 08-statefulset.yaml
└── helm/broker/        # Equivalent chart, parameterized via values.yaml
    ├── Chart.yaml
    ├── values.yaml
    └── templates/
```

## Prerequisites

- Kubernetes 1.24+ (uses `policy/v1` PodDisruptionBudget and standard
  `networking.k8s.io/v1` NetworkPolicy — your CNI must enforce
  NetworkPolicy, e.g. Calico/Cilium; some CNIs do not).
- A default (or explicitly named) `StorageClass` that supports
  `ReadWriteOnce` volumes for the WAL `PersistentVolumeClaim`.
- A real `brokerd` image pushed to a registry you control. Neither the raw
  manifests nor the chart publish or reference a maintained public image;
  `image.repository`/`image.tag` are placeholders (`ghcr.io/oarkflow/broker:latest`)
  — build and push the image from the repository's root `Dockerfile`
  (`brokerd` target) and point the manifests at your own tag/digest.
- Real TLS certificate/key material. `docs/OPERATIONS.md` requires TLS or
  mTLS, and `main.go` itself refuses to start in non-`insecure-dev` mode
  without `-tls-cert`/`-tls-key`. The committed Secret/`values.yaml` only
  contain placeholder PEM blocks that will fail the TLS handshake.
- `kubectl` (and `helm`, if using the chart) configured against your target
  cluster.

## Installing with raw manifests

```bash
# 1. Build and push your own image, then edit the `image:` field in
#    deploy/kubernetes/08-statefulset.yaml to point at it.

# 2. Replace deploy/kubernetes/03-secret.yaml with real values. Prefer NOT
#    committing real secrets — create it imperatively instead, e.g.:
kubectl create namespace broker
kubectl create secret generic broker-secrets -n broker \
  --from-literal=BROKER_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=BROKER_READ_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=BROKER_WORKER_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=BROKER_PUBLISHER_TOKEN="$(openssl rand -hex 32)" \
  --from-literal=BROKER_FRAME_SIGNING_KEY="$(openssl rand -hex 32)" \
  --from-literal=BROKER_FRAME_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  --from-literal=BROKER_AMQP_USERNAME=broker \
  --from-literal=BROKER_AMQP_PASSWORD="$(openssl rand -hex 16)" \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt

# 3. Apply everything else (skip 00-namespace.yaml and 03-secret.yaml if you
#    created the namespace/secret imperatively above):
kubectl apply -f deploy/kubernetes/

# 4. Watch the rollout:
kubectl -n broker rollout status statefulset/broker
```

## Installing with Helm

Helm is not installed in the current authoring environment. The clustered-mode
templates have only received source review; neither `helm lint` nor `helm
template` has been run after these changes, and no live cluster validation has
been performed. Run these before relying on the chart:

```bash
helm lint deploy/helm/broker
helm template broker deploy/helm/broker | kubectl apply --dry-run=client -f -

helm install broker deploy/helm/broker \
  --create-namespace -n broker \
  --set image.repository=your-registry/broker \
  --set image.tag=v0.1.0-your-build \
  --set-string secrets.adminToken="$(openssl rand -hex 32)" \
  --set-file tls.crt=./server.crt \
  --set-file tls.key=./server.key
```

See `deploy/helm/broker/templates/NOTES.txt` (shown after install) for
post-install verification steps. Key `values.yaml` knobs:

| Key | Purpose |
|---|---|
| `replicaCount` | Number of independent broker pods (default 3; see caveat above — not a real cluster yet) |
| `image.repository` / `image.tag` / `image.digest` | Your built image; clustered mode requires the immutable SHA-256 digest |
| `persistence.size` / `persistence.storageClassName` | WAL volume sizing |
| `tls.enabled` / `tls.existingSecret` | TLS on/off, bring-your-own-secret |
| `secrets.existingSecret` | Use an externally managed Secret instead of the chart's placeholder one |
| `broker.listeners.*.enabled` | Toggle gRPC/MQTT/AMQP listeners; enabling native adds its port to both Services |
| `service.ports.native` | BST1 Service port; must match `broker.listeners.native.addr` when enabled |
| `networkPolicy.nativeIngressFrom` | Explicit NetworkPolicy peers allowed to reach BST1; empty keeps it blocked |
| `broker.extraEnv` | Pass through extra env vars (e.g. `BROKER_OTLP_ENDPOINT`) |
| `resources` | CPU/memory requests and limits |

## Configuring TLS

Both the raw manifests and the chart default to TLS **enabled**, matching
`docs/OPERATIONS.md` ("Require TLS or mTLS ... Never enable `--insecure-dev`
outside an isolated development machine"). You must supply real certificate
material — the shipped placeholders are deliberately invalid and will cause
TLS handshake failures / container crash loops if left in place. For mTLS,
also set `tls.requireClientCert: true` (Helm) or add
`-client-ca=/etc/broker/tls/ca.crt -require-client-cert` to the container
args (raw manifests) and include `ca.crt` in the Secret.

## Operational notes carried over from `docs/OPERATIONS.md`

- Use the `durable` performance profile with a segmented WAL on dedicated,
  monitored storage (default in both the manifests and chart).
- Back up by stopping writes or snapshotting the volume, copying the WAL
  directory with permissions preserved, and verifying with `broker-wal`
  before trusting the backup. Never overlay a restore onto an active broker.
- Rolling upgrades: back up every node first, drain/upgrade one pod at a
  time, wait for it to rejoin and catch up before moving to the next. Do not
  mix versions across a release that changes Raft commands or storage
  encoding without a documented compatibility window.
- Wire the required alerts documented there (readiness/health failures, no
  leader/leader churn/quorum loss once clustering exists, WAL fsync
  latency/errors, disk/inode exhaustion, queue depth and DLQ growth,
  publish/consume error rates and latency percentiles, auth failures) into
  your monitoring stack — none of that is automated by these manifests.

## What this tooling does NOT do

- It does not stand up a real Raft/clustered deployment (see above).
- It does not provision TLS certificates, a certificate authority, secret
  rotation, or integrate with a specific secrets manager — wire that up for
  your environment.
- It does not include a metrics/alerting stack (Prometheus/Grafana); `/metrics`
  is exposed on the admin port for you to scrape.
- It does not include a CI/CD pipeline, image signing, or SBOM generation
  (the repository has scripts for SBOM/vuln scanning elsewhere; this tooling
  only deploys whatever image you give it).
