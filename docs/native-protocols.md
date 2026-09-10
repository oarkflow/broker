# Native protocol specifications

Status: pre-v1. These transports are not gRPC, AMQP, MQTT, or WebSocket.

## Native JSON/TCP RPC

Each request and response is one JSON object consumed by `json.Decoder`; clients
must not assume newline framing. Requests contain `id`, `method`, `body`, and
optional `headers`. When authentication is enabled, `headers.Authorization`
contains a bearer credential. Responses contain the matching `id`, numeric
`status`, optional `error`, and optional JSON `body`.

The default maximum decoded message is 4 MiB. TLS and mTLS are configured with
`NativeRPCConfig`. This protocol has no stable compatibility promise before v1.

## BST1 native stream

BST1 is the current append/fetch/commit stream protocol implemented in
`native_stream.go`. It uses bounded length-prefixed binary requests. It is
unauthenticated and therefore rejected by `brokerd` production mode. BST2 will
add version negotiation, authenticated identity, explicit error classes, and
partition leader metadata. BST1 remains development-only.
