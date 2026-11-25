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

BST1 is the current append/fetch/commit frame protocol implemented in
`native_stream.go`. New clients begin with a BST2 negotiation envelope that
offers a minimum and maximum protocol version; the server selects the highest
supported version in that range. The current implementation selects BST1
version 1 only. A range with no common version receives an explicit protocol
error before authentication. The selected frame protocol still uses BST1
version 1; BST2 is the negotiation envelope, not a second data-frame codec.

The negotiation handshake authenticates with a bearer token or a verified
client certificate mapped by the shared ACL. Legacy BST1 clients remain
accepted by the server. New clients do not silently downgrade when talking to
an older BST1-only server; set `NativeStreamDialOptions.LegacyHandshake` for
that migration case. `brokerd` requires TLS and an ACL credential in production;
anonymous access and development token bypass are available only with
`--insecure-dev`. Requests use bounded length-prefixed binary frames. The
protocol remains pre-v1 and has no stable compatibility promise.
