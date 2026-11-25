# Protocol Compatibility

| Interface | Implemented scope | Production status |
| :--- | :--- | :--- |
| HTTP/JSON and FWB frames | Publish, batch, pull, ack/nack, queue/admin APIs | Pre-v1; repository integration tests |
| gRPC | Broker protobuf service over HTTP/2 | Pre-v1; generated client and integration tests required per release |
| AMQP 0-9-1 | Core connection/channel, publish, consume and confirms subset | Subset only; not full RabbitMQ compatibility |
| MQTT 3.1.1 | Connect, publish/subscribe and persistent-session subset | Subset only; MQTT 5 is not supported |
| WebSocket | Broker data-plane framing over RFC 6455 | Pre-v1 extension, not an industry broker protocol |
| Native JSON/TCP | Bounded request/response transport | Pre-v1, no compatibility promise |
| BST1 stream | Append/fetch/commit with token or mapped mTLS authentication; BST2 handshake negotiates the supported frame version | Pre-v1; production requires TLS and ACL; only frame version 1 is implemented and compatibility certification remains open |

“Implemented” does not mean complete protocol conformance. A release record must
name the third-party client libraries and versions exercised against the exact
release commit. Unsupported methods or protocol versions must fail explicitly;
they must not be advertised as compatible.
