# ADR-003: Surgewave Native Protocol Design

## Status

Accepted

## Date

2025-11

## Context

Kafka's wire protocol carries overhead from years of backward-compatible evolution: version negotiation on every request, flexible-version headers, tagged fields, and redundant metadata. For deployments where Kafka client compatibility is not needed, this overhead is unnecessary. We measured significant latency spent in protocol parsing and version dispatch, especially at high request rates.

At the same time, dropping Kafka protocol support entirely would prevent adoption by teams with existing Kafka clients and tooling.

### Alternatives Considered

- **Kafka protocol only:** Simplest to maintain, but leaves performance on the table for Surgewave-native clients.
- **Custom protocol only:** Maximum performance, but zero ecosystem compatibility.
- **gRPC:** Good tooling and cross-language support, but adds HTTP/2 framing overhead and makes pipelining awkward.

## Decision

Support dual protocols on different ports:

1. **Kafka wire protocol** on the standard Kafka port for full client compatibility.
2. **Surgewave Native binary protocol** on a separate port for maximum performance.

The native protocol uses a minimal framing format: a fixed-size header with opcode and payload length, followed by the payload. There is no version negotiation --- the protocol version is determined at connection time. Requests are pipelined (multiple in-flight requests per connection without waiting for responses).

Clients choose which protocol to use based on their needs. The Surgewave .NET client defaults to the native protocol.

## Consequences

- **lower latency** measured on the native protocol compared to the Kafka protocol path for simple produce/consume operations.
- **Two protocol handlers** must be maintained. Changes to broker semantics (e.g., new error codes) must be reflected in both.
- **Kafka compatibility** is preserved for existing tooling (kafka-console-consumer, Confluent clients, etc.).
- **Pipelining** enables much higher throughput on a single connection, reducing the need for connection pooling.
- The native protocol is intentionally simple and may not cover all Kafka protocol features (e.g., transactions use the Kafka path).
