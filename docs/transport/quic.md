# QUIC / HTTP/3

Surgewave runs QUIC end-to-end: client ↔ broker on `:9094`, broker ↔ broker
for replication and geo-replication, and HTTP/3 for the gRPC and REST
APIs. Loss-tolerant streaming with 0-RTT resumption keeps cold
reconnects fast.

## Why QUIC

- **0-RTT resumption** — cold reconnects under 50 ms with cached session
  tickets, ideal for mobile producers and edge gateways that cycle
  connections
- **Stream multiplexing** — head-of-line blocking on one topic doesn't
  starve others on the same connection
- **Connection migration** — survives client IP/port changes (Wi-Fi
  ↔ cellular handoff) without re-authenticating
- **Built-in TLS 1.3** — encryption is mandatory, no opt-in TLS to
  forget

## Configuration

```yaml
Surgewave:
  Quic:
    Enabled: true
    Port: 9094
    Certificate: "./certs/surgewave.pfx"
```

## Inter-broker QUIC

Surgewave clusters use QUIC for replication by default — see the
[clustering deployment guide](../clustering/index.md). Replication
falls back to TCP automatically if a peer doesn't advertise QUIC.

## Benchmarks

Local A/B benchmarks live in `benchmarks/Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp`.
Real-world LAN/WAN benchmarks are tracked in the
[performance suite](../performance/benchmarks.md).
