# Transport Overview

Surgewave supports multiple transport protocols for different use cases.

## Available Transports

| Transport | Port | Use Case |
|-----------|------|----------|
| [Kafka Protocol](kafka-protocol.md) | 9092 | Kafka client compatibility |
| [Native Protocol](native-protocol.md) | 9092 | High-performance .NET |
| [gRPC](grpc.md) | 9093 | Cross-language clients |
| [Shared Memory](shared-memory.md) | IPC | Co-located services |

## Performance Comparison

| Transport | Throughput | Language Support |
|-----------|------------|------------------|
| Kafka | 68K msg/s | All Kafka clients |
| Native | 1.25M msg/s | .NET only |
| gRPC | 500K msg/s | All gRPC languages |
| SharedMemory | 625K msg/s | .NET only |

> Per-protocol P50/P99 latency targets: Native ≤ Kafka wire, Shared Memory ≪ Native, gRPC roughly on par with Kafka. Comparative head-to-head numbers will be published alongside the 1.0 release.

## Selection Guide

```
┌─────────────────────────────────────────────────────────────┐
│                 Transport Selection                          │
├─────────────────────────────────────────────────────────────┤
│ Existing Kafka clients?                                      │
│   Yes → Kafka Protocol (drop-in compatible)                  │
│                                                              │
│ Cross-language (Python, Java, Go)?                           │
│   Yes → gRPC                                                 │
│                                                              │
│ .NET only, maximum performance?                              │
│   Same host → Shared Memory                                  │
│   Network  → Native Protocol                                │
└─────────────────────────────────────────────────────────────┘
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
├─────────────────────────────────────────────────────────────┤
│  Kafka Client  │  Surgewave.Client  │  gRPC Client  │  SHM IPC  │
├─────────────────────────────────────────────────────────────┤
│  TCP :9092     │  TCP :9092     │  HTTP/2 :9093 │  Ring Buf │
├─────────────────────────────────────────────────────────────┤
│                    Surgewave Broker                              │
├─────────────────────────────────────────────────────────────┤
│  Kafka Handler │ Native Handler │ gRPC Service  │ SHM Poller│
└─────────────────────────────────────────────────────────────┘
```

## Configuration

### Kafka Protocol (Default)

```json
{
  "Surgewave": {
    "Port": 9092
  }
}
```

### gRPC

```json
{
  "Surgewave": {
    "GrpcPort": 9093
  }
}
```

### Shared Memory

```json
{
  "Surgewave": {
    "SharedMemory": {
      "Enabled": true,
      "BasePath": "/dev/shm/surgewave"
    }
  }
}
```

## Protocol Detection

Surgewave automatically detects the protocol:

1. **First 4 bytes** - Kafka request length
2. **API Key** - Determines Kafka vs Native
3. **Separate port** - gRPC on dedicated port
4. **Memory region** - Shared memory via mmap

## Next Steps

- [Kafka Protocol](kafka-protocol.md) - Full Kafka compatibility
- [Native Protocol](native-protocol.md) - High-performance binary
- [gRPC](grpc.md) - Cross-language streaming
- [Shared Memory](shared-memory.md) - Ultra-low latency IPC
