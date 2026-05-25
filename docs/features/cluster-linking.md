# Cluster Linking

Topic-level cross-cluster mirroring with offset translation for seamless failover.

## Overview

Cluster Linking creates managed, read-only mirror topics that continuously replicate from a source cluster. Unlike Geo-Replication (active-active with conflict resolution), Cluster Linking is unidirectional and designed for disaster recovery and data sharing. Mirror topics can be promoted to writable topics for planned failover, with consumers continuing at the same offset.

Key characteristics:

- **Read-only mirrors**: Mirror topics are read-only replicas of source topics
- **Offset translation**: Consumers can seamlessly fail over without offset confusion
- **Promote for failover**: Convert a mirror to a normal writable topic with one API call
- **Batched replication**: Configurable batch size and interval for throughput/latency tradeoff
- **State persistence**: Mirror offsets and link metadata survive broker restarts

## How It Works

1. Create a **cluster link** to a source cluster by providing its bootstrap servers.
2. Create **mirror topics** on the link, specifying source topic and optional local name.
3. A `TopicMirrorWorker` per mirror continuously fetches from the source and produces to the local topic.
4. `OffsetTranslation` maps source offsets to local offsets so consumers can switch clusters.
5. To fail over, **promote** the mirror topic -- replication stops and the topic becomes writable.

```mermaid
flowchart LR
    subgraph Source["Source Cluster"]
        SO["orders"]
        SP["payments"]
    end
    subgraph Target["Target Cluster"]
        TO["orders<br/>(read-only mirror)"]
        TP["payments<br/>(read-only mirror)"]
        TOW["orders<br/>(now writable)"]
    end

    SO -->|ClusterLink| TO
    SP -->|ClusterLink| TP
    TO -->|Promote (failover)| TOW
```

### Difference from Geo-Replication

| Aspect | Cluster Linking | Geo-Replication |
|--------|----------------|-----------------|
| Direction | Unidirectional | Bidirectional (active-active) |
| Mirror topics | Read-only until promoted | Both clusters accept writes |
| Conflict resolution | Not needed (one writer) | Required (last-write-wins, etc.) |
| Use case | DR failover, data sharing | Multi-region active-active |

## Configuration

```json
{
  "Surgewave": {
    "ClusterLinking": {
      "Enabled": true,
      "ReplicationIntervalMs": 100,
      "BatchSize": 500,
      "StateFile": "cluster-linking-state.json",
      "Links": []
    }
  }
}
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `Enabled` | bool | `false` | Enable cluster linking |
| `ReplicationIntervalMs` | int | `100` | Milliseconds between replication cycles |
| `BatchSize` | int | `500` | Messages per replication batch |
| `StateFile` | string | `cluster-linking-state.json` | Path for persisted state |

## REST API

### Cluster Link Endpoints (`/api/cluster-links`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/cluster-links/` | Create a cluster link |
| `GET` | `/api/cluster-links/` | List all cluster links |
| `GET` | `/api/cluster-links/{id}` | Get link status with mirrors |
| `PUT` | `/api/cluster-links/{id}/pause` | Pause a link |
| `PUT` | `/api/cluster-links/{id}/resume` | Resume a paused link |
| `DELETE` | `/api/cluster-links/{id}` | Delete link and stop mirrors |
| `POST` | `/api/cluster-links/{id}/mirrors` | Create a mirror topic |
| `GET` | `/api/cluster-links/{id}/mirrors` | List mirrors for a link |

### Mirror Endpoints (`/api/mirrors`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/mirrors/` | List all mirror topics |
| `GET` | `/api/mirrors/{topic}` | Get mirror details |
| `POST` | `/api/mirrors/{topic}/stop` | Stop mirroring |
| `POST` | `/api/mirrors/{topic}/promote` | Promote to writable (failover) |
| `GET` | `/api/mirrors/{topic}/lag` | Get replication lag |

### Create a Link and Mirror

```bash
# Create a cluster link
curl -X POST http://localhost:9092/api/cluster-links/ \
  -H "Content-Type: application/json" \
  -d '{
    "linkId": "dc-west",
    "sourceClusterId": "west-1",
    "sourceBootstrapServers": "west-surgewave:9092"
  }'

# Create a mirror topic
curl -X POST http://localhost:9092/api/cluster-links/dc-west/mirrors \
  -H "Content-Type: application/json" \
  -d '{"sourceTopic": "orders", "localTopicName": "orders"}'
```

### Promote for Failover

```bash
curl -X POST http://localhost:9092/api/mirrors/orders/promote
```

Response:

```json
{
  "topicName": "orders",
  "success": true,
  "finalOffset": 150342,
  "errorMessage": null
}
```

### Check Replication Lag

```bash
curl http://localhost:9092/api/mirrors/orders/lag
```

## Use Cases

- **Disaster recovery**: Mirror critical topics to a standby cluster, promote on failure
- **Data sharing**: Share topics across clusters without giving write access
- **Migration**: Mirror topics to a new cluster, then promote and cut over
- **Analytics isolation**: Mirror production topics to an analytics cluster

## Next Steps

- [Schema Linking](schema-linking.md) - Cross-cluster schema synchronization
- [Cross-Topic Transactions](cross-topic-transactions.md) - Atomic multi-topic writes
- [Transactions](transactions.md) - Single-topic exactly-once semantics
