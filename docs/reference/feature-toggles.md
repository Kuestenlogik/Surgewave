# Feature Toggles Reference

All `Surgewave:X:Enabled` feature flags. Flags are `false` (opt-in) unless noted.

Set via `appsettings.json`, environment variable (`Surgewave__X__Enabled=true`), or CLI argument (`--Surgewave:X:Enabled=true`).

---

## Feature Flag Table

| Toggle Key | Default | What It Enables |
|------------|---------|-----------------|
| `Surgewave:Kafka:Enabled` | **`true`** | Kafka wire protocol surface (listener acceptance, handler array, dispatcher) on the shared broker port. When `false` the broker runs **native-only** — Kafka clients are rejected, the native protocol still works. Kafka is Surgewave's optional compatibility layer. Note: multi-broker clustering currently uses the Kafka wire for its inter-broker control plane, so native-only is a single-broker mode for now (#60). |
| `Surgewave:SharedMemory:Enabled` | `false` | Shared memory transport for same-host clients. Sub-microsecond latency IPC. Requires matching client setting. |
| `Surgewave:SchemaRegistry:Enabled` | **`true`** | Confluent-compatible Schema Registry REST API at `/subjects`, `/schemas`, `/config`. |
| `Surgewave:SchemaRegistry:Inference:Enabled` | **`true`** | Automatic schema inference from topic messages (`GET /schemas/infer/{topic}`). |
| `Surgewave:SchemaEvolution:Enabled` | `false` | Schema evolution analysis, breaking-change detection, and migration code generation. |
| `Surgewave:SchemaMigration:Enabled` | `false` | Zero-downtime schema migration API (`POST /api/schema-migration/migrate`). |
| `Surgewave:SchemaLinking:Enabled` | `false` | Cross-cluster schema synchronization (schema linking). |
| `Surgewave:Connect:Enabled` | `false` | Kafka Connect framework, pipeline designer, connector REST API. Requires connector plugins in `PluginsDirectory`. |
| `Surgewave:Mqtt:Enabled` | `false` | MQTT 3.1.1 protocol adapter. Clients connect on port `1883`. MQTT topics map to Surgewave topics via `TopicPrefix`. |
| `Surgewave:WebSocket:Enabled` | `false` | WebSocket protocol adapter. Endpoints: `/ws/produce/{topic}`, `/ws/consume/{topic}`, `/ws/subscribe`. |
| `Surgewave:GraphQL:Enabled` | `false` | GraphQL API at `/graphql`. Includes Banana Cake Pop IDE. |
| `Surgewave:Wasm:Enabled` | `false` | WASM plugin subsystem. Sandboxed plugins run in Wasmtime. Supports hot-deploy. |
| `Surgewave:ML:Enabled` | `false` | ONNX ML scoring API (`/api/ml/models`). Loads `.onnx` files from `ModelsDirectory`. |
| `Surgewave:DataMesh:Enabled` | `false` | Data Mesh product registry with quality metrics, lineage, and contract validation. |
| `Surgewave:Privacy:Enabled` | `false` | Privacy-by-design: PII field encryption, right-to-erasure, compliance reports. |
| `Surgewave:MultiTenancy:Enabled` | `false` | Multi-tenant topic namespacing (`tenant/namespace/topic`). Tenant and namespace CRUD API. |
| `Surgewave:Amqp:Enabled` | `false` | AMQP 0-9-1 protocol adapter (port `5672`). RabbitMQ-compatible client support. |
| `Surgewave:TieredStorage:Enabled` | `false` | Tiered storage — offload cold segments to S3/Azure/GCP/local. |
| `Surgewave:Audit:Enabled` | `false` | Audit logging of produce, consume, authentication, and ACL events to an internal topic. |
| `Surgewave:Ttl:Enabled` | `false` | Per-message TTL via `surgewave-ttl-ms` header. Messages are filtered from fetch responses after expiry. |
| `Surgewave:Deduplication:Enabled` | `false` | Content-based deduplication using XxHash64. Detects duplicates without producer-side configuration. |
| `Surgewave:DelayDelivery:Enabled` | `false` | Delayed delivery via `surgewave-deliver-at-ms` or `surgewave-deliver-after-ms` headers. |
| `Surgewave:BrokerDlq:Enabled` | `false` | Broker-managed DLQ: automatic retry tracking and routing to DLQ topics on failure. |
| `Surgewave:BandwidthQuota:Enabled` | `false` | Per-client and per-user bandwidth throttling (produce + consume bytes/sec limits). |
| `Surgewave:AutoTuning:Enabled` | `false` | Auto-tuning rule engine that analyses metrics and recommends (or applies) configuration changes. |
| `Surgewave:CruiseControl:Enabled` | `false` | Cruise Control-compatible partition auto-balancing across brokers. |
| `Surgewave:CrossTopicTransactions:Enabled` | **`true`** | REST API for cross-topic transactions (`/api/transactions`). |
| `Surgewave:Security:SaslEnabled` | `false` | SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512). |
| `Surgewave:Security:TlsEnabled` | `false` | TLS encryption for client connections. |
| `Surgewave:Security:AclEnabled` | `false` | ACL-based authorization for topics, groups, and admin operations. |
| `Surgewave:Security:OAuth2:Enabled` | `false` | OAuth2/OIDC JWT token validation. |
| `Surgewave:UseRaftConsensus` | `false` | Raft-based controller election (replaces ISR-based leader election). |
| `Surgewave:GeoReplicationEnabled` | `false` | Broker-native geo-replication via cluster linking. |
| `Surgewave:ActiveReplicationEnabled` | `false` | Active-active multi-datacenter replication with conflict resolution. |
| `Surgewave:AutoRebalanceEnabled` | **`true`** | Automatic partition rebalancing when imbalance exceeds threshold. |
| `Surgewave:AllowAutoLeaderRebalance` | **`true`** | Automatic preferred leader election. |

---

## QueueView

QueueView has its own config section (`Surgewave:QueueView`):

| Toggle Key | Default | What It Enables |
|------------|---------|-----------------|
| `Surgewave:QueueView:Enabled` | `false` | QueueView queue semantics overlay: ack/nack/DLQ on top of the immutable log. Topics must be individually enrolled. |

---

## Streams (Application-level Flags)

These are set in `StreamsConfig` code, not in `appsettings.json`:

| Property | Default | What It Enables |
|----------|---------|-----------------|
| `StreamsConfig.EnableIdempotence` | `true` | Idempotent production in Streams (prevents duplicates). |
| `StreamsConfig.ProcessingGuarantee` | `AtLeastOnce` | Set to `ExactlyOnce` for transactional exactly-once semantics. |
| `StreamsConfig.OptimizeTopology` | `false` | Merge repartition nodes to reduce intermediate topics. |
| `StreamsRetryConfig.Enabled` | `false` | Retry processing on transient failures with backoff. |
| `BackpressureConfig.PauseConsumerOnHighWatermark` | `true` | Pause polling when buffer > 80% capacity. |
| `CachingConfig.Enabled` | `false` | Record caching for aggregations (reduces downstream updates). |
| `InteractiveQueryConfig.Enabled` | `false` | Embedded TCP query server for remote state store queries. |
| `ChangelogConfig.Enabled` | `true` | State store changelog replication to internal topics. |

---

## Tips

- **Start minimal**: only enable what you need. Each subsystem adds memory and CPU overhead.
- **Schema Registry is on by default** — disable with `Surgewave:SchemaRegistry:Enabled=false` if not needed.
- **Connect requires plugins**: collect DLLs with `.\scripts\collect-connectors.ps1` before enabling.
- **MultiTenancy + ACLs** are independent — you can use ACLs without multi-tenancy.
- **Shared memory transport** must be enabled on both broker and client (`SurgewaveTransportType.SharedMemory`).
