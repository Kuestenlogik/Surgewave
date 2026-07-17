# Configuration Reference

All configuration keys in alphabetical order by section. Set via `appsettings.json`, environment variables, or CLI arguments.

Environment variable format: replace `:` with `__` (e.g., `Surgewave__Port=9092`).
CLI argument format: `--Surgewave:Port=9092`.

---

## Surgewave (Broker Core)

Section: `Surgewave`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `BrokerId` | int | `0` | Unique broker ID in a cluster |
| `Host` | string | `localhost` | Bind address |
| `Port` | int | `9092` | Kafka protocol port |
| `GrpcPort` | int | `9099` | gRPC API port |
| `DataDirectory` | string | `./data` | Log segment storage path |
| `LogDirectory` | string | `./logs` | Application log path |
| `StorageMode` | enum | `File` | `File`, `Memory`, `Arrow`, `ArrowNoCompression`, `ArrowMmap`, `ArrowHighCompression`, `ArrowLowLatency`, `ZeroCopyWal`, `ZeroCopyMemory`, `ObjectStore` |
| `AutoCreateTopics` | bool | `true` | Create topics on first produce |
| `DefaultNumPartitions` | int | `1` | Default partition count for new topics |
| `DefaultReplicationFactor` | short | `1` | Default replication factor |
| `LogRetentionHours` | int | `168` | Retention period (hours) |
| `LogRetentionBytes` | long | `-1` | Retention size per partition (-1 = unlimited) |
| `LogSegmentBytes` | long | `1073741824` | Max segment size (1 GB) |
| `MaxConnectionsPerIp` | int | `100` | Max concurrent connections per IP |
| `MaxRequestSize` | int | `1048576` | Max message size (1 MB) |
| `SocketSendBufferBytes` | int | `102400` | Socket send buffer, applied to accepted data-port sockets (new connections) |
| `SocketReceiveBufferBytes` | int | `102400` | Socket receive buffer, applied to accepted data-port sockets (new connections) |
| `EnableDualMode` | bool | `true` | IPv4+IPv6 dual-stack binding |
| `ProducerBatchSizeBytes` | int | `16384` | Default batch size advertised to clients |
| `ProducerLingerMs` | int | `5` | Default linger time advertised to clients |
| `ProducerMaxBatchMessages` | int | `10000` | Max messages per batch |
| `NativeProtocolCompressionEnabled` | bool | `true` | Enable compression on native protocol |
| `NativeProtocolPipelineDepth` | int | `16` | Native protocol request pipeline depth |
| `KafkaPipelineDepth` | int | `8` | Kafka protocol request pipeline depth |
| `MaxStreamingSubscriptionsPerConnection` | int | `100` | Max push subscriptions per native connection |
| `SimdBatchThreshold` | int | `4` | Min batch size for SIMD path (-1=off, 0=always) |
| `UseChannelPipeline` | bool | `true` | Use channel-based write pipeline |
| `ChannelWriteWorkers` | int | `0` | Write workers (0 = auto: 2x CPU, min 8) |
| `ChannelReadWorkers` | int | `8` | Read workers |
| `ChannelWriteBufferSize` | int | `10000` | Channel buffer capacity |
| `ChannelWriteBatchSize` | int | `100` | Messages per channel batch |
| `ChannelWriteBatchDelayMs` | int | `10` | Max wait to fill a channel batch |
| `ShutdownTimeoutSeconds` | int | `30` | Graceful shutdown timeout |
| `ClusterNodes` | string | `""` | Comma-separated broker endpoints for clustering |
| `Rack` | string | `null` | Rack identifier for rack-aware replication |
| `ClusterId` | string | `null` | Override cluster ID |
| `AllowAutoLeaderRebalance` | bool | `true` | Automatic preferred leader election |
| `LeaderImbalanceCheckIntervalSeconds` | int | `300` | Leader imbalance check interval |
| `ControlledShutdownMaxRetries` | int | `3` | Max retries for controlled shutdown |
| `HeartbeatIntervalMs` | int | `3000` | Heartbeat interval |
| `HeartbeatTimeoutMs` | int | `10000` | Heartbeat timeout |
| `MaxHeartbeatFailures` | int | `3` | Failures before broker marked dead |
| `UseRaftConsensus` | bool | `false` | Use Raft for controller election |
| `RaftDataDirectory` | string | `./data/raft` | Raft log directory |
| `RaftElectionTimeoutMinMs` | int | `150` | Raft election timeout minimum |
| `RaftElectionTimeoutMaxMs` | int | `300` | Raft election timeout maximum |
| `RaftHeartbeatIntervalMs` | int | `75` | Raft leader heartbeat interval |
| `AutoRebalanceEnabled` | bool | `true` | Automatic partition rebalancing |
| `RebalanceCheckIntervalSeconds` | int | `300` | Rebalance check interval |
| `RebalanceImbalanceThreshold` | double | `0.1` | Imbalance ratio to trigger rebalance |
| `ReassignmentThrottleBytesPerSec` | long | `50000000` | Reassignment throttle (50 MB/s) |
| `ReassignmentMaxConcurrent` | int | `5` | Max concurrent reassignments |
| `GeoReplicationEnabled` | bool | `false` | Enable geo-replication (cluster linking) |
| `ActiveReplicationEnabled` | bool | `false` | Enable active-active multi-DC replication |

### Replication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ReplicationPort` | int | `9094` | Inter-broker replication port |
| `MinInSyncReplicas` | int | `1` | Minimum ISR for acks=all |
| `ReplicaLagTimeMaxMs` | int | `10000` | Max time for replica to be considered in-sync |
| `ReplicaLagMaxMessages` | long | `10000` | Max message lag before replica removed from ISR |
| `ReplicaFetchMaxBytes` | int | `1048576` | Max bytes per fetch from leader |
| `ReplicaFetchWaitMaxMs` | int | `500` | Max wait per fetch |

---

## Surgewave:Gc

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `LatencyMode` | string | `SustainedLowLatency` | GC latency mode: `Interactive`, `LowLatency`, `SustainedLowLatency`, `NoGCRegion` |
| `CompactLargeObjectHeap` | bool | `false` | Compact LOH on full GC |
| `ForceGcAfterMb` | int | `0` | Force GC after N MB allocated (0 = disabled) |

---

## Surgewave:Transactions

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `DefaultTimeoutMs` | int | `60000` | Default transaction timeout (1 minute) |
| `MaxTimeoutMs` | int | `900000` | Maximum transaction timeout (15 minutes) |
| `MinTimeoutMs` | int | `1000` | Minimum transaction timeout |
| `TimeoutCheckIntervalMs` | int | `5000` | How often to check for timed-out transactions |
| `CompletedRetentionHours` | int | `168` | Retention for completed transactions (7 days) |
| `CompactionIntervalHours` | int | `1` | Compaction interval for transaction log |
| `EnablePersistence` | bool | `true` | Persist transaction state to disk |

---

## Surgewave:Quotas

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable quota enforcement |
| `DefaultProducerBytesPerSec` | long | `0` | Default producer quota (0 = unlimited) |
| `DefaultConsumerBytesPerSec` | long | `0` | Default consumer quota (0 = unlimited) |

---

## Surgewave:BandwidthQuota

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable bandwidth quota enforcement |
| `DefaultProduceBytesPerSec` | long | `0` | Default produce limit (0 = unlimited) |
| `DefaultConsumeBytesPerSec` | long | `0` | Default consume limit (0 = unlimited) |
| `EnforcementWindowMs` | int | `1000` | Sliding window for rate measurement |
| `ThrottleDelayFactor` | double | `1.5` | Multiplier for throttle delay calculation |

---

## Surgewave:Security

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `SaslEnabled` | bool | `false` | Enable SASL authentication |
| `SaslMechanisms` | string[] | `["PLAIN"]` | Enabled mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `CredentialsFile` | string | `null` | Path to credentials file |
| `Users` | string[] | `[]` | Inline users: `"username:password"` |
| `AllowAnonymous` | bool | `false` | Allow unauthenticated connections |
| `TlsEnabled` | bool | `false` | Enable TLS |
| `CertificatePath` | string | `null` | Path to PFX/PKCS12 certificate |
| `CertificatePassword` | string | `null` | Certificate password |
| `RequireClientCertificate` | bool | `false` | Require mTLS client certificate |
| `TrustedCaCertificatePath` | string | `null` | CA certificate for client validation |
| `MinTlsVersion` | string | `TLS12` | Minimum TLS version: `TLS12`, `TLS13` |
| `AclEnabled` | bool | `false` | Enable ACL-based authorization |
| `AclFile` | string | `null` | Path to ACL file |
| `SuperUsers` | string[] | `[]` | Users that bypass ACL checks (format: `User:admin`) |
| `AllowIfNoAclFound` | bool | `false` | Default when no ACL matches (false = deny) |

### Surgewave:Security:OAuth2

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable OAuth2/OIDC authentication |
| `Issuer` | string | `null` | OIDC issuer URL |
| `JwksUri` | string | `null` | JWKS endpoint (discovered from issuer if null) |
| `Audience` | string | `null` | Expected audience claim |
| `UsernameClaim` | string | `preferred_username` | Claim containing username |
| `GroupsClaim` | string | `groups` | Claim containing roles/groups |
| `ClockSkewMinutes` | int | `5` | Token clock skew tolerance |
| `JwksCacheHours` | int | `1` | JWKS cache duration |
| `RequireHttpsMetadata` | bool | `true` | Require HTTPS for OIDC metadata |
| `AllowedAlgorithms` | string[] | `["RS256","RS384","RS512","ES256","ES384","ES512"]` | Allowed signing algorithms |

---

## Surgewave:SchemaRegistry

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `true` | Enable the Confluent-compatible Schema Registry |
| `DataPath` | string | `./data/schemas` | Schema storage path |
| `DefaultCompatibility` | string | `Backward` | Default compatibility: `None`, `Backward`, `BackwardTransitive`, `Forward`, `ForwardTransitive`, `Full`, `FullTransitive` |

---

## Surgewave:SchemaEvolution

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable automatic schema evolution analysis |

---

## Surgewave:SchemaMigration

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable zero-downtime schema migration |

---

## Surgewave:SchemaLinking

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable cross-cluster schema synchronization |

---

## Surgewave:TieredStorage

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable tiered storage |
| `Provider` | string | `local` | Storage provider: `local`, `azure`, `s3`, `gcp` |
| `LocalPath` | string | `./tiered-storage` | Local provider path |
| `AzureConnectionString` | string | `null` | Azure Storage connection string |
| `AzureContainerName` | string | `surgewave-tiered` | Azure Blob container name |
| `S3BucketName` | string | `null` | S3 bucket name |
| `S3Region` | string | `null` | S3 region |
| `GcpBucketName` | string | `null` | GCP bucket name |
| `Prefix` | string | `""` | Object key prefix |
| `LocalRetentionHours` | int | `24` | Hours to keep segments locally after upload |
| `RemoteRetentionHours` | int | `-1` | Hours to retain in remote storage (-1 = forever) |
| `TieringLagHours` | int | `1` | Minimum segment age before tiering |
| `MinSegmentSizeBytes` | long | `1048576` | Minimum segment size to tier (1 MB) |
| `LocalCacheSizeBytes` | long | `1073741824` | Local read cache size (1 GB) |
| `LocalCachePath` | string | `./tiered-cache` | Cache directory for downloaded segments |
| `DeleteAfterUpload` | bool | `true` | Delete local segment after successful upload |
| `TieringIntervalSeconds` | int | `300` | Background tiering check interval |

---

## Surgewave:Connect

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable Kafka Connect framework |
| `GroupId` | string | `surgewave-connect` | Consumer group for offset management |
| `ConfigTopic` | string | `surgewave-connect-configs` | Connector configuration topic |
| `OffsetsTopic` | string | `surgewave-connect-offsets` | Connector offsets topic |
| `StatusTopic` | string | `surgewave-connect-status` | Connector status topic |
| `PluginsDirectory` | string | `plugins` | Directory to scan for connector DLLs |

---

## Surgewave:SharedMemory

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable shared memory transport for same-host clients |
| `BasePath` | string | `null` | Base path for shared memory files (auto-detected) |
| `RingBufferCapacity` | int | `16777216` | Ring buffer size in bytes (must be power of 2, default 16 MB) |
| `PollingStrategy` | string | `Adaptive` | `BusySpin`, `Sleep`, `Adaptive` |
| `SpinCount` | int | `100` | Spin iterations before yielding |
| `IdleSleepMicroseconds` | int | `1` | Sleep interval when idle (microseconds) |
| `MaxClients` | int | `100` | Max concurrent shared memory clients (0 = unlimited) |
| `ClientScanIntervalMs` | int | `100` | Interval to scan for new client connections |

---

## Surgewave:Audit

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable audit logging |
| `Partitions` | int | `1` | Audit log topic partition count |
| `ReplicationFactor` | short | `1` | Audit log topic replication factor |
| `RetentionMs` | long | `604800000` | Audit log retention (7 days) |
| `ExcludeInternalTopics` | bool | `true` | Exclude `__` topics from audit |
| `LogSuccessfulAuthentication` | bool | `false` | Log successful logins |
| `LogAuthorizationChecks` | bool | `false` | Log all ACL checks (verbose) |

---

## Surgewave:Ttl

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable per-message TTL via `surgewave-ttl-ms` header |
| `DefaultTtlMs` | long | `0` | Default TTL (0 = no default, messages live forever) |
| `MaxTtlMs` | long | `604800000` | Maximum allowed TTL (7 days) |
| `IndexCleanupIntervalMs` | int | `1000` | TTL index sweep interval |

---

## Surgewave:Deduplication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable content-based deduplication (XxHash64) |
| `MaxEntriesPerPartition` | int | `10000` | Max hash entries per partition |
| `WindowSizeMs` | long | `300000` | Deduplication window size (5 minutes) |
| `CleanupIntervalMs` | int | `10000` | Cleanup interval for expired entries |

---

## Surgewave:DelayDelivery

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable delayed delivery via `surgewave-deliver-at-ms` / `surgewave-deliver-after-ms` headers |
| `MaxDelayMs` | long | `604800000` | Maximum allowed delay (7 days) |
| `IndexCleanupIntervalMs` | int | `1000` | Delay index sweep interval |

---

## Surgewave:BrokerDlq

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable broker-level DLQ management |
| `MaxRetries` | int | `3` | Max retry attempts before DLQ routing |
| `RetryBackoffMs` | long | `1000` | Delay between retries |
| `TopicSuffix` | string | `.DLQ` | Suffix appended to source topic to form DLQ topic name |
| `CleanupIntervalMs` | int | `60000` | Retry tracking cleanup interval |
| `EntryMaxAgeMs` | long | `300000` | Max age for retry tracking entries (5 minutes) |

---

## Surgewave:Mqtt

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable MQTT protocol adapter |
| `Port` | int | `1883` | MQTT TCP port |
| `TopicPrefix` | string | `mqtt.` | Prefix for Surgewave topic names (MQTT `/` becomes `.`) |
| `MaxClients` | int | `1000` | Max concurrent MQTT connections |
| `MaxMessageSizeBytes` | int | `262144` | Max payload size (256 KB) |
| `AllowAnonymous` | bool | `true` | Allow unauthenticated MQTT connections |
| `KeepAliveSeconds` | int | `60` | MQTT keep-alive interval |

---

## Surgewave:WebSocket

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable WebSocket protocol adapter |
| `Path` | string | `/ws` | Base URL path for WebSocket endpoints |
| `MaxMessageSizeBytes` | int | `1048576` | Max WebSocket message size (1 MB) |
| `PingInterval` | TimeSpan | `00:00:30` | WebSocket ping interval |
| `MaxConnections` | int | `5000` | Max concurrent WebSocket connections |

---

## Surgewave:GraphQL

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable GraphQL API |
| `Path` | string | `/graphql` | HTTP path for GraphQL endpoint |

---

## Surgewave:ML

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable ONNX ML scoring subsystem |
| `ModelsDirectory` | string | `models` | Directory to scan for `.onnx` model files |
| `MaxModelsLoaded` | int | `10` | Max simultaneous loaded models |
| `UseGpu` | bool | `false` | Use CUDA GPU acceleration |

---

## Surgewave:Wasm

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable WASM plugin subsystem |
| `WasmDirectory` | string | `wasm-plugins` | Directory for WASM plugin subdirectories |
| `MaxMemoryBytes` | long | `67108864` | Max linear memory per WASM module (64 MB) |
| `ExecutionTimeout` | TimeSpan | `00:00:30` | Max duration for a single WASM function call |
| `AllowFileAccess` | bool | `false` | Allow WASM file system access via WASI |
| `AllowNetworkAccess` | bool | `false` | Allow WASM outbound network via WASI |
| `EnableHotDeploy` | bool | `true` | Auto-reload on `.wasm` file changes |
| `HotDeployDebounce` | TimeSpan | `00:00:02` | Debounce interval for hot-deploy events |

---

## Surgewave:DataMesh

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable Data Mesh subsystem |
| `QualityCheckIntervalSeconds` | int | `300` | Quality measurement interval |
| `QualitySampleSize` | int | `100` | Messages to sample for quality measurement |
| `InternalTopicName` | string | `__data_mesh_products` | Compacted internal topic for product state |

---

## Surgewave:Privacy

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable privacy-by-design subsystem |
| `AuditLoggingEnabled` | bool | `true` | Enable privacy audit events |
| `AuditTopic` | string | `__privacy_audit` | Internal audit events topic |
| `DefaultPiiScanEnabled` | bool | `false` | Scan all topics for PII by default |

---

## Surgewave:MultiTenancy

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable multi-tenancy subsystem |
| `TopicSeparator` | string | `/` | Separator for `tenant/namespace/topic` naming |
| `RequireNamespace` | bool | `false` | Require namespaced topic names for all topics |
| `InternalTopicName` | string | `__tenants` | Compacted internal topic for tenant state |

---

## Surgewave:AutoTuning

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable auto-tuning recommendations |
| `Mode` | enum | `SuggestOnly` | `SuggestOnly` (recommend only) or `AutoApply` (apply automatically) |
| `AnalysisIntervalSeconds` | int | `30` | Analysis run interval |
| `DisabledRules` | string[] | `[]` | Rule IDs to exclude from analysis |

---

## Surgewave:CruiseControl

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `false` | Enable Cruise Control auto-balancing |

---

## Surgewave:CrossTopicTransactions

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `Enabled` | bool | `true` | Enable cross-topic REST transaction API |
