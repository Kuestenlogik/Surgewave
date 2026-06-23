using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Api.GraphQL;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Core.Storage;
// Enterprise plugin: Kuestenlogik.Surgewave.Replication
using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Kuestenlogik.Surgewave.Wasm;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Broker configuration - bound from appsettings.json "Surgewave" section
/// </summary>
public sealed class BrokerConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json
    /// </summary>
    public const string SectionName = "Surgewave";

    [Range(0, int.MaxValue)]
    public int BrokerId { get; set; } = 0;

    [Required]
    [MinLength(1)]
    public string Host { get; set; } = "localhost";

    [Range(1, 65535)]
    public int Port { get; set; } = KafkaConstants.Ports.Kafka;

    [Range(1, 65535)]
    public int GrpcPort { get; set; } = KafkaConstants.Ports.Grpc;

    /// <summary>
    /// Whether the gRPC / REST endpoint on <see cref="GrpcPort"/> binds over HTTPS instead of
    /// cleartext HTTP. When <c>true</c>, broker startup overrides
    /// <c>Kestrel:Endpoints:Grpc:Url</c> to <c>https://*:{GrpcPort}</c> and, if
    /// <see cref="GrpcCertificatePath"/> is set, configures the cert path and password for the
    /// endpoint. When unset, falls back to the ASP.NET Core development certificate
    /// (<c>dotnet dev-certs https --trust</c>). Default <c>false</c> keeps the existing cleartext
    /// binding so the toggle is additive.
    /// </summary>
    public bool GrpcUseTls { get; set; }

    /// <summary>
    /// Path to a PFX / PEM certificate file used for the HTTPS binding when
    /// <see cref="GrpcUseTls"/> is <c>true</c>. Leave unset in development to use the .NET
    /// dev cert. Required for production deployments that need a trusted certificate chain.
    /// </summary>
    public string? GrpcCertificatePath { get; set; }

    /// <summary>Password for <see cref="GrpcCertificatePath"/>, if any.</summary>
    public string? GrpcCertificatePassword { get; set; }

    [Required]
    [MinLength(1)]
    public string DataDirectory { get; set; } = "./data";

    [Required]
    [MinLength(1)]
    public string LogDirectory { get; set; } = "./logs";

    // Network settings
    [Range(1, int.MaxValue)]
    public int MaxConnectionsPerIp { get; set; } = 100;

    [Range(1024, int.MaxValue)]
    public int SocketSendBufferBytes { get; set; } = KafkaConstants.BufferSizes.SocketDefault;

    [Range(1024, int.MaxValue)]
    public int SocketReceiveBufferBytes { get; set; } = KafkaConstants.BufferSizes.SocketDefault;

    [Range(1024, int.MaxValue)]
    public int MaxRequestSize { get; set; } = KafkaConstants.BufferSizes.MaxRequest;

    /// <summary>
    /// Enable dual-stack socket binding (IPv4 + IPv6).
    /// When true and Host is "localhost", binds to IPv6Any with DualMode enabled.
    /// When false, binds to IPv4 only (127.0.0.1 for localhost).
    /// Defaults to true. Set to false for environments without IPv6 support.
    /// </summary>
    public bool EnableDualMode { get; set; } = true;

    // Log settings
    [Range(1024, long.MaxValue)]
    public long LogSegmentBytes { get; set; } = KafkaConstants.Defaults.MaxSegmentSize;

    [Range(1, int.MaxValue)]
    public int LogRetentionHours { get; set; } = KafkaConstants.Retention.DefaultHours;

    public long LogRetentionBytes { get; set; } = KafkaConstants.Retention.Unlimited;

    /// <summary>
    /// Storage engine name: "file" (persistent disk storage), "memory" (in-memory, ephemeral), etc.
    /// See <see cref="Kuestenlogik.Surgewave.Core.Storage.StorageEngines"/> for well-known engine names.
    /// </summary>
    public string StorageEngine { get; set; } = StorageEngines.File;

    // Topic defaults
    public bool AutoCreateTopics { get; set; } = true;

    [Range(1, short.MaxValue)]
    public short DefaultReplicationFactor { get; set; } = 1;

    [Range(1, int.MaxValue)]
    public int DefaultNumPartitions { get; set; } = 1;

    // Producer defaults (sent to clients via metadata)
    /// <summary>
    /// Default producer batch size in bytes (Kafka-compatible).
    /// Clients can override. Default: 16384 (16KB, Kafka default)
    /// </summary>
    public int ProducerBatchSizeBytes { get; set; } = 16384;

    /// <summary>
    /// Default producer linger time in milliseconds.
    /// How long to wait for more messages before sending a batch.
    /// Default: 5ms (slightly higher than Kafka's 0 for better throughput)
    /// </summary>
    public int ProducerLingerMs { get; set; } = 5;

    /// <summary>
    /// Maximum producer batch size in messages.
    /// Default: 10000 (limits memory usage per batch)
    /// </summary>
    public int ProducerMaxBatchMessages { get; set; } = 10000;

    // Native protocol settings
    public bool NativeProtocolCompressionEnabled { get; set; } = true;

    /// <summary>
    /// Maximum number of active push subscriptions per native protocol connection.
    /// Default: 100.
    /// </summary>
    public int MaxStreamingSubscriptionsPerConnection { get; set; } = 100;

    /// <summary>
    /// Pipeline depth for native protocol request pipelining.
    /// Allows reading ahead while processing the current request.
    /// Higher values trade memory for throughput on busy connections.
    /// </summary>
    public int NativeProtocolPipelineDepth { get; set; } = 16;

    /// <summary>
    /// Pipeline depth for Kafka protocol request pipelining.
    /// Allows reading ahead while processing the current request.
    /// Sequential processing maintains partition ordering guarantees.
    /// </summary>
    public int KafkaPipelineDepth { get; set; } = 8;

    // SIMD batch threshold: -1 = disabled (scalar), 0 = auto (always SIMD), >0 = min batch size
    public int SimdBatchThreshold { get; set; } = 4;

    // Channel pipeline settings (for high-throughput mode)
    public bool UseChannelPipeline { get; set; } = true;
    public int ChannelWriteWorkers { get; set; } = 0; // 0 = auto (2x processor count, min 8)
    public int ChannelReadWorkers { get; set; } = 8;
    public int ChannelWriteBufferSize { get; set; } = 10000;
    public int ChannelWriteBatchSize { get; set; } = 100; // Batch size for high throughput
    public int ChannelWriteBatchDelayMs { get; set; } = 10;

    // Shutdown settings
    public int ShutdownTimeoutSeconds { get; set; } = KafkaConstants.Timeouts.ShutdownTimeoutSeconds;

    // Replication settings
    [Range(1, 65535)]
    public int ReplicationPort { get; set; } = KafkaConstants.Ports.Replication;

    /// <summary>
    /// Transport protocol for inter-broker traffic (Raft RPC, partition
    /// replication, geo-replication). Allowed: "tcp" (default) or "quic".
    /// QUIC requires msquic on all brokers.
    /// </summary>
    [RegularExpression("^(tcp|quic)$",
        ErrorMessage = "InterBrokerTransport must be 'tcp' or 'quic'.")]
    public string InterBrokerTransport { get; set; } = "tcp";

    /// <summary>
    /// Path to this broker's PKCS#12 (.pfx) certificate for QUIC peer mTLS.
    /// Must be signed by <see cref="InterBrokerCaCertificatePath"/>. When unset,
    /// Surgewave generates a self-signed dev cert (requires TrustAllCertificates).
    /// </summary>
    public string InterBrokerCertificatePath { get; set; } = "";

    /// <summary>Password for the PKCS#12 broker certificate.</summary>
    public string InterBrokerCertificatePassword { get; set; } = "";

    /// <summary>
    /// Path to the shared cluster CA certificate that every broker trusts.
    /// Setting this together with <see cref="InterBrokerCertificatePath"/>
    /// enables mutual TLS on QUIC peer connections.
    /// </summary>
    public string InterBrokerCaCertificatePath { get; set; } = "";

    [Range(1, int.MaxValue)]
    public int MinInSyncReplicas { get; set; } = 1;
    public int ReplicaLagTimeMaxMs { get; set; } = KafkaConstants.Timeouts.ReplicaLagTimeMaxMs;
    public long ReplicaLagMaxMessages { get; set; } = 10000;
    public int ReplicaFetchMaxBytes { get; set; } = KafkaConstants.BufferSizes.FetchDefault;
    public int ReplicaFetchWaitMaxMs { get; set; } = KafkaConstants.Timeouts.FetchMaxWaitMs;

    // Cluster settings (comma-separated list of broker endpoints)
    public string ClusterNodes { get; set; } = "";
    public string? Rack { get; set; }
    public string? ClusterId { get; set; }

    // Controller settings
    public bool AllowAutoLeaderRebalance { get; set; } = true;
    public int LeaderImbalanceCheckIntervalSeconds { get; set; } = KafkaConstants.Timeouts.LeaderImbalanceCheckSeconds;
    public int ControlledShutdownMaxRetries { get; set; } = 3;

    // Heartbeat settings (Phase 1: Failure Detection)
    [Range(1, int.MaxValue)]
    public int HeartbeatIntervalMs { get; set; } = KafkaConstants.Timeouts.HeartbeatIntervalMs;

    [Range(1, int.MaxValue)]
    public int HeartbeatTimeoutMs { get; set; } = KafkaConstants.Timeouts.HeartbeatTimeoutMs;

    [Range(1, int.MaxValue)]
    public int MaxHeartbeatFailures { get; set; } = 3;

    // Raft consensus settings (Phase 2: Controller Election)
    public bool UseRaftConsensus { get; set; } = false;
    public string RaftDataDirectory { get; set; } = "./data/raft";
    public int RaftElectionTimeoutMinMs { get; set; } = KafkaConstants.Raft.ElectionTimeoutMinMs;
    public int RaftElectionTimeoutMaxMs { get; set; } = KafkaConstants.Raft.ElectionTimeoutMaxMs;
    public int RaftHeartbeatIntervalMs { get; set; } = KafkaConstants.Raft.HeartbeatIntervalMs;
    public int RaftPeerDiscoveryTimeoutSeconds { get; set; } = 30;

    // Partition reassignment settings (Phase 4)
    public long ReassignmentThrottleBytesPerSec { get; set; } = 50_000_000; // 50 MB/s
    public int ReassignmentMaxConcurrent { get; set; } = 5;

    // Auto-rebalancing settings (Phase 5)
    public bool AutoRebalanceEnabled { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int RebalanceCheckIntervalSeconds { get; set; } = 300;

    [Range(0.0, 1.0)]
    public double RebalanceImbalanceThreshold { get; set; } = 0.1; // 10%

    // Transaction settings
    public TransactionConfig Transactions { get; set; } = new();

    // Quota settings
    public QuotaConfig Quotas { get; set; } = new();

    // Delegation token settings
    public DelegationTokenConfig DelegationTokens { get; set; } = new();

    // Security settings
    public SecurityConfig Security { get; set; } = new();

    // Tiered storage settings
    public TieredStorageConfig TieredStorage { get; set; } = new();

    // Schema Registry settings
    public SchemaRegistryConfig SchemaRegistry { get; set; } = new();

    // Shared memory transport settings
    public SharedMemoryBrokerConfig SharedMemory { get; set; } = new();

    // Kafka Connect settings
    public ConnectConfig Connect { get; set; } = new();

    // GC/Performance settings
    public GcConfig Gc { get; set; } = new();

    // Audit logging settings
    public AuditConfig Audit { get; set; } = new();

    // Client telemetry settings (KIP-714).
    public ClientTelemetryConfig Telemetry { get; set; } = new();

    // Data integrity settings
    public DataIntegrityConfig DataIntegrity { get; set; } = new();

    // Delayed delivery settings
    public DeliveryDelayConfig DelayDelivery { get; set; } = new();

    // TTL settings
    public TtlConfig Ttl { get; set; } = new();

    // Broker-level DLQ settings
    public DlqManagerConfig BrokerDlq { get; set; } = new();

    // Deduplication settings
    public DeduplicationConfig Deduplication { get; set; } = new();

    // GraphQL API settings
    public GraphQLConfig GraphQL { get; set; } = new();

    // WASM plugin settings
    public WasmPluginConfig Wasm { get; set; } = new();

    // Bandwidth quota settings (per-client/user bandwidth throttling)
    public BandwidthQuotaConfig BandwidthQuota { get; set; } = new();

    // Schema evolution settings
    public SchemaEvolutionConfig SchemaEvolution { get; set; } = new();

    // Schema migration settings (zero-downtime schema migration)
    public SchemaMigrationConfig SchemaMigration { get; set; } = new();

    // Schema linking settings (cross-cluster schema synchronization)
    public SchemaLinkingConfig SchemaLinking { get; set; } = new();

    // Cross-topic transaction settings
    public Transactions.CrossTopicTransactionConfig CrossTopicTransactions { get; set; } = new();

    // Auto-tuning settings
    public AutoTuning.AutoTuningConfig AutoTuning { get; set; } = new();

    // Cruise Control (auto-balance) settings
    public CruiseControl.CruiseControlConfig CruiseControl { get; set; } = new();

    // Geo-replication settings

    /// <summary>
    /// Enable broker-native geo-replication (cluster linking).
    /// </summary>
    public bool GeoReplicationEnabled { get; set; } = false;

    /// <summary>
    /// Cluster link configurations for geo-replication.
    /// </summary>
    public ClusterLinkConfig[]? ClusterLinks { get; set; }

    // Active-active multi-DC replication settings

    /// <summary>
    /// Enable active-active multi-datacenter replication with conflict resolution.
    /// This is separate from one-way geo-replication (cluster linking) above.
    /// </summary>
    public bool ActiveReplicationEnabled { get; set; } = false;

    // Enterprise plugin: Kuestenlogik.Surgewave.Replication
    // /// <summary>
    // /// Configuration for active-active multi-DC replication.
    // /// Only used when <see cref="ActiveReplicationEnabled"/> is <c>true</c>.
    // /// </summary>
    // public Replication.GeoReplicationConfig? ActiveReplication { get; set; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // Cross-property: all broker-exposed ports must be distinct
        if (Port == GrpcPort)
            errors.Add($"{nameof(Port)} and {nameof(GrpcPort)} must be different (both {Port}).");
        if (Port == ReplicationPort)
            errors.Add($"{nameof(Port)} and {nameof(ReplicationPort)} must be different (both {Port}).");
        if (GrpcPort == ReplicationPort)
            errors.Add($"{nameof(GrpcPort)} and {nameof(ReplicationPort)} must be different (both {GrpcPort}).");

        // TLS cert configuration: when a path is supplied, the file must exist. A missing
        // cert path with GrpcUseTls=true is allowed (falls back to the .NET dev cert),
        // but a path that points at nothing is almost certainly a config mistake.
        if (!string.IsNullOrEmpty(GrpcCertificatePath) && !System.IO.File.Exists(GrpcCertificatePath))
            errors.Add($"{nameof(GrpcCertificatePath)} '{GrpcCertificatePath}' does not exist.");
        if (!GrpcUseTls && !string.IsNullOrEmpty(GrpcCertificatePath))
            errors.Add($"{nameof(GrpcCertificatePath)} is set but {nameof(GrpcUseTls)} is false; either enable TLS or unset the cert path.");

        // Heartbeat timeout must leave room for at least one heartbeat interval
        if (HeartbeatTimeoutMs <= HeartbeatIntervalMs)
        {
            errors.Add($"{nameof(HeartbeatTimeoutMs)} must be greater than " +
                       $"{nameof(HeartbeatIntervalMs)} ({HeartbeatIntervalMs}ms).");
        }

        // Raft election timeout range must be valid
        if (UseRaftConsensus && RaftElectionTimeoutMinMs >= RaftElectionTimeoutMaxMs)
        {
            errors.Add($"{nameof(RaftElectionTimeoutMinMs)} ({RaftElectionTimeoutMinMs}ms) must be " +
                       $"strictly less than {nameof(RaftElectionTimeoutMaxMs)} ({RaftElectionTimeoutMaxMs}ms).");
        }

        // MinInSyncReplicas must not exceed DefaultReplicationFactor
        if (MinInSyncReplicas > DefaultReplicationFactor)
        {
            errors.Add($"{nameof(MinInSyncReplicas)} ({MinInSyncReplicas}) must not exceed " +
                       $"{nameof(DefaultReplicationFactor)} ({DefaultReplicationFactor}).");
        }

        // KIP-1161 — stricter LIST-type config validation. The four
        // string-array configs Surgewave exposes (SaslMechanisms, Users,
        // SuperUsers, AllowedAlgorithms) must not contain null/blank
        // entries; duplicates are deduplicated in-place with the dropped
        // count surfaced as a warning-shaped error (Kafka logs a warning
        // and proceeds; Surgewave routes it through the same validation
        // channel so admins can't miss it on startup).
        ValidateListConfig(nameof(Security.SaslMechanisms), Security.SaslMechanisms, errors, StringComparer.OrdinalIgnoreCase, deduplicateInPlace: true, value => Security.SaslMechanisms = value);
        ValidateListConfig(nameof(Security.Users), Security.Users, errors, StringComparer.Ordinal, deduplicateInPlace: true, value => Security.Users = value);
        ValidateListConfig(nameof(Security.SuperUsers), Security.SuperUsers, errors, StringComparer.Ordinal, deduplicateInPlace: true, value => Security.SuperUsers = value);
        ValidateListConfig(nameof(Security.OAuth2.AllowedAlgorithms), Security.OAuth2.AllowedAlgorithms, errors, StringComparer.OrdinalIgnoreCase, deduplicateInPlace: true, value => Security.OAuth2.AllowedAlgorithms = value);

        return errors;
    }

    /// <summary>
    /// KIP-1161 — generic LIST-type validator. Rejects null/blank entries
    /// outright, deduplicates the array in-place (case-folded per the
    /// supplied comparer) and emits a single "warning-shaped" error per
    /// list with the dropped count so admins notice on startup.
    /// </summary>
    private static void ValidateListConfig(
        string propertyName,
        string[] current,
        List<string> errors,
        StringComparer comparer,
        bool deduplicateInPlace,
        Action<string[]> writeBack)
    {
        for (int i = 0; i < current.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(current[i]))
            {
                errors.Add($"{propertyName} contains a null or blank entry at index {i}; LIST-type configs reject null entries (KIP-1161).");
                return;
            }
        }

        if (!deduplicateInPlace) return;

        var seen = new HashSet<string>(comparer);
        var deduped = new List<string>(current.Length);
        int duplicates = 0;
        foreach (var entry in current)
        {
            if (seen.Add(entry)) deduped.Add(entry);
            else duplicates++;
        }
        if (duplicates > 0)
        {
            writeBack(deduped.ToArray());
            errors.Add($"{propertyName} contained {duplicates} duplicate entry(s); LIST-type configs deduplicate (KIP-1161). Surviving values: [{string.Join(", ", deduped)}].");
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Coordinator-internal buffer pools (KIP-1196)
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// KIP-1196 — maximum buffer size (in bytes) that the
    /// <c>ConsumerGroupCoordinator</c> / <c>ConsumerGroupV2Coordinator</c> /
    /// <c>TransactionCoordinator</c> retain for reuse across writes. Upstream
    /// Kafka maps this 1:1 to <c>group.coordinator.cached.buffer.max.bytes</c>
    /// with a default of 1 MiB + 12-byte log-record overhead and a lower
    /// bound of 512 KiB.
    ///
    /// Captured as config today so admins can tune from the outside; the
    /// actual buffer-reuse pool inside the coordinators is a documented
    /// follow-up (Surgewave allocates per-write today, which is fine for
    /// the current throughput envelope but loses a few % at sustained
    /// high-rate group-metadata churn).
    /// </summary>
    [Range(524288, int.MaxValue)]
    public int GroupCoordinatorCachedBufferMaxBytes { get; set; } = 1024 * 1024 + 12;

    /// <summary>
    /// KIP-1196 — same as <see cref="GroupCoordinatorCachedBufferMaxBytes"/>
    /// but for the <c>ShareGroupCoordinator</c>. Maps to
    /// <c>share.coordinator.cached.buffer.max.bytes</c> upstream. Default
    /// matches the consumer-group coordinator (1 MiB + 12-byte overhead).
    /// </summary>
    [Range(524288, int.MaxValue)]
    public int ShareCoordinatorCachedBufferMaxBytes { get; set; } = 1024 * 1024 + 12;

    // ─────────────────────────────────────────────────────────────────────
    // Coordinator background threads + assignment intervals (KIP-1263)
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// KIP-1263 — size of the group coordinator's background thread pool
    /// used for regex-subscription updates. Upstream Kafka:
    /// <c>group.coordinator.background.threads</c>, default 2,
    /// lower bound 1. Prior to KIP-1263 this was hard-wired to a single
    /// thread.
    ///
    /// Captured as config today; Surgewave's regex-subscription path runs
    /// inline on the heartbeat thread, so the pool isn't wired yet. The
    /// follow-up is a `BackgroundWorkScheduler` that fans regex resolution
    /// out to <see cref="GroupCoordinatorBackgroundThreads"/> workers.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int GroupCoordinatorBackgroundThreads { get; set; } = 2;

    /// <summary>
    /// KIP-1263 — interval between assignment recomputes for a consumer
    /// group. Upstream: <c>group.consumer.assignment.interval.ms</c>,
    /// default 1000 ms, lower bound 0. Prior to this KIP the effective
    /// interval was 0 ms — the assignor ran on every heartbeat that
    /// changed membership, which caused thrash during high-churn rebalance
    /// storms.
    ///
    /// Captured as config; the `TargetAssignmentComputer` already only
    /// runs when membership/subscription changed materially (Surgewave is
    /// less prone to the upstream thrash pattern), so the additional
    /// time-based gate is a documented follow-up rather than a critical
    /// gap.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int ConsumerGroupAssignmentIntervalMs { get; set; } = 1000;

    /// <summary>
    /// KIP-1263 — same as <see cref="ConsumerGroupAssignmentIntervalMs"/>
    /// but for share groups. Maps to <c>group.share.assignment.interval.ms</c>
    /// upstream. Default 1000 ms.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int ShareGroupAssignmentIntervalMs { get; set; } = 1000;

    /// <summary>
    /// KIP-1263 — same as <see cref="ConsumerGroupAssignmentIntervalMs"/>
    /// but for streams groups. Maps to <c>group.streams.assignment.interval.ms</c>
    /// upstream. Default 1000 ms.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int StreamsGroupAssignmentIntervalMs { get; set; } = 1000;
}

/// <summary>
/// Garbage collection and memory configuration for low latency
/// </summary>
public sealed class GcConfig
{
    /// <summary>
    /// GC latency mode. Options:
    /// - Interactive (default): Normal interactive application
    /// - LowLatency: Aggressive latency reduction, may increase memory
    /// - SustainedLowLatency: Extended low latency mode (recommended for brokers)
    /// - NoGCRegion: Manual control - not recommended for general use
    /// </summary>
    public string LatencyMode { get; set; } = "SustainedLowLatency";

    /// <summary>
    /// Enable large object heap compaction on full GC.
    /// Can reduce memory fragmentation at cost of occasional longer pauses.
    /// </summary>
    public bool CompactLargeObjectHeap { get; set; } = false;

    /// <summary>
    /// Force GC collection after this many megabytes of allocation (0 = disabled).
    /// Useful for preventing unexpected large GC pauses by triggering smaller collections more often.
    /// </summary>
    public int ForceGcAfterMb { get; set; } = 0;
}

/// <summary>
/// Configuration for shared memory transport on the broker
/// </summary>
public sealed class SharedMemoryBrokerConfig
{
    /// <summary>
    /// Enable shared memory transport for same-machine clients.
    /// When enabled, clients can connect via shared memory for sub-microsecond latency.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Base path for shared memory files.
    /// If null, uses platform-specific defaults:
    /// - Linux: /dev/shm/surgewave-broker-{port}
    /// - Windows: Global\surgewave-broker-{port}
    /// - macOS: /tmp/surgewave-broker-{port}
    /// </summary>
    public string? BasePath { get; set; }

    /// <summary>
    /// Ring buffer capacity in bytes. Must be a power of 2.
    /// Default: 16MB. Higher values reduce contention for high-throughput workloads.
    /// </summary>
    public int RingBufferCapacity { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Polling strategy for reading messages from the ring buffer.
    /// Options: BusySpin (lowest latency, high CPU), Sleep (low CPU, higher latency),
    /// Adaptive (spin when active, sleep when idle).
    /// </summary>
    public string PollingStrategy { get; set; } = "Adaptive";

    /// <summary>
    /// Number of busy-spin iterations before yielding.
    /// Only applies when PollingStrategy is BusySpin or Adaptive.
    /// </summary>
    public int SpinCount { get; set; } = 100;

    /// <summary>
    /// Delay between polls when idle (microseconds).
    /// Only applies when PollingStrategy is Sleep or Adaptive.
    /// </summary>
    public int IdleSleepMicroseconds { get; set; } = 1;

    /// <summary>
    /// Maximum number of concurrent shared memory clients.
    /// Set to 0 for unlimited (bounded by system resources).
    /// Default: 100.
    /// </summary>
    public int MaxClients { get; set; } = 100;

    /// <summary>
    /// Interval in milliseconds to scan for new shared memory client connections.
    /// Default: 100ms.
    /// </summary>
    public int ClientScanIntervalMs { get; set; } = 100;
}

/// <summary>
/// Configuration for embedded Schema Registry
/// </summary>
public sealed class SchemaRegistryConfig
{
    /// <summary>
    /// Enable Schema Registry support. Default: <c>true</c>.
    /// When <c>false</c>, schema support is completely disabled (no embedded, no proxy).
    /// When <c>true</c> without <see cref="ExternalUrl"/>: embedded mode.
    /// When <c>true</c> with <see cref="ExternalUrl"/>: external mode (proxy + remote inference).
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// URL of an external standalone Schema Registry (e.g. <c>http://registry:8081</c>).
    /// When set (and <see cref="Enabled"/> is <c>true</c>), the broker proxies
    /// <c>/subjects</c>, <c>/schemas</c>, <c>/config</c> to the external instance.
    /// Inference runs locally and registers schemas at the remote.
    /// </summary>
    public string? ExternalUrl { get; set; }

    /// <summary>
    /// Path to store schema data. If null, uses DataDirectory/schemas
    /// </summary>
    public string? DataPath { get; set; }

    /// <summary>
    /// Default compatibility mode for new subjects
    /// Options: None, Backward, BackwardTransitive, Forward, ForwardTransitive, Full, FullTransitive
    /// </summary>
    public string DefaultCompatibility { get; set; } = "Backward";
}

/// <summary>
/// Configuration for transaction coordinator
/// </summary>
public sealed class TransactionConfig
{
    /// <summary>
    /// Default transaction timeout in milliseconds.
    /// Transactions not completed within this time will be aborted.
    /// Default: 60,000ms (1 minute). Max: 900,000ms (15 minutes).
    /// </summary>
    public int DefaultTimeoutMs { get; set; } = 60_000;

    /// <summary>
    /// Maximum allowed transaction timeout in milliseconds.
    /// Clients cannot request a timeout larger than this.
    /// Default: 900,000ms (15 minutes).
    /// </summary>
    public int MaxTimeoutMs { get; set; } = 900_000;

    /// <summary>
    /// Minimum allowed transaction timeout in milliseconds.
    /// Default: 1,000ms (1 second).
    /// </summary>
    public int MinTimeoutMs { get; set; } = 1_000;

    /// <summary>
    /// Interval for checking transaction timeouts in milliseconds.
    /// Default: 5,000ms (5 seconds).
    /// </summary>
    public int TimeoutCheckIntervalMs { get; set; } = 5_000;

    /// <summary>
    /// Retention period for completed transaction state in hours.
    /// After this time, completed transactions are compacted.
    /// Default: 168 hours (7 days).
    /// </summary>
    public int CompletedRetentionHours { get; set; } = 168;

    /// <summary>
    /// Interval for transaction log compaction in hours.
    /// Default: 1 hour.
    /// </summary>
    public int CompactionIntervalHours { get; set; } = 1;

    /// <summary>
    /// Enable transaction state persistence to disk.
    /// Default: true.
    /// </summary>
    public bool EnablePersistence { get; set; } = true;
}

/// <summary>
/// Configuration for tiered storage (offloading segments to remote storage)
/// </summary>
public sealed class TieredStorageConfig
{
    /// <summary>
    /// Enable tiered storage
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Storage provider: "local", "azure", "s3", "gcp"
    /// </summary>
    public string Provider { get; set; } = "local";

    /// <summary>
    /// Path for local filesystem provider
    /// </summary>
    public string LocalPath { get; set; } = "./tiered-storage";

    /// <summary>
    /// Azure Storage connection string
    /// </summary>
    public string? AzureConnectionString { get; set; }

    /// <summary>
    /// Azure Storage container name
    /// </summary>
    public string AzureContainerName { get; set; } = "surgewave-tiered";

    /// <summary>
    /// S3 bucket name
    /// </summary>
    public string? S3BucketName { get; set; }

    /// <summary>
    /// S3 region (optional)
    /// </summary>
    public string? S3Region { get; set; }

    /// <summary>
    /// GCP bucket name
    /// </summary>
    public string? GcpBucketName { get; set; }

    /// <summary>
    /// Prefix for all remote objects
    /// </summary>
    public string Prefix { get; set; } = "";

    /// <summary>
    /// Hours to keep segments locally after tiering.
    /// -1 for indefinite local retention.
    /// </summary>
    public int LocalRetentionHours { get; set; } = 24;

    /// <summary>
    /// Hours to keep segments in remote storage.
    /// -1 for indefinite retention.
    /// </summary>
    public int RemoteRetentionHours { get; set; } = -1;

    /// <summary>
    /// Minimum segment age (hours) before tiering.
    /// </summary>
    public int TieringLagHours { get; set; } = 1;

    /// <summary>
    /// Minimum segment size (bytes) to tier.
    /// </summary>
    public long MinSegmentSizeBytes { get; set; } = 1024 * 1024; // 1 MB

    /// <summary>
    /// Maximum size of local cache for downloaded remote segments (bytes).
    /// </summary>
    public long LocalCacheSizeBytes { get; set; } = 1024L * 1024 * 1024; // 1 GB

    /// <summary>
    /// Directory for caching downloaded remote segments
    /// </summary>
    public string LocalCachePath { get; set; } = "./tiered-cache";

    /// <summary>
    /// Delete local segments after successful upload to remote
    /// </summary>
    public bool DeleteAfterUpload { get; set; } = true;

    /// <summary>
    /// Background tiering interval (seconds)
    /// </summary>
    public int TieringIntervalSeconds { get; set; } = 300;
}

/// <summary>
/// Security configuration for authentication and authorization
/// </summary>
public sealed class SecurityConfig
{
    /// <summary>
    /// Enable SASL authentication (default: false for dev convenience)
    /// </summary>
    public bool SaslEnabled { get; set; } = false;

    /// <summary>
    /// Enabled SASL mechanisms (default: PLAIN)
    /// Options: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    /// </summary>
    public string[] SaslMechanisms { get; set; } = ["PLAIN"];

    /// <summary>
    /// Path to credentials file for file-based authentication
    /// Format: username:passwordHash:salt per line
    /// </summary>
    public string? CredentialsFile { get; set; }

    /// <summary>
    /// In-memory user credentials for simple setups
    /// Format: "username:password" pairs
    /// </summary>
    public string[] Users { get; set; } = [];

    /// <summary>
    /// Allow unauthenticated connections when SASL is enabled
    /// Useful for migration period
    /// </summary>
    public bool AllowAnonymous { get; set; } = false;

    // TLS/SSL settings

    /// <summary>
    /// Enable TLS for encrypted connections
    /// </summary>
    public bool TlsEnabled { get; set; } = false;

    /// <summary>
    /// Path to the server certificate file (PFX/PKCS12 format)
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Password for the certificate file
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Require client certificates for mutual TLS (mTLS)
    /// </summary>
    public bool RequireClientCertificate { get; set; } = false;

    /// <summary>
    /// Path to trusted CA certificates for client validation
    /// </summary>
    public string? TrustedCaCertificatePath { get; set; }

    /// <summary>
    /// Minimum TLS version (TLS12, TLS13). Default: TLS12
    /// </summary>
    public string MinTlsVersion { get; set; } = "TLS12";

    // HTTP/3 (QUIC): configure via Kestrel:Endpoints in appsettings.json
    // Set Protocols to "Http1AndHttp2AndHttp3" and provide a TLS certificate

    // Authorization (ACL) settings

    /// <summary>
    /// Enable ACL-based authorization
    /// </summary>
    public bool AclEnabled { get; set; } = false;

    /// <summary>
    /// Path to ACL configuration file
    /// Format: principal|host|resourceType|patternType|resourceName|operation|permission
    /// </summary>
    public string? AclFile { get; set; }

    /// <summary>
    /// Super users who bypass ACL checks (format: "User:admin")
    /// </summary>
    public string[] SuperUsers { get; set; } = [];

    /// <summary>
    /// Allow operations if no ACL is found (default: false = deny)
    /// </summary>
    public bool AllowIfNoAclFound { get; set; } = false;

    /// <summary>
    /// OAuth2/OIDC authentication settings for JWT token validation.
    /// </summary>
    public OAuth2Config OAuth2 { get; set; } = new();

    /// <summary>
    /// SASL/OAUTHBEARER configuration (KIP-936). Bound from
    /// <c>Surgewave:Security:OAuthBearer</c>. When <c>OAuthBearer.Enabled</c>
    /// is <c>true</c> and <c>OAUTHBEARER</c> appears in <see cref="SaslMechanisms"/>,
    /// the broker stands up a JWKS-backed JWT validator and accepts bearer
    /// tokens on the Kafka wire.
    /// </summary>
    public OAuthBearerConfig OAuthBearer { get; set; } = new();
}

/// <summary>
/// OAuth2/OIDC authentication configuration.
/// </summary>
public sealed class OAuth2Config
{
    /// <summary>
    /// Enable OAuth2/OIDC authentication.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// OIDC issuer URL (e.g., https://auth.example.com/realms/surgewave).
    /// </summary>
    public string? Issuer { get; set; }

    /// <summary>
    /// JWKS endpoint URL. If not set, discovered from issuer.
    /// </summary>
    public string? JwksUri { get; set; }

    /// <summary>
    /// Expected audience claim (usually client ID).
    /// </summary>
    public string? Audience { get; set; }

    /// <summary>
    /// Claim containing the username. Default: "preferred_username"
    /// </summary>
    public string UsernameClaim { get; set; } = "preferred_username";

    /// <summary>
    /// Claim containing groups/roles. Default: "groups"
    /// </summary>
    public string GroupsClaim { get; set; } = "groups";

    /// <summary>
    /// Clock skew tolerance in minutes. Default: 5
    /// </summary>
    public int ClockSkewMinutes { get; set; } = 5;

    /// <summary>
    /// JWKS cache duration in hours. Default: 1
    /// </summary>
    public int JwksCacheHours { get; set; } = 1;

    /// <summary>
    /// Require HTTPS for metadata endpoint. Default: true
    /// </summary>
    public bool RequireHttpsMetadata { get; set; } = true;

    /// <summary>
    /// Allowed signing algorithms. Default: RS256, ES256
    /// </summary>
    public string[] AllowedAlgorithms { get; set; } = ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"];

    /// <summary>
    /// Clock skew tolerance as TimeSpan.
    /// </summary>
    public TimeSpan ClockSkew => TimeSpan.FromMinutes(ClockSkewMinutes);

    /// <summary>
    /// JWKS cache duration as TimeSpan.
    /// </summary>
    public TimeSpan JwksCacheDuration => TimeSpan.FromHours(JwksCacheHours);
}

/// <summary>
/// Configuration for Kafka Connect
/// </summary>
public sealed class ConnectConfig
{
    /// <summary>
    /// Enable Kafka Connect framework
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Consumer group ID for connector offset management
    /// </summary>
    public string GroupId { get; set; } = "surgewave-connect";

    /// <summary>
    /// Topic for storing connector configurations
    /// </summary>
    public string ConfigTopic { get; set; } = "surgewave-connect-configs";

    /// <summary>
    /// Topic for storing connector offsets
    /// </summary>
    public string OffsetsTopic { get; set; } = "surgewave-connect-offsets";

    /// <summary>
    /// Topic for storing connector status
    /// </summary>
    public string StatusTopic { get; set; } = "surgewave-connect-status";

    /// <summary>
    /// Directory to scan for connector plugins
    /// </summary>
    public string PluginsDirectory { get; set; } = "plugins";
}

/// <summary>
/// Configuration for audit logging
/// </summary>
public sealed class AuditConfig
{
    /// <summary>
    /// Enable audit logging
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Number of partitions for the audit log topic
    /// </summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// Replication factor for the audit log topic
    /// </summary>
    public short ReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Retention period for audit log in milliseconds.
    /// Default: 7 days (604800000 ms)
    /// </summary>
    public long RetentionMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Event types to include in audit log.
    /// Empty list means all events are logged (subject to ExcludeEventTypes).
    /// </summary>
    public HashSet<AuditEventType> IncludeEventTypes { get; set; } = [];

    /// <summary>
    /// Event types to exclude from audit log.
    /// Takes precedence over IncludeEventTypes.
    /// </summary>
    public HashSet<AuditEventType> ExcludeEventTypes { get; set; } = [];

    /// <summary>
    /// Exclude internal topics (topics starting with __) from audit log.
    /// Default: true
    /// </summary>
    public bool ExcludeInternalTopics { get; set; } = true;

    /// <summary>
    /// Log successful authentication events.
    /// Default: false (only log failed authentications)
    /// </summary>
    public bool LogSuccessfulAuthentication { get; set; } = false;

    /// <summary>
    /// Log authorization checks (can be very verbose).
    /// Default: false
    /// </summary>
    public bool LogAuthorizationChecks { get; set; } = false;

    // (TopicSinkEnabled / TopicName below — kept in this audit block for
    // backwards-compat; the parallel telemetry topic-sink block lives in
    // ClientTelemetryConfig.)

    /// <summary>
    /// Mirror audit events to a Surgewave topic in addition to the file sink. When
    /// enabled, every event written to <c>audit.log</c> is also produced to
    /// <see cref="TopicName"/> so a downstream SIEM / compliance pipeline can
    /// consume it through the standard Kafka wire (G13 of the Surgewave
    /// competitive gap analysis — Confluent Audit Logs parity). Default
    /// <c>false</c>: file-only sink, no topic side effect.
    /// </summary>
    public bool TopicSinkEnabled { get; set; } = false;

    /// <summary>
    /// Name of the audit topic when <see cref="TopicSinkEnabled"/> is on. The
    /// underscore prefix marks it as an internal topic so it's hidden from
    /// regular topic listings and excluded from default consumer subscriptions
    /// — operators must opt in explicitly to consume it. Default
    /// <c>_audit_events</c>.
    /// </summary>
    public string TopicName { get; set; } = "_audit_events";
}

/// <summary>
/// Configuration for data integrity validation
/// </summary>
public sealed class DataIntegrityConfig
{
    /// <summary>
    /// Enable CRC validation when reading record batches.
    /// Default: true. Validates data integrity at the cost of some CPU overhead.
    /// </summary>
    public bool ValidateCrcOnRead { get; set; } = true;

    /// <summary>
    /// How to handle corrupted batches when detected during reads.
    /// Default: SkipAndContinue - skip corrupted batches and continue reading.
    /// </summary>
    public CorruptionRecoveryMode CorruptionRecovery { get; set; } = CorruptionRecoveryMode.SkipAndContinue;

    /// <summary>
    /// Log corruption events to the application logger.
    /// Default: true.
    /// </summary>
    public bool LogCorruptionEvents { get; set; } = true;

    /// <summary>
    /// Emit metrics for corruption detection (corrupted batches, bytes).
    /// Default: true.
    /// </summary>
    public bool EmitCorruptionMetrics { get; set; } = true;
}
