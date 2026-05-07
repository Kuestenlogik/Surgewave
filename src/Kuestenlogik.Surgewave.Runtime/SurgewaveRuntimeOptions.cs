using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Runtime;

/// <summary>
/// Configuration options for a Surgewave broker runtime.
/// Use <see cref="SurgewaveRuntime.CreateBuilder"/> to create instances via fluent API.
/// </summary>
public sealed record SurgewaveRuntimeOptions
{
    // ==================== Network Options ====================

    /// <summary>
    /// Host address to bind to. Defaults to "localhost".
    /// </summary>
    public string Host { get; init; } = "localhost";

    /// <summary>
    /// Port to listen on. Use 0 for automatic port assignment.
    /// Defaults to 0 for automatic assignment.
    /// </summary>
    public int Port { get; init; } = 0;

    /// <summary>
    /// Port for inter-broker replication. Use 0 for automatic assignment.
    /// </summary>
    public int ReplicationPort { get; init; } = 0;

    /// <summary>
    /// Enable dual-stack socket binding (IPv4 + IPv6).
    /// When true and Host is "localhost", binds to IPv6Any with DualMode enabled.
    /// When false, binds to IPv4 only (127.0.0.1 for localhost).
    /// Defaults to true. Set to false for environments without IPv6 support.
    /// </summary>
    public bool EnableDualMode { get; init; } = true;

    // ==================== Identity Options ====================

    /// <summary>
    /// Broker ID. Defaults to 0.
    /// </summary>
    public int BrokerId { get; init; } = 0;

    // ==================== Storage Options ====================

    /// <summary>
    /// Data directory for logs and state. If null, uses a temporary directory
    /// that is automatically cleaned up on disposal.
    /// </summary>
    public string? DataDirectory { get; init; }

    /// <summary>
    /// Storage engine name for the broker logs.
    /// "file": Uses memory-mapped files with disk persistence (default).
    /// "memory": Uses in-memory storage without disk I/O (ephemeral, ideal for testing).
    /// Ignored when CustomLogSegmentFactory is set.
    /// See <see cref="Kuestenlogik.Surgewave.Core.Storage.StorageEngines"/> for well-known engine names.
    /// </summary>
    public string StorageEngine { get; init; } = StorageEngines.File;

    /// <summary>
    /// Custom log segment factory for advanced storage configurations.
    /// When set, this takes precedence over StorageEngine.
    /// Set via builder extension methods like WithRocksDbStorage(), WithSqliteStorage(), etc.
    /// </summary>
    public Func<Core.Storage.ILogSegmentFactory>? CustomLogSegmentFactory { get; init; }

    /// <summary>
    /// Retention period in hours. -1 for infinite retention.
    /// </summary>
    public int RetentionHours { get; init; } = -1;

    /// <summary>
    /// Maximum log size in bytes. -1 for unlimited.
    /// </summary>
    public long RetentionBytes { get; init; } = -1;

    // ==================== Topic Options ====================

    /// <summary>
    /// Whether to automatically create topics when they don't exist.
    /// Defaults to true.
    /// </summary>
    public bool AutoCreateTopics { get; init; } = true;

    /// <summary>
    /// Default number of partitions for auto-created topics.
    /// Defaults to 1.
    /// </summary>
    public int DefaultNumPartitions { get; init; } = 1;

    /// <summary>
    /// Default replication factor for auto-created topics.
    /// Defaults to 1 (single node).
    /// </summary>
    public short DefaultReplicationFactor { get; init; } = 1;

    // ==================== Cluster Options ====================

    /// <summary>
    /// Enable cluster mode. When true, the broker will participate in a cluster.
    /// Defaults to false (standalone mode).
    /// </summary>
    public bool EnableCluster { get; init; } = false;

    /// <summary>
    /// List of other brokers in the cluster (format: "brokerId:host:port").
    /// Example: ["1:localhost:10092", "2:localhost:10093"]
    /// </summary>
    public List<string> ClusterNodes { get; init; } = [];

    /// <summary>
    /// Enable Raft consensus for controller election and metadata replication.
    /// Requires EnableCluster = true.
    /// </summary>
    public bool UseRaftConsensus { get; init; } = false;

    /// <summary>
    /// Minimum election timeout in milliseconds (Raft).
    /// </summary>
    public int RaftElectionTimeoutMinMs { get; init; } = 150;

    /// <summary>
    /// Maximum election timeout in milliseconds (Raft).
    /// </summary>
    public int RaftElectionTimeoutMaxMs { get; init; } = 300;

    /// <summary>
    /// Raft heartbeat interval in milliseconds.
    /// </summary>
    public int RaftHeartbeatIntervalMs { get; init; } = 50;

    /// <summary>
    /// Timeout in seconds to wait for peer discovery before starting elections.
    /// Set to 0 to disable waiting. Default: 30 seconds.
    /// </summary>
    public int RaftPeerDiscoveryTimeoutSeconds { get; init; } = 30;

    /// <summary>
    /// Heartbeat interval in milliseconds.
    /// </summary>
    public int HeartbeatIntervalMs { get; init; } = 3000;

    /// <summary>
    /// Heartbeat timeout in milliseconds. Broker is considered dead after this.
    /// </summary>
    public int HeartbeatTimeoutMs { get; init; } = 10000;

    // ==================== Security Options ====================

    /// <summary>
    /// Enable SASL authentication.
    /// </summary>
    public bool EnableSasl { get; init; } = false;

    /// <summary>
    /// Enable TLS/SSL.
    /// </summary>
    public bool EnableTls { get; init; } = false;

    /// <summary>
    /// Enable ACL authorization.
    /// </summary>
    public bool EnableAcl { get; init; } = false;

    // ==================== Lifecycle Options ====================

    /// <summary>
    /// Shutdown timeout in seconds when disposing the broker.
    /// </summary>
    public int ShutdownTimeoutSeconds { get; init; } = 5;

    /// <summary>
    /// Whether to delete the data directory on disposal.
    /// Defaults to true when DataDirectory is null (temp directory).
    /// </summary>
    public bool CleanupOnDispose { get; init; } = true;

    // ==================== Internal ====================

    /// <summary>
    /// Logger factory for the runtime. Set via builder.
    /// </summary>
    internal ILoggerFactory? LoggerFactory { get; init; }

    /// <summary>
    /// Starts the Surgewave broker with these options.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for startup.</param>
    /// <returns>A running SurgewaveRuntime instance.</returns>
    public Task<SurgewaveRuntime> StartAsync(CancellationToken cancellationToken = default)
        => SurgewaveRuntime.StartAsync(this, cancellationToken);
}
