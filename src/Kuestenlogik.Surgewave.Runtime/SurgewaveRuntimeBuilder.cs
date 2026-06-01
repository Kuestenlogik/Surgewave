using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Runtime;

/// <summary>
/// Fluent builder for configuring and starting a Surgewave broker runtime.
/// Create via <see cref="SurgewaveRuntime.CreateBuilder"/>.
/// </summary>
public sealed class SurgewaveRuntimeBuilder
{
    // Default Host ist die IPv4-Loopback-Adresse (statt "localhost"), weil
    // Linux getaddrinfo("localhost") gerne ::1 zuerst liefert und ein
    // librdkafka-Client auf v6-deaktivierten oder dual-stack-roulette
    // Setups dann am Connect scheitert. Wer Dual-Stack braucht, opt-in
    // ueber WithDualMode() — das setzt zusaetzlich den Host wieder auf
    // "localhost" zurueck, damit Clients beide Stacks probieren.
    private string _host = "127.0.0.1";
    private int _port = 0;
    private int _replicationPort = 0;
    private bool _enableDualMode = false;
    private int _brokerId = 0;
    private string? _dataDirectory;
    private string _storageEngine = StorageEngines.File;
    private Func<Core.Storage.ILogSegmentFactory>? _customLogSegmentFactory;
    private int _retentionHours = -1;
    private long _retentionBytes = -1;
    private bool _autoCreateTopics = true;
    private int _defaultNumPartitions = 1;
    private short _defaultReplicationFactor = 1;
    private bool _enableCluster = false;
    private List<string> _clusterNodes = [];
    private bool _useRaftConsensus = false;
    private int _raftElectionTimeoutMinMs = 150;
    private int _raftElectionTimeoutMaxMs = 300;
    private int _raftHeartbeatIntervalMs = 50;
    private int _raftPeerDiscoveryTimeoutSeconds = 30;
    private int _heartbeatIntervalMs = 3000;
    private int _heartbeatTimeoutMs = 10000;
    private bool _enableSasl = false;
    private bool _enableTls = false;
    private bool _enableAcl = false;
    private int _shutdownTimeoutSeconds = 5;
    private bool _cleanupOnDispose = true;
    private ILoggerFactory? _loggerFactory;

    internal SurgewaveRuntimeBuilder() { }

    // ==================== Network ====================

    /// <summary>
    /// Sets the host address to bind to.
    /// </summary>
    public SurgewaveRuntimeBuilder WithHost(string host)
    {
        _host = host;
        return this;
    }

    /// <summary>
    /// Sets the port to listen on. Use 0 for automatic assignment.
    /// </summary>
    public SurgewaveRuntimeBuilder WithPort(int port)
    {
        _port = port;
        return this;
    }

    /// <summary>
    /// Sets the replication port. Use 0 for automatic assignment.
    /// </summary>
    public SurgewaveRuntimeBuilder WithReplicationPort(int port)
    {
        _replicationPort = port;
        return this;
    }

    /// <summary>
    /// Aktiviert Dual-Stack-Listen (IPv4 + IPv6). Default false; in dem Fall
    /// bindet der Broker auf 127.0.0.1 und der Host bleibt 127.0.0.1 — der
    /// deterministische Pfad fuer Tests/Dev/CI ohne IPv6-Roulette. Mit
    /// `enable: true` wechseln wir auf IPv6Any+DualMode und stellen den
    /// Default-Host wieder auf "localhost", damit Clients beide Stacks
    /// probieren koennen. Wer einen anderen Host explizit gesetzt hat,
    /// bleibt unangetastet.
    /// </summary>
    public SurgewaveRuntimeBuilder WithDualMode(bool enable = true)
    {
        _enableDualMode = enable;
        if (enable && _host == "127.0.0.1")
        {
            _host = "localhost";
        }
        return this;
    }

    /// <summary>
    /// Disables dual-stack socket binding, forcing IPv4 only.
    /// Equivalent to WithDualMode(false).
    /// </summary>
    public SurgewaveRuntimeBuilder WithIPv4Only()
    {
        _enableDualMode = false;
        return this;
    }

    // ==================== Identity ====================

    /// <summary>
    /// Sets the broker ID.
    /// </summary>
    public SurgewaveRuntimeBuilder WithBrokerId(int brokerId)
    {
        _brokerId = brokerId;
        return this;
    }

    // ==================== Storage ====================

    /// <summary>
    /// Sets the data directory for logs and state.
    /// If not set, uses a temporary directory.
    /// </summary>
    public SurgewaveRuntimeBuilder WithDataDirectory(string directory)
    {
        _dataDirectory = directory;
        return this;
    }

    /// <summary>
    /// Sets the storage engine by name.
    /// Note: This is ignored if WithStorage() is used.
    /// See <see cref="StorageEngines"/> for well-known engine names.
    /// </summary>
    public SurgewaveRuntimeBuilder WithStorageEngine(string engine)
    {
        _storageEngine = engine;
        return this;
    }

    /// <summary>
    /// Sets a custom log segment factory for storage.
    /// This takes precedence over StorageEngine.
    /// Prefer using extension methods like WithRocksDbStorage(), WithSqliteStorage(), etc.
    /// </summary>
    public SurgewaveRuntimeBuilder WithStorage(Func<ILogSegmentFactory> factory)
    {
        _customLogSegmentFactory = factory;
        return this;
    }

    /// <summary>
    /// Sets a custom log segment factory for storage.
    /// This takes precedence over StorageEngine.
    /// Prefer using extension methods like WithRocksDbStorage(), WithSqliteStorage(), etc.
    /// </summary>
    public SurgewaveRuntimeBuilder WithStorage(ILogSegmentFactory factory)
    {
        _customLogSegmentFactory = () => factory;
        return this;
    }

    /// <summary>
    /// Sets the retention period in hours. Use -1 for infinite.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRetentionHours(int hours)
    {
        _retentionHours = hours;
        return this;
    }

    /// <summary>
    /// Sets the maximum log size in bytes. Use -1 for unlimited.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRetentionBytes(long bytes)
    {
        _retentionBytes = bytes;
        return this;
    }

    // ==================== Topics ====================

    /// <summary>
    /// Enables or disables automatic topic creation.
    /// </summary>
    public SurgewaveRuntimeBuilder WithAutoCreateTopics(bool enable = true)
    {
        _autoCreateTopics = enable;
        return this;
    }

    /// <summary>
    /// Sets the default number of partitions for auto-created topics.
    /// </summary>
    public SurgewaveRuntimeBuilder WithPartitions(int partitions)
    {
        _defaultNumPartitions = partitions;
        return this;
    }

    /// <summary>
    /// Sets the default replication factor for auto-created topics.
    /// </summary>
    public SurgewaveRuntimeBuilder WithReplicationFactor(short factor)
    {
        _defaultReplicationFactor = factor;
        return this;
    }

    // ==================== Cluster ====================

    /// <summary>
    /// Enables cluster mode with the specified nodes.
    /// </summary>
    /// <param name="nodes">List of nodes in format "brokerId:host:port".</param>
    public SurgewaveRuntimeBuilder WithCluster(params string[] nodes)
    {
        _enableCluster = true;
        _clusterNodes = [.. nodes];
        return this;
    }

    /// <summary>
    /// Enables cluster mode with the specified nodes.
    /// </summary>
    public SurgewaveRuntimeBuilder WithCluster(IEnumerable<string> nodes)
    {
        _enableCluster = true;
        _clusterNodes = nodes.ToList();
        return this;
    }

    /// <summary>
    /// Enables or disables Raft consensus.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRaft(bool enable = true)
    {
        _useRaftConsensus = enable;
        return this;
    }

    /// <summary>
    /// Configures Raft election timeouts.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRaftElectionTimeout(int minMs, int maxMs)
    {
        _raftElectionTimeoutMinMs = minMs;
        _raftElectionTimeoutMaxMs = maxMs;
        return this;
    }

    /// <summary>
    /// Sets the Raft heartbeat interval.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRaftHeartbeatInterval(int intervalMs)
    {
        _raftHeartbeatIntervalMs = intervalMs;
        return this;
    }

    /// <summary>
    /// Sets the timeout in seconds to wait for peer discovery before starting elections.
    /// Set to 0 to disable waiting. Default: 30 seconds.
    /// </summary>
    public SurgewaveRuntimeBuilder WithRaftPeerDiscoveryTimeout(int timeoutSeconds)
    {
        _raftPeerDiscoveryTimeoutSeconds = timeoutSeconds;
        return this;
    }

    /// <summary>
    /// Sets the cluster heartbeat interval.
    /// </summary>
    public SurgewaveRuntimeBuilder WithHeartbeatInterval(int intervalMs)
    {
        _heartbeatIntervalMs = intervalMs;
        return this;
    }

    /// <summary>
    /// Sets the cluster heartbeat timeout.
    /// </summary>
    public SurgewaveRuntimeBuilder WithHeartbeatTimeout(int timeoutMs)
    {
        _heartbeatTimeoutMs = timeoutMs;
        return this;
    }

    // ==================== Security ====================

    /// <summary>
    /// Enables or disables SASL authentication.
    /// </summary>
    public SurgewaveRuntimeBuilder WithSasl(bool enable = true)
    {
        _enableSasl = enable;
        return this;
    }

    /// <summary>
    /// Enables or disables TLS/SSL.
    /// </summary>
    public SurgewaveRuntimeBuilder WithTls(bool enable = true)
    {
        _enableTls = enable;
        return this;
    }

    /// <summary>
    /// Enables or disables ACL authorization.
    /// </summary>
    public SurgewaveRuntimeBuilder WithAcl(bool enable = true)
    {
        _enableAcl = enable;
        return this;
    }

    // ==================== Lifecycle ====================

    /// <summary>
    /// Sets the shutdown timeout in seconds.
    /// </summary>
    public SurgewaveRuntimeBuilder WithShutdownTimeout(int seconds)
    {
        _shutdownTimeoutSeconds = seconds;
        return this;
    }

    /// <summary>
    /// Enables or disables cleanup of data directory on dispose.
    /// </summary>
    public SurgewaveRuntimeBuilder WithCleanup(bool cleanup = true)
    {
        _cleanupOnDispose = cleanup;
        return this;
    }

    // ==================== Logging ====================

    /// <summary>
    /// Sets the logger factory for the runtime.
    /// </summary>
    public SurgewaveRuntimeBuilder WithLogging(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    // ==================== Configuration ====================

    /// <summary>
    /// Applies configuration from a BrokerConfig instance.
    /// </summary>
    public SurgewaveRuntimeBuilder ConfigureFrom(BrokerConfig config)
    {
        _host = config.Host;
        _port = config.Port;
        _brokerId = config.BrokerId;
        _dataDirectory = config.DataDirectory;
        _storageEngine = config.StorageEngine;
        _autoCreateTopics = config.AutoCreateTopics;
        _defaultNumPartitions = config.DefaultNumPartitions;
        _defaultReplicationFactor = config.DefaultReplicationFactor;
        _retentionHours = config.LogRetentionHours;
        _retentionBytes = config.LogRetentionBytes;
        _enableCluster = !string.IsNullOrEmpty(config.ClusterNodes);
        _useRaftConsensus = config.UseRaftConsensus;
        _enableSasl = config.Security.SaslEnabled;
        _enableTls = config.Security.TlsEnabled;
        _enableAcl = config.Security.AclEnabled;
        return this;
    }

    // ==================== Build ====================

    /// <summary>
    /// Builds the runtime options without starting the broker.
    /// </summary>
    public SurgewaveRuntimeOptions Build() => new()
    {
        Host = _host,
        Port = _port,
        ReplicationPort = _replicationPort,
        EnableDualMode = _enableDualMode,
        BrokerId = _brokerId,
        DataDirectory = _dataDirectory,
        StorageEngine = _storageEngine,
        CustomLogSegmentFactory = _customLogSegmentFactory,
        RetentionHours = _retentionHours,
        RetentionBytes = _retentionBytes,
        AutoCreateTopics = _autoCreateTopics,
        DefaultNumPartitions = _defaultNumPartitions,
        DefaultReplicationFactor = _defaultReplicationFactor,
        EnableCluster = _enableCluster,
        ClusterNodes = _clusterNodes,
        UseRaftConsensus = _useRaftConsensus,
        RaftElectionTimeoutMinMs = _raftElectionTimeoutMinMs,
        RaftElectionTimeoutMaxMs = _raftElectionTimeoutMaxMs,
        RaftHeartbeatIntervalMs = _raftHeartbeatIntervalMs,
        RaftPeerDiscoveryTimeoutSeconds = _raftPeerDiscoveryTimeoutSeconds,
        HeartbeatIntervalMs = _heartbeatIntervalMs,
        HeartbeatTimeoutMs = _heartbeatTimeoutMs,
        EnableSasl = _enableSasl,
        EnableTls = _enableTls,
        EnableAcl = _enableAcl,
        ShutdownTimeoutSeconds = _shutdownTimeoutSeconds,
        CleanupOnDispose = _cleanupOnDispose,
        LoggerFactory = _loggerFactory
    };
}
