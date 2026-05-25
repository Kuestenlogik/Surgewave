using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Clustering;

/// <summary>
/// Configuration for clustering features - extracted from BrokerConfig for clean separation.
/// This allows the Clustering project to be independent of the Broker project.
/// </summary>
public sealed class ClusteringConfig : IValidatableConfig
{
    // Identity
    [Range(0, int.MaxValue)]
    public int BrokerId { get; set; } = 0;

    [Required]
    [MinLength(1)]
    public string Host { get; set; } = "localhost";

    [Range(1, 65535)]
    public int Port { get; set; } = 9092;

    public string? Rack { get; set; }
    public string? ClusterId { get; set; }

    [Required]
    [MinLength(1)]
    public string DataDirectory { get; set; } = "./data";

    // Cluster settings
    public string ClusterNodes { get; set; } = "";

    [Range(1, 65535)]
    public int ReplicationPort { get; set; } = 9093;

    /// <summary>
    /// Transport to use for broker-to-broker traffic (Raft RPC, partition
    /// replication, geo-replication). Allowed values: "tcp", "quic".
    /// Default: "tcp". QUIC requires msquic on all brokers in the cluster.
    /// </summary>
    [RegularExpression("^(tcp|quic)$",
        ErrorMessage = "InterBrokerTransport must be 'tcp' or 'quic'.")]
    public string InterBrokerTransport { get; set; } = "tcp";

    /// <summary>
    /// Path to this broker's PKCS#12 (.pfx) certificate used for QUIC inter-broker
    /// mTLS. The certificate must be signed by the CA at
    /// <see cref="InterBrokerCaCertificatePath"/>. When both are set, QUIC peer
    /// connections enforce mutual authentication. When empty, Surgewave falls back to
    /// a self-signed dev certificate (only safe in development).
    /// </summary>
    public string InterBrokerCertificatePath { get; set; } = "";

    /// <summary>
    /// Password for the PKCS#12 certificate at <see cref="InterBrokerCertificatePath"/>.
    /// </summary>
    public string InterBrokerCertificatePassword { get; set; } = "";

    /// <summary>
    /// Path to the shared cluster CA certificate (.cer/.crt/.pem). Every broker
    /// in the cluster must trust the same CA, and their individual broker certs
    /// must be signed by it. Set this together with
    /// <see cref="InterBrokerCertificatePath"/> to enable production mTLS.
    /// </summary>
    public string InterBrokerCaCertificatePath { get; set; } = "";

    [Range(1, int.MaxValue)]
    public int MinInSyncReplicas { get; set; } = 1;

    // Controller settings
    public bool AllowAutoLeaderRebalance { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int LeaderImbalanceCheckIntervalSeconds { get; set; } = 300;

    [Range(0, int.MaxValue)]
    public int ControlledShutdownMaxRetries { get; set; } = 3;

    // Heartbeat settings
    [Range(1, int.MaxValue)]
    public int HeartbeatIntervalMs { get; set; } = 3000;

    [Range(1, int.MaxValue)]
    public int HeartbeatTimeoutMs { get; set; } = 10000;

    [Range(1, int.MaxValue)]
    public int MaxHeartbeatFailures { get; set; } = 3;

    // Raft consensus settings
    public bool UseRaftConsensus { get; set; } = false;

    [Required]
    [MinLength(1)]
    public string RaftDataDirectory { get; set; } = "./data/raft";

    [Range(1, int.MaxValue)]
    public int RaftElectionTimeoutMinMs { get; set; } = 150;

    [Range(1, int.MaxValue)]
    public int RaftElectionTimeoutMaxMs { get; set; } = 300;

    [Range(1, int.MaxValue)]
    public int RaftHeartbeatIntervalMs { get; set; } = 50;

    [Range(1, int.MaxValue)]
    public int RaftIsolationTimeoutMs { get; set; } = 10000; // 10 seconds - step down if isolated

    /// <summary>
    /// Timeout in seconds to wait for peer discovery before starting elections.
    /// This prevents split-brain during sequential broker startup.
    /// Default: 30 seconds. Set to 0 to disable waiting.
    /// </summary>
    [Range(0, int.MaxValue)]
    public int RaftPeerDiscoveryTimeoutSeconds { get; set; } = 30;

    // Auto-rebalancing settings
    public bool AutoRebalanceEnabled { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int RebalanceCheckIntervalSeconds { get; set; } = 300;

    [Range(0.0, 1.0)]
    public double RebalanceImbalanceThreshold { get; set; } = 0.1;

    // Partition reassignment settings
    [Range(1, long.MaxValue)]
    public long ReassignmentThrottleBytesPerSec { get; set; } = 50_000_000;

    [Range(1, int.MaxValue)]
    public int ReassignmentMaxConcurrent { get; set; } = 5;

    // Failure domain / rack-aware settings
    /// <summary>
    /// Minimum number of distinct racks/zones required for replica placement.
    /// 0 means no minimum (validation disabled).
    /// </summary>
    [Range(0, int.MaxValue)]
    public int MinInSyncRacks { get; set; } = 0;

    /// <summary>
    /// Whether to prevent all replicas from being placed in the same rack.
    /// </summary>
    public bool PreventSingleRackReplicas { get; set; } = true;

    /// <summary>
    /// Failure domain validation level: Rack, Zone, Datacenter, or Region.
    /// Uses hierarchical rack format: region/datacenter/zone/rack
    /// </summary>
    [RegularExpression("^(Rack|Zone|Datacenter|Region)$",
        ErrorMessage = "FailureDomainLevel must be one of: Rack, Zone, Datacenter, Region.")]
    public string FailureDomainLevel { get; set; } = "Rack";

    /// <summary>
    /// Placement constraints for replica assignment.
    /// Format: "spread_across:zone" or "prefer:region=us-east"
    /// </summary>
    public string? PlacementConstraints { get; set; }

    /// <summary>
    /// Leader election strategy: PreferredReplica, RackLocal, or LatencyOptimized.
    /// </summary>
    [RegularExpression("^(PreferredReplica|RackLocal|LatencyOptimized)$",
        ErrorMessage = "LeaderElectionStrategy must be one of: PreferredReplica, RackLocal, LatencyOptimized.")]
    public string LeaderElectionStrategy { get; set; } = "PreferredReplica";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (Port == ReplicationPort)
            errors.Add($"{nameof(Port)} and {nameof(ReplicationPort)} must be different (both {Port}).");

        if (HeartbeatTimeoutMs <= HeartbeatIntervalMs)
            errors.Add($"{nameof(HeartbeatTimeoutMs)} must exceed {nameof(HeartbeatIntervalMs)}.");

        if (UseRaftConsensus && RaftElectionTimeoutMinMs >= RaftElectionTimeoutMaxMs)
            errors.Add($"{nameof(RaftElectionTimeoutMinMs)} must be less than {nameof(RaftElectionTimeoutMaxMs)}.");

        return errors;
    }

    /// <summary>
    /// Creates a ClusteringConfig from BrokerConfig properties via reflection-free copying.
    /// This factory is called from the Broker project.
    /// </summary>
    public static ClusteringConfig Create(
        int brokerId,
        string host,
        int port,
        string? rack,
        string? clusterId,
        string dataDirectory,
        string clusterNodes,
        int replicationPort,
        int minInSyncReplicas,
        bool allowAutoLeaderRebalance,
        int leaderImbalanceCheckIntervalSeconds,
        int controlledShutdownMaxRetries,
        int heartbeatIntervalMs,
        int heartbeatTimeoutMs,
        int maxHeartbeatFailures,
        bool useRaftConsensus,
        string raftDataDirectory,
        int raftElectionTimeoutMinMs,
        int raftElectionTimeoutMaxMs,
        int raftHeartbeatIntervalMs,
        int raftPeerDiscoveryTimeoutSeconds,
        bool autoRebalanceEnabled,
        int rebalanceCheckIntervalSeconds,
        double rebalanceImbalanceThreshold,
        long reassignmentThrottleBytesPerSec,
        int reassignmentMaxConcurrent,
        string interBrokerTransport = "tcp",
        string interBrokerCertificatePath = "",
        string interBrokerCertificatePassword = "",
        string interBrokerCaCertificatePath = "")
    {
        return new ClusteringConfig
        {
            BrokerId = brokerId,
            Host = host,
            Port = port,
            Rack = rack,
            ClusterId = clusterId,
            DataDirectory = dataDirectory,
            ClusterNodes = clusterNodes,
            ReplicationPort = replicationPort,
            InterBrokerTransport = interBrokerTransport,
            InterBrokerCertificatePath = interBrokerCertificatePath,
            InterBrokerCertificatePassword = interBrokerCertificatePassword,
            InterBrokerCaCertificatePath = interBrokerCaCertificatePath,
            MinInSyncReplicas = minInSyncReplicas,
            AllowAutoLeaderRebalance = allowAutoLeaderRebalance,
            LeaderImbalanceCheckIntervalSeconds = leaderImbalanceCheckIntervalSeconds,
            ControlledShutdownMaxRetries = controlledShutdownMaxRetries,
            HeartbeatIntervalMs = heartbeatIntervalMs,
            HeartbeatTimeoutMs = heartbeatTimeoutMs,
            MaxHeartbeatFailures = maxHeartbeatFailures,
            UseRaftConsensus = useRaftConsensus,
            RaftDataDirectory = raftDataDirectory,
            RaftElectionTimeoutMinMs = raftElectionTimeoutMinMs,
            RaftElectionTimeoutMaxMs = raftElectionTimeoutMaxMs,
            RaftHeartbeatIntervalMs = raftHeartbeatIntervalMs,
            RaftPeerDiscoveryTimeoutSeconds = raftPeerDiscoveryTimeoutSeconds,
            AutoRebalanceEnabled = autoRebalanceEnabled,
            RebalanceCheckIntervalSeconds = rebalanceCheckIntervalSeconds,
            RebalanceImbalanceThreshold = rebalanceImbalanceThreshold,
            ReassignmentThrottleBytesPerSec = reassignmentThrottleBytesPerSec,
            ReassignmentMaxConcurrent = reassignmentMaxConcurrent
        };
    }
}
