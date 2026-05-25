namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Cluster configuration options.
/// </summary>
public sealed class ClusterOptions
{
    /// <summary>
    /// Enable cluster mode.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Cluster ID. Auto-generated if not specified.
    /// </summary>
    public string? ClusterId { get; set; }

    /// <summary>
    /// Other broker nodes in the cluster.
    /// Format: "brokerId:host:port" or "host:port".
    /// </summary>
    public List<string> Nodes { get; set; } = [];

    /// <summary>
    /// Replication port for inter-broker communication.
    /// Default: 9094.
    /// </summary>
    public int ReplicationPort { get; set; } = 9094;

    /// <summary>
    /// Use Raft consensus for controller election.
    /// </summary>
    public bool UseRaft { get; set; }

    /// <summary>
    /// Rack ID for rack-aware replica placement.
    /// </summary>
    public string? Rack { get; set; }
}
