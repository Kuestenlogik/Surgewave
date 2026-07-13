namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Represents a broker node in the cluster.
/// </summary>
public sealed record BrokerNode
{
    public required int BrokerId { get; init; }
    public required string Host { get; init; }
    public required int Port { get; init; }
    public string? Rack { get; init; }

    /// <summary>
    /// #60 Inc3 — the inter-broker protocol level this broker advertised at registration:
    /// <see cref="InterBrokerProtocolFeature.KafkaWire"/> (0) or <see cref="InterBrokerProtocolFeature.Native"/> (1).
    /// A broker that never advertised the feature (an older build) reads as
    /// <see cref="InterBrokerProtocolFeature.KafkaWire"/>. The cluster-wide finalized level is the MIN of
    /// this across all brokers — see <see cref="ClusterState.FinalizedInterBrokerProtocol"/>.
    /// </summary>
    public short InterBrokerProtocol { get; init; } = InterBrokerProtocolFeature.KafkaWire;

    /// <summary>
    /// Endpoint for inter-broker communication.
    /// Defaults to Port + 1000 if not explicitly set.
    /// </summary>
    public int ReplicationPort
    {
        get => _replicationPort ?? Port + 1000;
        init => _replicationPort = value;
    }
    private readonly int? _replicationPort;

    public string Endpoint => $"{Host}:{Port}";
    public string ReplicationEndpoint => $"{Host}:{ReplicationPort}";

    public override string ToString() => $"Broker-{BrokerId}@{Endpoint}";

    // Records use all properties for equality by default.
    // Override to only use BrokerId for dictionary key behavior.
    public bool Equals(BrokerNode? other) => other is not null && BrokerId == other.BrokerId;
    public override int GetHashCode() => BrokerId.GetHashCode();
}
