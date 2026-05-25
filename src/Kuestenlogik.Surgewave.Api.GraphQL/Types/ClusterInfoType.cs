namespace Kuestenlogik.Surgewave.Api.GraphQL.Types;

/// <summary>
/// Represents cluster information in the GraphQL schema.
/// </summary>
public sealed class ClusterInfoType
{
    /// <summary>
    /// Broker identifier.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Broker host address.
    /// </summary>
    public required string Host { get; init; }

    /// <summary>
    /// Broker Kafka wire protocol port.
    /// </summary>
    public int Port { get; init; }

    /// <summary>
    /// Total number of topics.
    /// </summary>
    public int TopicCount { get; init; }

    /// <summary>
    /// Total number of partitions across all topics.
    /// </summary>
    public int PartitionCount { get; init; }
}
