using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Metadata for a single Streams application instance.
/// Tracks which partitions and state stores this instance owns.
/// </summary>
public sealed class StreamsMetadata
{
    public HostInfo HostInfo { get; }
    public IReadOnlySet<string> StateStoreNames { get; }
    public IReadOnlySet<TopicPartition> TopicPartitions { get; }

    public StreamsMetadata(
        HostInfo hostInfo,
        IEnumerable<string> stateStoreNames,
        IEnumerable<TopicPartition> topicPartitions)
    {
        HostInfo = hostInfo;
        StateStoreNames = new HashSet<string>(stateStoreNames);
        TopicPartitions = new HashSet<TopicPartition>(topicPartitions);
    }

    /// <summary>
    /// Whether this instance owns the given state store.
    /// </summary>
    public bool HasStateStore(string name) => StateStoreNames.Contains(name);

    /// <summary>
    /// Whether this instance owns the given partition.
    /// </summary>
    public bool HasPartition(TopicPartition partition) => TopicPartitions.Contains(partition);

    /// <summary>
    /// Whether this instance owns a partition with the given number (any topic).
    /// </summary>
    public bool HasPartitionNumber(int partition)
        => TopicPartitions.Any(tp => tp.Partition == partition);

    public override string ToString()
        => $"StreamsMetadata({HostInfo}, stores=[{string.Join(",", StateStoreNames)}], partitions={TopicPartitions.Count})";
}
