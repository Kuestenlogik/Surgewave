using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Linq;

/// <summary>
/// Configuration for Surgewave LINQ queries.
/// </summary>
public sealed class SurgewaveQueryOptions
{
    /// <summary>
    /// Surgewave broker address (e.g., "localhost:9092").
    /// </summary>
    public required string BootstrapServers { get; init; }

    /// <summary>
    /// Maximum number of messages to scan per partition per query.
    /// Default: 10_000. Set to -1 for unlimited.
    /// </summary>
    public int MaxScanMessages { get; init; } = 10_000;

    /// <summary>
    /// Read timeout per partition.
    /// </summary>
    public TimeSpan ReadTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether to scan partitions in parallel.
    /// </summary>
    public bool ParallelPartitionScan { get; init; } = true;

    /// <summary>
    /// Maximum degree of parallelism for partition scanning.
    /// Default: number of processors.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Custom value deserializer. Default: JSON.
    /// </summary>
    public Type? DeserializerType { get; init; }
}
