using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Client.Partitioning;

/// <summary>
/// Configuration for <see cref="PriorityPartitioner"/>.
/// </summary>
public sealed class PriorityPartitionerOptions
{
    /// <summary>
    /// Number of physical partitions allocated to each priority level.
    /// Default: 1 partition per priority, giving 3 total (indices 0, 1, 2).
    /// </summary>
    public int PartitionsPerPriority { get; init; } = 1;

    /// <summary>
    /// Inner strategy used to spread load within the partition range for each priority.
    /// Defaults to round-robin.
    /// </summary>
    public IPartitionStrategy InnerStrategy { get; init; } = Partitioner.RoundRobin;
}

/// <summary>
/// A partition strategy that maps messages to separate partition ranges based on their
/// <c>surgewave-priority</c> header (<see cref="MessagePriority"/>).
/// <para>
/// Partition layout (with <c>PartitionsPerPriority = P</c>):
/// <list type="bullet">
///   <item><description>High   → partitions  0 … P-1</description></item>
///   <item><description>Normal → partitions  P … 2P-1</description></item>
///   <item><description>Low    → partitions 2P … 3P-1</description></item>
/// </list>
/// The strategy always ignores the caller-supplied <c>partitionCount</c> argument and
/// routes within the pre-configured ranges. Use
/// <see cref="SelectPartition(byte[],Dictionary{string,byte[]},int)"/> to route with a
/// known priority extracted from message headers.
/// </para>
/// </summary>
public sealed class PriorityPartitioner : IPartitionStrategy
{
    private readonly PriorityPartitionerOptions _options;
    private readonly int _partitionsPerPriority;
    private readonly int _totalPartitions;

    /// <summary>
    /// Creates a <see cref="PriorityPartitioner"/> with default options
    /// (1 partition per priority = 3 total partitions).
    /// </summary>
    public PriorityPartitioner() : this(new PriorityPartitionerOptions()) { }

    /// <summary>
    /// Creates a <see cref="PriorityPartitioner"/> with the supplied options.
    /// </summary>
    public PriorityPartitioner(PriorityPartitionerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.PartitionsPerPriority < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "PartitionsPerPriority must be >= 1.");

        _options = options;
        _partitionsPerPriority = options.PartitionsPerPriority;
        _totalPartitions = _partitionsPerPriority * 3; // High + Normal + Low
    }

    /// <summary>
    /// Total number of physical partitions managed by this strategy
    /// (3 × <see cref="PriorityPartitionerOptions.PartitionsPerPriority"/>).
    /// </summary>
    public int TotalPartitions => _totalPartitions;

    /// <summary>
    /// Returns the first partition index for the given priority level.
    /// </summary>
    public int GetPartitionRangeStart(MessagePriority priority)
        => (int)priority * _partitionsPerPriority;

    /// <summary>
    /// Selects a partition using the <see cref="MessagePriority"/> read from the supplied headers.
    /// Falls back to <see cref="MessagePriority.Normal"/> when no header is present.
    /// </summary>
    public int SelectPartition(byte[]? key, Dictionary<string, byte[]>? headers, int partitionCount)
    {
        var priority = headers.GetPriority();
        return SelectPartitionForPriority(key, priority);
    }

    /// <summary>
    /// Selects a partition for the given priority level.
    /// </summary>
    public int SelectPartitionForPriority(byte[]? key, MessagePriority priority)
    {
        var rangeStart = GetPartitionRangeStart(priority);
        var innerPartition = _options.InnerStrategy.SelectPartition(key, _partitionsPerPriority);
        return rangeStart + innerPartition;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// When called without a header dictionary (the <see cref="IPartitionStrategy"/> contract),
    /// the strategy defaults to <see cref="MessagePriority.Normal"/>.
    /// Use <see cref="SelectPartition(byte[],Dictionary{string,byte[]},int)"/> to route with
    /// an explicit priority header.
    /// </remarks>
    public int SelectPartition(byte[]? key, int partitionCount)
        => SelectPartitionForPriority(key, MessagePriority.Normal);
}
