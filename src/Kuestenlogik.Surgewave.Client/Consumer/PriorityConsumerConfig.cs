using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Client.Partitioning;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Configuration for priority-aware consumer polling behaviour.
/// </summary>
/// <remarks>
/// <para>
/// Priority consumers poll partitions at different frequencies according to their weights.
/// During each poll cycle a budget of <c>HighWeight + NormalWeight + LowWeight</c> poll tokens
/// is distributed. High-priority partitions consume <see cref="HighWeight"/> tokens, normal
/// consume <see cref="NormalWeight"/>, and low consume <see cref="LowWeight"/>.
/// </para>
/// <para>
/// The consumer drains all available high-priority partitions before moving to normal, and
/// normal before low, within each budget cycle.
/// </para>
/// </remarks>
public sealed class PriorityConsumerConfig : IValidatableConfig
{
    /// <summary>Default poll weight for high-priority partitions.</summary>
    public const int DefaultHighWeight = 3;

    /// <summary>Default poll weight for normal-priority partitions.</summary>
    public const int DefaultNormalWeight = 2;

    /// <summary>Default poll weight for low-priority partitions.</summary>
    public const int DefaultLowWeight = 1;

    /// <summary>
    /// Relative poll frequency for high-priority partitions.
    /// Must be >= 1. Default: <see cref="DefaultHighWeight"/> (3).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int HighWeight { get; init; } = DefaultHighWeight;

    /// <summary>
    /// Relative poll frequency for normal-priority partitions.
    /// Must be >= 1. Default: <see cref="DefaultNormalWeight"/> (2).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int NormalWeight { get; init; } = DefaultNormalWeight;

    /// <summary>
    /// Relative poll frequency for low-priority partitions.
    /// Must be >= 1. Default: <see cref="DefaultLowWeight"/> (1).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int LowWeight { get; init; } = DefaultLowWeight;

    /// <summary>
    /// Number of partitions per priority level (must match the producer-side
    /// <see cref="PriorityPartitionerOptions.PartitionsPerPriority"/>).
    /// Default: 1.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int PartitionsPerPriority { get; init; } = 1;

    /// <summary>
    /// When <see langword="true"/> the consumer drains all messages from higher-priority
    /// partitions before processing any lower-priority messages within the same poll budget.
    /// Default: <see langword="true"/>.
    /// </summary>
    public bool DrainHighBeforeLow { get; init; } = true;

    /// <summary>
    /// Returns the ordered sequence of <see cref="MessagePriority"/> values to poll,
    /// reflecting the configured weights.
    /// </summary>
    /// <returns>
    /// An enumerable that yields each priority level the number of times equal to its weight.
    /// The order is always High → Normal → Low, consistent with <see cref="DrainHighBeforeLow"/>.
    /// </returns>
    public IEnumerable<MessagePriority> BuildPollSchedule()
    {
        for (int i = 0; i < HighWeight; i++)   yield return MessagePriority.High;
        for (int i = 0; i < NormalWeight; i++) yield return MessagePriority.Normal;
        for (int i = 0; i < LowWeight; i++)    yield return MessagePriority.Low;
    }

    /// <summary>
    /// Returns the starting partition index for the given priority level.
    /// </summary>
    public int GetPartitionRangeStart(MessagePriority priority)
        => (int)priority * PartitionsPerPriority;

    /// <summary>
    /// Returns all partition indices that belong to the given priority level.
    /// </summary>
    public IEnumerable<int> GetPartitionsForPriority(MessagePriority priority)
    {
        var start = GetPartitionRangeStart(priority);
        for (int i = 0; i < PartitionsPerPriority; i++)
            yield return start + i;
    }

    /// <summary>
    /// Total number of physical partitions across all priority levels
    /// (3 × <see cref="PartitionsPerPriority"/>).
    /// </summary>
    public int TotalPartitions => PartitionsPerPriority * 3;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
