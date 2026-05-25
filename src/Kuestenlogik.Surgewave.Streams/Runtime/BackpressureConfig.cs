using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Configuration for backpressure and flow control.
/// </summary>
public sealed class BackpressureConfig : IValidatableConfig
{
    [Range(1, int.MaxValue)]
    public int MaxBufferedRecords { get; init; } = 10_000;

    public BackpressureStrategy Strategy { get; init; } = BackpressureStrategy.Block;

    public TimeSpan MaxWaitTime { get; init; } = TimeSpan.FromSeconds(5);

    public bool PauseConsumerOnHighWatermark { get; init; } = true;

    [Range(0.0, 1.0)]
    public double HighWatermarkRatio { get; init; } = 0.8;

    [Range(0.0, 1.0)]
    public double LowWatermarkRatio { get; init; } = 0.5;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (LowWatermarkRatio >= HighWatermarkRatio)
            errors.Add($"{nameof(LowWatermarkRatio)} ({LowWatermarkRatio}) must be less than " +
                       $"{nameof(HighWatermarkRatio)} ({HighWatermarkRatio}).");

        if (MaxWaitTime <= TimeSpan.Zero)
            errors.Add($"{nameof(MaxWaitTime)}: must be positive.");

        return errors;
    }
}

/// <summary>
/// Strategy for handling backpressure when the buffer is full.
/// </summary>
public enum BackpressureStrategy
{
    /// <summary>Block the producer until space is available.</summary>
    Block,
    /// <summary>Drop the oldest record in the buffer.</summary>
    DropOldest,
    /// <summary>Drop the newest (incoming) record.</summary>
    DropNewest
}
