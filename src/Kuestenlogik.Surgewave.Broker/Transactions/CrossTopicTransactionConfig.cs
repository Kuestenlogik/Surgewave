using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Configuration for cross-topic transactions.
/// </summary>
public sealed class CrossTopicTransactionConfig : IValidatableConfig
{
    /// <summary>Enable cross-topic transactions. Default: true.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Default transaction timeout. Default: 60 seconds.</summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Maximum allowed transaction timeout. Default: 15 minutes.</summary>
    public TimeSpan MaxTimeout { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>Maximum number of pending writes per transaction. Default: 10000.</summary>
    [Range(1, int.MaxValue)]
    public int MaxPendingWrites { get; set; } = 10_000;

    /// <summary>Interval for cleaning up expired transactions in seconds. Default: 30.</summary>
    [Range(1, int.MaxValue)]
    public int CleanupIntervalSeconds { get; set; } = 30;

    /// <summary>Internal topic for transaction log persistence. Default: __cross_topic_txn_log.</summary>
    [Required]
    [MinLength(1)]
    public string TransactionLogTopic { get; set; } = "__cross_topic_txn_log";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (DefaultTimeout <= TimeSpan.Zero)
            errors.Add($"{nameof(DefaultTimeout)}: must be positive.");

        if (MaxTimeout <= TimeSpan.Zero)
            errors.Add($"{nameof(MaxTimeout)}: must be positive.");

        if (DefaultTimeout > MaxTimeout)
            errors.Add($"{nameof(DefaultTimeout)} ({DefaultTimeout}) must not exceed " +
                       $"{nameof(MaxTimeout)} ({MaxTimeout}).");

        return errors;
    }
}
