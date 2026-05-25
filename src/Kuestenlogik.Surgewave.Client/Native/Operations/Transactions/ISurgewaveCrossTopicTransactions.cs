namespace Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;

/// <summary>
/// Interface for cross-topic transaction operations.
/// </summary>
public interface ISurgewaveCrossTopicTransactions
{
    /// <summary>
    /// Begin a new cross-topic transaction with optional timeout.
    /// </summary>
    Task<SurgewaveCrossTopicTransaction> BeginAsync(TimeSpan? timeout = null, CancellationToken ct = default);
}
