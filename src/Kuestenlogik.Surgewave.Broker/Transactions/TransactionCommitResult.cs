namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Result of committing a cross-topic transaction.
/// </summary>
public sealed record TransactionCommitResult(
    string TransactionId,
    bool Success,
    int TopicsWritten,
    int MessagesWritten,
    TimeSpan Duration,
    string? Error,
    Dictionary<string, long>? Offsets);
