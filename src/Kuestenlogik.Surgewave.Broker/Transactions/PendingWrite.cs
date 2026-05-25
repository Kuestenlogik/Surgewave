namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// A write buffered within a cross-topic transaction, not yet committed.
/// </summary>
public sealed class PendingWrite
{
    /// <summary>Target topic.</summary>
    public required string Topic { get; init; }

    /// <summary>Target partition within the topic.</summary>
    public int Partition { get; init; }

    /// <summary>Optional message key.</summary>
    public byte[]? Key { get; init; }

    /// <summary>Message value (payload).</summary>
    public required byte[] Value { get; init; }

    /// <summary>Optional message headers.</summary>
    public Dictionary<string, string>? Headers { get; init; }
}
