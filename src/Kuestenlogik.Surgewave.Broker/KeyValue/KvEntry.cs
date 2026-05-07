namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Represents a single versioned entry in a KV bucket.
/// </summary>
public sealed record KvEntry(
    string Bucket,
    string Key,
    byte[] Value,
    long Revision,
    DateTimeOffset Created,
    KvOperation Operation)
{
    /// <summary>
    /// Revision delta (used in watch notifications).
    /// </summary>
    public long Delta => 0;
}
