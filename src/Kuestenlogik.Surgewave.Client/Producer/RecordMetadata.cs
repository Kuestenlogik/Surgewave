namespace Kuestenlogik.Surgewave.Client.Producer;

/// <summary>
/// Result of a send operation
/// </summary>
public sealed record RecordMetadata(
    string Topic,
    int Partition,
    long Offset,
    long Timestamp);
