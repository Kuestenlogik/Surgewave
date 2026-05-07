namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Result containing messages from a partition.
/// </summary>
public sealed record MessagesResult(
    string Topic,
    int Partition,
    long Offset,
    long HighWatermark,
    long LogStartOffset,
    IReadOnlyList<MessageDetail> Messages);

/// <summary>
/// A single message from a partition.
/// </summary>
public sealed record MessageDetail(
    long Offset,
    DateTimeOffset Timestamp,
    string? Key,
    string? Value,
    string? ValueBase64,
    IReadOnlyDictionary<string, string> Headers,
    bool IsCompressed,
    int ValueSizeBytes);
