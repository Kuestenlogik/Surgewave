namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Batch of messages for efficient I/O
/// </summary>
public sealed record MessageBatch
{
    public required long BaseOffset { get; init; }
    public required int PartitionId { get; init; }
    public required List<Message> Messages { get; init; }
    public required long Timestamp { get; init; }

    public int MessageCount => Messages.Count;
    public long LastOffset => BaseOffset + MessageCount - 1;
}
