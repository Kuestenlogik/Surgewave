namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Record batch header compatible with Kafka format
/// </summary>
public readonly record struct RecordBatchHeader
{
    public required long BaseOffset { get; init; }
    public required int BatchLength { get; init; }
    public required int PartitionLeaderEpoch { get; init; }
    public required byte Magic { get; init; }
    public required uint Crc { get; init; }
    public required short Attributes { get; init; }
    public required int LastOffsetDelta { get; init; }
    public required long BaseTimestamp { get; init; }
    public required long MaxTimestamp { get; init; }
    public required long ProducerId { get; init; }
    public required short ProducerEpoch { get; init; }
    public required int BaseSequence { get; init; }
    public required int RecordCount { get; init; }
}
