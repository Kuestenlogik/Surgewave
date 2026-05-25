namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Represents a topic and partition combination
/// </summary>
public readonly record struct TopicPartition
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }

    public override string ToString() => $"{Topic}-{Partition}";
}
