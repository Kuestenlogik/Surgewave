namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Represents a topic-partition pair.
/// </summary>
public sealed record TopicPartition(string Topic, int Partition);
