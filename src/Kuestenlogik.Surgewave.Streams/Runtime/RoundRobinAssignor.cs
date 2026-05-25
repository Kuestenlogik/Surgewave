namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Simple round-robin partition assignment. Distributes partitions evenly across threads.
/// </summary>
public sealed class RoundRobinAssignor : IPartitionAssignor
{
    public static readonly RoundRobinAssignor Instance = new();

    public Dictionary<int, List<TopicPartition>> Assign(
        IReadOnlyList<TopicPartition> partitions,
        int numThreads,
        IReadOnlyDictionary<TopicPartition, int>? previousAssignment)
    {
        var result = new Dictionary<int, List<TopicPartition>>();
        for (var i = 0; i < numThreads; i++)
            result[i] = [];

        for (var i = 0; i < partitions.Count; i++)
            result[i % numThreads].Add(partitions[i]);

        return result;
    }
}
