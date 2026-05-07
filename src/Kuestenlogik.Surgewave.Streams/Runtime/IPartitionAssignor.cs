namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Strategy interface for assigning partitions to stream threads.
/// </summary>
public interface IPartitionAssignor
{
    /// <summary>
    /// Assigns partitions across the given number of threads.
    /// Returns a dictionary mapping thread index to its assigned partitions.
    /// </summary>
    Dictionary<int, List<TopicPartition>> Assign(
        IReadOnlyList<TopicPartition> partitions,
        int numThreads,
        IReadOnlyDictionary<TopicPartition, int>? previousAssignment);
}
