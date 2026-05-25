namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Sticky partition assignment that minimizes partition movements during rebalance.
/// Keeps partitions on their current thread when possible, only moving partitions
/// when threads are over/under-loaded.
/// </summary>
public sealed class StickyAssignor : IPartitionAssignor
{
    public static readonly StickyAssignor Instance = new();

    public Dictionary<int, List<TopicPartition>> Assign(
        IReadOnlyList<TopicPartition> partitions,
        int numThreads,
        IReadOnlyDictionary<TopicPartition, int>? previousAssignment)
    {
        var result = new Dictionary<int, List<TopicPartition>>();
        for (var i = 0; i < numThreads; i++)
            result[i] = [];

        if (previousAssignment == null || previousAssignment.Count == 0)
        {
            // No previous assignment — fall back to round-robin
            for (var i = 0; i < partitions.Count; i++)
                result[i % numThreads].Add(partitions[i]);
            return result;
        }

        var unassigned = new List<TopicPartition>();

        // Phase 1: Keep partitions on their previous thread if valid
        foreach (var tp in partitions)
        {
            if (previousAssignment.TryGetValue(tp, out var prevThread) && prevThread < numThreads)
            {
                result[prevThread].Add(tp);
            }
            else
            {
                unassigned.Add(tp);
            }
        }

        // Phase 2: Balance — compute ideal load
        var idealMax = (int)Math.Ceiling((double)partitions.Count / numThreads);

        // Phase 3: Move excess partitions from overloaded threads to unassigned pool
        for (var i = 0; i < numThreads; i++)
        {
            while (result[i].Count > idealMax)
            {
                var last = result[i][^1];
                result[i].RemoveAt(result[i].Count - 1);
                unassigned.Add(last);
            }
        }

        // Phase 4: Assign unassigned partitions to least-loaded threads
        foreach (var tp in unassigned)
        {
            var minThread = 0;
            var minCount = result[0].Count;
            for (var i = 1; i < numThreads; i++)
            {
                if (result[i].Count < minCount)
                {
                    minThread = i;
                    minCount = result[i].Count;
                }
            }
            result[minThread].Add(tp);
        }

        return result;
    }
}
