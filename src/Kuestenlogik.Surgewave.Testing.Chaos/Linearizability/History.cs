using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;

/// <summary>
/// Thread-safe recorder for a sequence of <see cref="ChaosOperation"/> events emitted by
/// producers and consumers during a chaos run. The broker is treated as a black box; the
/// history captures only what each client saw, with enough context for a subsequent
/// <see cref="LinearizabilityChecker"/> to verify the broker's per-partition ordering and
/// durability guarantees.
/// </summary>
/// <remarks>
/// The underlying store is a <see cref="ConcurrentQueue{T}"/> so <see cref="Record"/> is
/// lock-free on the hot path. Readers (<see cref="Operations"/>, <see cref="ForPartition"/>)
/// snapshot the queue, which is cheap relative to the checker's work.
/// </remarks>
public sealed class History
{
    private readonly ConcurrentQueue<ChaosOperation> _operations = new();

    /// <summary>
    /// All events in the order they were recorded. The enumeration reflects the
    /// queue at call-time; further <see cref="Record"/> calls after the snapshot
    /// will not show up in the returned list.
    /// </summary>
    public IReadOnlyList<ChaosOperation> Operations => [.. _operations];

    /// <summary>Adds an event to the history. Thread-safe.</summary>
    public void Record(ChaosOperation op)
    {
        ArgumentNullException.ThrowIfNull(op);
        _operations.Enqueue(op);
    }

    /// <summary>Distinct (topic, partition) pairs that appear in the history.</summary>
    public IReadOnlySet<(string Topic, int Partition)> Partitions()
    {
        var set = new HashSet<(string, int)>();
        foreach (var op in _operations)
        {
            switch (op)
            {
                case ProduceInvoke pi: set.Add((pi.Topic, pi.Partition)); break;
                case ProduceOk po: set.Add((po.Topic, po.Partition)); break;
                case ProduceFail pf: set.Add((pf.Topic, pf.Partition)); break;
                case ConsumeInvoke ci: set.Add((ci.Topic, ci.Partition)); break;
                case ConsumeOk co: set.Add((co.Topic, co.Partition)); break;
                case ConsumeFail cf: set.Add((cf.Topic, cf.Partition)); break;
            }
        }
        return set;
    }

    /// <summary>Events that target the given (topic, partition), preserving relative order.</summary>
    public IEnumerable<ChaosOperation> ForPartition(string topic, int partition)
    {
        foreach (var op in _operations)
        {
            var match = op switch
            {
                ProduceInvoke pi => pi.Topic == topic && pi.Partition == partition,
                ProduceOk po => po.Topic == topic && po.Partition == partition,
                ProduceFail pf => pf.Topic == topic && pf.Partition == partition,
                ConsumeInvoke ci => ci.Topic == topic && ci.Partition == partition,
                ConsumeOk co => co.Topic == topic && co.Partition == partition,
                ConsumeFail cf => cf.Topic == topic && cf.Partition == partition,
                _ => false
            };
            if (match) yield return op;
        }
    }

    /// <summary>Total number of events currently in the history.</summary>
    public int Count => _operations.Count;
}
