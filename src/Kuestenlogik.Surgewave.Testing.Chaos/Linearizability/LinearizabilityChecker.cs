namespace Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;

/// <summary>
/// Inspects a recorded <see cref="History"/> for per-partition consistency anomalies.
/// The checker does not aim for full Kyle-Kingsbury-style linearizability — Kafka-like
/// brokers split ordering across partitions, which makes per-partition checking both
/// sufficient and tractable. It verifies the invariants the broker advertises:
/// <list type="number">
///   <item>Each (topic, partition, offset) maps to at most one value (append-only log).</item>
///   <item>A consumer at offset N sees the value that was acknowledged to a producer at offset N.</item>
///   <item>Two consumers at the same offset see the same value (consistent reads).</item>
///   <item>Acknowledged offsets have no interior gaps that consumers would observe.</item>
/// </list>
/// Produces (<see cref="ProduceFail"/>) are treated as "unknown outcome" — if a later
/// consume turns up at the same offset with the produced value, the write succeeded;
/// otherwise the checker does not count the fail against the broker.
/// </summary>
public sealed class LinearizabilityChecker
{
    /// <summary>
    /// Runs all invariant checks on the snapshot of the history. Thread-safe; the
    /// history itself is iterated once.
    /// </summary>
    public LinearizabilityResult Check(History history)
    {
        ArgumentNullException.ThrowIfNull(history);

        var violations = new List<Violation>();
        foreach (var (topic, partition) in history.Partitions())
        {
            CheckPartition(history, topic, partition, violations);
        }

        return new LinearizabilityResult(violations.Count == 0, violations);
    }

    private static void CheckPartition(History history, string topic, int partition, List<Violation> violations)
    {
        // Per-offset append-only log built from acknowledged produces. The list-per-offset
        // lets us detect duplicates; collisions are reported as separate violations even when
        // the same offset gets >2 different values acknowledged.
        var ackedByOffset = new Dictionary<long, List<byte[]>>();
        var consumedByOffset = new Dictionary<long, List<byte[]>>();
        var maxConsumedOffset = long.MinValue;

        foreach (var op in history.ForPartition(topic, partition))
        {
            switch (op)
            {
                case ProduceOk ok:
                {
                    if (!ackedByOffset.TryGetValue(ok.Offset, out var list))
                    {
                        list = [];
                        ackedByOffset[ok.Offset] = list;
                    }
                    list.Add(ok.Value);
                    break;
                }
                case ConsumeOk co:
                {
                    if (!consumedByOffset.TryGetValue(co.Offset, out var list))
                    {
                        list = [];
                        consumedByOffset[co.Offset] = list;
                    }
                    list.Add(co.Value);
                    if (co.Offset > maxConsumedOffset) maxConsumedOffset = co.Offset;
                    break;
                }
            }
        }

        // Invariant 1: at most one distinct acknowledged value per offset.
        foreach (var (offset, values) in ackedByOffset)
        {
            for (var i = 1; i < values.Count; i++)
            {
                if (!values[0].SequenceEqual(values[i]))
                {
                    violations.Add(new OffsetCollisionViolation(topic, partition, offset, values[0], values[i]));
                }
            }
        }

        // Invariant 2 & 3: consumed values must agree with each other per offset and
        // with the acknowledged produce at that offset when one exists.
        foreach (var (offset, readValues) in consumedByOffset)
        {
            for (var i = 1; i < readValues.Count; i++)
            {
                if (!readValues[0].SequenceEqual(readValues[i]))
                {
                    violations.Add(new InconsistentReadsViolation(topic, partition, offset, readValues[0], readValues[i]));
                }
            }

            if (ackedByOffset.TryGetValue(offset, out var acked) && acked.Count > 0)
            {
                if (!acked[0].SequenceEqual(readValues[0]))
                {
                    violations.Add(new DivergentReadViolation(topic, partition, offset, acked[0], readValues[0]));
                }
            }
        }

        // Invariant 4: offsets acknowledged by produces form a contiguous range. Gaps
        // strictly enclosed within the acked range indicate the partition lost data at
        // those offsets. Terminal gaps at the tail are ignored — records can still be
        // in flight when the history ends.
        if (ackedByOffset.Count >= 2)
        {
            var sortedOffsets = ackedByOffset.Keys.Order().ToArray();
            for (var i = 1; i < sortedOffsets.Length; i++)
            {
                var prev = sortedOffsets[i - 1];
                var next = sortedOffsets[i];
                if (next > prev + 1)
                {
                    violations.Add(new OffsetGapViolation(topic, partition, prev, next));
                }
            }
        }

        // Potentially-lost-write check: an acknowledged offset that no consumer ever
        // returned, despite consumers reading past it. Only flag when we actually read
        // far enough for the gap to matter.
        foreach (var (offset, ackedValues) in ackedByOffset)
        {
            if (offset > maxConsumedOffset) continue;
            if (consumedByOffset.ContainsKey(offset)) continue;
            violations.Add(new PotentiallyLostWriteViolation(topic, partition, offset, ackedValues[0]));
        }
    }
}

/// <summary>Outcome of <see cref="LinearizabilityChecker.Check"/>.</summary>
public sealed record LinearizabilityResult(bool IsValid, IReadOnlyList<Violation> Violations);
