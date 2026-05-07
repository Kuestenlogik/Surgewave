namespace Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;

/// <summary>
/// A specific invariant that <see cref="LinearizabilityChecker"/> found violated while
/// replaying a <see cref="History"/>. Each subtype carries enough context to locate the
/// offending records in the original recording — copy the description verbatim into an
/// xunit assertion message to produce an actionable failure.
/// </summary>
public abstract record Violation
{
    /// <summary>Short human-readable summary of what went wrong.</summary>
    public abstract string Description { get; }

    public sealed override string ToString() => Description;
}

/// <summary>
/// Two different values were acknowledged at the same (topic, partition, offset).
/// Violates append-only log semantics — a produce that returns offset N means the
/// record at N is that value, never another.
/// </summary>
public sealed record OffsetCollisionViolation(
    string Topic,
    int Partition,
    long Offset,
    byte[] AckedValueA,
    byte[] AckedValueB) : Violation
{
    public override string Description =>
        $"Offset collision at {Topic}:{Partition}@{Offset} — two different values were " +
        $"both acknowledged to the same offset (lengths {AckedValueA.Length} vs {AckedValueB.Length}).";
}

/// <summary>
/// A consume at offset N returned a different value than what a prior produce at the
/// same offset was acknowledged with. Violates read-after-write consistency per
/// partition.
/// </summary>
public sealed record DivergentReadViolation(
    string Topic,
    int Partition,
    long Offset,
    byte[] AckedValue,
    byte[] ReadValue) : Violation
{
    public override string Description =>
        $"Divergent read at {Topic}:{Partition}@{Offset} — consumer saw a value that " +
        $"differs from the acknowledged produce (lengths {AckedValue.Length} vs {ReadValue.Length}).";
}

/// <summary>
/// Two consumers reading the same (topic, partition, offset) saw different payloads.
/// Even without a matching produce acknowledgement (the produce might have been
/// dropped from the history), the log at a given offset must be the same for every
/// reader.
/// </summary>
public sealed record InconsistentReadsViolation(
    string Topic,
    int Partition,
    long Offset,
    byte[] ValueA,
    byte[] ValueB) : Violation
{
    public override string Description =>
        $"Inconsistent reads at {Topic}:{Partition}@{Offset} — two consumers saw " +
        $"different values at the same offset (lengths {ValueA.Length} vs {ValueB.Length}).";
}

/// <summary>
/// A record whose produce was acknowledged with a specific offset could not be
/// consumed from that offset by any reader in the history. This is only a violation
/// when the history contains a subsequent read-attempt at or beyond that offset from
/// the partition's leader; the checker reports it so tests can decide whether the
/// scenario ever tried to read far enough to observe the record.
/// </summary>
public sealed record PotentiallyLostWriteViolation(
    string Topic,
    int Partition,
    long Offset,
    byte[] AckedValue) : Violation
{
    public override string Description =>
        $"Potentially lost write at {Topic}:{Partition}@{Offset} — produce was " +
        $"acknowledged but no consumer in the history observed the offset despite " +
        $"reading past it.";
}

/// <summary>
/// Offsets acknowledged by produce calls skip a range that no consumer subsequently
/// observed, suggesting the partition's log may have holes. This is only reported
/// when the range is strictly enclosed by acknowledged offsets — terminal gaps at
/// the end of the history are tolerated (records may simply not have been consumed yet).
/// </summary>
public sealed record OffsetGapViolation(
    string Topic,
    int Partition,
    long GapStart,
    long GapEnd) : Violation
{
    public override string Description =>
        $"Offset gap at {Topic}:{Partition} — no produce acknowledged between " +
        $"offsets {GapStart} and {GapEnd}, but both sides were acknowledged.";
}
