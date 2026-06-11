using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Ordered list of <see cref="StreamObjectRef"/> for one partition,
/// plus the partition identity. Persisted in the cluster metadata
/// store (KRaft-backed today, see ADR-014). The list is kept sorted
/// by <see cref="StreamObjectRef.FirstOffset"/> so a range query is
/// a binary search.
/// </summary>
public sealed record PartitionManifest
{
    public required TopicPartition Partition { get; init; }

    /// <summary>Stream objects in increasing-offset order.</summary>
    public required IReadOnlyList<StreamObjectRef> Objects { get; init; }

    /// <summary>
    /// Monotonic version counter — bumped on every manifest mutation.
    /// Used by the offset-commit protocol for optimistic concurrency
    /// (a stale committer is rejected so two parallel commits don't
    /// silently overwrite each other's refs).
    /// </summary>
    public required long Version { get; init; }

    public static PartitionManifest Empty(TopicPartition partition) => new()
    {
        Partition = partition,
        Objects = [],
        Version = 0,
    };

    /// <summary>
    /// Smallest offset still in the manifest, or null if the manifest is
    /// empty. Reads below this offset fall back to the WAL (for
    /// <c>disaggregated-wal</c>) or return <c>OFFSET_OUT_OF_RANGE</c>
    /// (for <c>disaggregated-stateless</c>).
    /// </summary>
    public long? FirstOffset => Objects.Count == 0 ? null : Objects[0].FirstOffset;

    /// <summary>Largest offset stored remotely.</summary>
    public long? LastOffset => Objects.Count == 0 ? null : Objects[^1].LastOffset;

    /// <summary>Find the stream object containing the given offset, or null.</summary>
    public StreamObjectRef? Locate(long offset)
    {
        var lo = 0;
        var hi = Objects.Count - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >>> 1;
            var obj = Objects[mid];
            if (obj.Contains(offset)) return obj;
            if (offset < obj.FirstOffset) hi = mid - 1;
            else lo = mid + 1;
        }
        return null;
    }

    /// <summary>
    /// Return a manifest with <paramref name="newObject"/> appended and
    /// <see cref="Version"/> bumped. Throws when the new ref's
    /// <see cref="StreamObjectRef.FirstOffset"/> overlaps the existing
    /// tail — disaggregated commits must be strictly monotonic.
    /// </summary>
    public PartitionManifest AppendObject(StreamObjectRef newObject)
    {
        if (Objects.Count > 0 && newObject.FirstOffset <= Objects[^1].LastOffset)
        {
            throw new InvalidOperationException(
                $"Stream object offset range [{newObject.FirstOffset}, {newObject.LastOffset}] "
                + $"overlaps the manifest's existing tail (ends at {Objects[^1].LastOffset}). "
                + "Disaggregated commits must be strictly monotonic.");
        }
        var next = new List<StreamObjectRef>(Objects.Count + 1);
        next.AddRange(Objects);
        next.Add(newObject);
        return this with { Objects = next, Version = Version + 1 };
    }
}
