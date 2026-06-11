using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Persistence layer for <see cref="PartitionManifest"/>. One manifest
/// per disaggregated topic-partition; the store is the single source of
/// truth for "what offset ranges live in the object store right now".
///
/// Two implementations ship in P2a:
/// - <see cref="InMemoryPartitionManifestStore"/> for tests + embedded broker.
/// - <see cref="FilePartitionManifestStore"/> for standalone brokers
///   (atomic JSON write per partition). KRaft-replicated variant is a
///   later iteration, see ADR-014 §"Per-partition manifest".
///
/// Implementations must be safe for concurrent calls across different
/// partitions; same-partition mutations are serialised by the store so
/// callers get optimistic-concurrency semantics via
/// <see cref="PartitionManifest.Version"/> instead of needing external
/// locks.
/// </summary>
public interface IPartitionManifestStore
{
    /// <summary>
    /// Read the current manifest for <paramref name="partition"/>. Returns
    /// an empty manifest (no stream objects, version 0) when the partition
    /// has never been written to.
    /// </summary>
    ValueTask<PartitionManifest> GetAsync(TopicPartition partition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically append <paramref name="newObject"/> to the manifest. Bumps
    /// <see cref="PartitionManifest.Version"/> on success and returns the
    /// updated manifest. Throws <see cref="InvalidOperationException"/> if
    /// the new object's offset range overlaps the manifest tail
    /// (disaggregated commits must be strictly monotonic).
    /// </summary>
    ValueTask<PartitionManifest> AppendObjectAsync(
        TopicPartition partition,
        StreamObjectRef newObject,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// List every partition that has a manifest on this store. Used at
    /// broker startup to rehydrate the in-memory manifest cache (and by
    /// the cost-reporting endpoint to enumerate disaggregated storage
    /// usage).
    /// </summary>
    ValueTask<IReadOnlyList<TopicPartition>> ListPartitionsAsync(CancellationToken cancellationToken = default);
}
