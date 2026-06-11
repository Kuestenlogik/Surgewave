using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// In-process manifest store. Used in tests, in the embedded broker, and
/// as the cache layer behind <see cref="FilePartitionManifestStore"/>. A
/// <see cref="SemaphoreSlim"/> per partition serialises append calls
/// without blocking unrelated partitions.
/// </summary>
public sealed class InMemoryPartitionManifestStore : IPartitionManifestStore
{
    private readonly ConcurrentDictionary<TopicPartition, PartitionManifest> _manifests = new();
    private readonly ConcurrentDictionary<TopicPartition, SemaphoreSlim> _locks = new();

    public ValueTask<PartitionManifest> GetAsync(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var manifest = _manifests.TryGetValue(partition, out var existing)
            ? existing
            : PartitionManifest.Empty(partition);
        return ValueTask.FromResult(manifest);
    }

    public async ValueTask<PartitionManifest> AppendObjectAsync(
        TopicPartition partition,
        StreamObjectRef newObject,
        CancellationToken cancellationToken = default)
    {
        var gate = _locks.GetOrAdd(partition, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var current = _manifests.TryGetValue(partition, out var existing)
                ? existing
                : PartitionManifest.Empty(partition);
            var next = current.AppendObject(newObject);
            _manifests[partition] = next;
            return next;
        }
        finally
        {
            gate.Release();
        }
    }

    public ValueTask<IReadOnlyList<TopicPartition>> ListPartitionsAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyList<TopicPartition> snapshot = _manifests.Keys.ToArray();
        return ValueTask.FromResult(snapshot);
    }
}
