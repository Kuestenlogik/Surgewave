using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated;

/// <summary>
/// Manifest store that persists one JSON file per partition under a
/// configured data directory. Writes are atomic (temp file + rename) so
/// a crash mid-write never corrupts the manifest on disk — the broker
/// either sees the old version or the new one, never a half-written
/// file.
///
/// File layout under <c>&lt;dataDir&gt;/disaggregated/manifests/</c>:
/// <code>
///   manifests/
///     orders__0.json
///     orders__1.json
///     events__0.json
/// </code>
/// The double-underscore is deliberate — Kafka topic names allow dots
/// but not underscores in our convention, so <c>__</c> can never appear
/// in a topic name and is therefore a safe partition separator.
///
/// All reads go through an in-memory cache so hot-path reads of the
/// manifest don't touch disk; writes go to disk first, then update the
/// cache.
/// </summary>
public sealed class FilePartitionManifestStore : IPartitionManifestStore
{
    private readonly string _root;
    private readonly InMemoryPartitionManifestStore _cache = new();
    private readonly ConcurrentDictionary<TopicPartition, SemaphoreSlim> _writeLocks = new();
    private bool _hydrated;
    private readonly SemaphoreSlim _hydrateLock = new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public FilePartitionManifestStore(string dataDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDirectory);
        _root = Path.Combine(Path.GetFullPath(dataDirectory), "disaggregated", "manifests");
        Directory.CreateDirectory(_root);
    }

    public async ValueTask<PartitionManifest> GetAsync(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        await EnsureHydratedAsync(cancellationToken).ConfigureAwait(false);
        return await _cache.GetAsync(partition, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<PartitionManifest> AppendObjectAsync(
        TopicPartition partition,
        StreamObjectRef newObject,
        CancellationToken cancellationToken = default)
    {
        await EnsureHydratedAsync(cancellationToken).ConfigureAwait(false);

        var gate = _writeLocks.GetOrAdd(partition, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Cache-Append performs the overlap check + version bump; that
            // result is the manifest we serialise to disk. Disk is the
            // durable side, but cache stays in lock-step so concurrent
            // readers don't see a manifest that exists on disk but not
            // in cache.
            var next = await _cache.AppendObjectAsync(partition, newObject, cancellationToken).ConfigureAwait(false);
            await WriteAtomicAsync(partition, next, cancellationToken).ConfigureAwait(false);
            return next;
        }
        finally
        {
            gate.Release();
        }
    }

    public async ValueTask<IReadOnlyList<TopicPartition>> ListPartitionsAsync(CancellationToken cancellationToken = default)
    {
        await EnsureHydratedAsync(cancellationToken).ConfigureAwait(false);
        return await _cache.ListPartitionsAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EnsureHydratedAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _hydrated)) return;
        await _hydrateLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_hydrated) return;
            foreach (var file in Directory.EnumerateFiles(_root, "*.json"))
            {
                var manifest = await ReadManifestAsync(file, cancellationToken).ConfigureAwait(false);
                if (manifest is null) continue;

                // Replay each ref through the in-memory store's Append so
                // the overlap-check invariants are re-validated on every
                // boot. A corrupted on-disk manifest fails loudly here
                // instead of silently returning bad data on later reads.
                foreach (var obj in manifest.Objects)
                {
                    await _cache.AppendObjectAsync(manifest.Partition, obj, cancellationToken).ConfigureAwait(false);
                }
            }
            Volatile.Write(ref _hydrated, true);
        }
        finally
        {
            _hydrateLock.Release();
        }
    }

    private static async Task<PartitionManifest?> ReadManifestAsync(string path, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<PartitionManifest>(stream, JsonOptions, cancellationToken).ConfigureAwait(false);
    }

    private async Task WriteAtomicAsync(TopicPartition partition, PartitionManifest manifest, CancellationToken cancellationToken)
    {
        var finalPath = Path.Combine(_root, FileNameFor(partition));
        var tempPath = finalPath + ".tmp";
        await using (var stream = File.Create(tempPath))
        {
            await JsonSerializer.SerializeAsync(stream, manifest, JsonOptions, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        File.Move(tempPath, finalPath, overwrite: true);
    }

    internal static string FileNameFor(TopicPartition partition) =>
        $"{partition.Topic}__{partition.Partition}.json";
}
