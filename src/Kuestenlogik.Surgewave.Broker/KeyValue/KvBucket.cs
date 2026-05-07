using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// A single KV bucket backed by a compacted Surgewave topic.
/// In-memory ConcurrentDictionary for fast reads; changelog persisted to topic.
/// </summary>
public sealed class KvBucket : IDisposable
{
    private readonly string _name;
    private readonly KvBucketConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger? _logger;
    private readonly string _backingTopic;

    // Current state: key -> list of entries (most recent last)
    private readonly ConcurrentDictionary<string, List<KvEntry>> _store = new();
    private long _revision;
    private readonly Lock _revisionLock = new();

    // Watch subscribers
    private readonly ConcurrentDictionary<Guid, Channel<KvEntry>> _watchers = new();
    private readonly ConcurrentDictionary<Guid, string?> _watcherKeyFilters = new();

    // TTL expiry
    private readonly CancellationTokenSource _ttlCts = new();
    private readonly Task? _ttlTask;

    private bool _disposed;

    public string Name => _name;
    public KvBucketConfig Config => _config;
    public string BackingTopic => _backingTopic;

    public KvBucket(
        string name,
        KvBucketConfig config,
        LogManager logManager,
        ILogger? logger = null)
    {
        _name = name;
        _config = config;
        _logManager = logManager;
        _logger = logger;
        _backingTopic = $"_kv_{name}";

        // Start TTL expiry worker if configured
        if (_config.Ttl.HasValue)
        {
            _ttlTask = Task.Run(() => TtlExpiryWorkerAsync(_ttlCts.Token));
        }
    }

    /// <summary>
    /// Create the backing compacted topic if it does not already exist.
    /// </summary>
    public async Task EnsureBackingTopicAsync(CancellationToken cancellationToken = default)
    {
        var existing = _logManager.GetTopicMetadata(_backingTopic);
        if (existing is not null)
            return;

        var topicConfig = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "compact",
        };

        await _logManager.CreateTopicAsync(_backingTopic, partitionCount: 1, config: topicConfig, cancellationToken: cancellationToken);
        _logger?.LogInformation("Created backing topic {Topic} for KV bucket {Bucket}", _backingTopic, _name);
    }

    /// <summary>
    /// Replay the backing topic to restore in-memory state (called on broker restart).
    /// </summary>
    public async Task RestoreAsync(CancellationToken cancellationToken = default)
    {
        var topicPartition = new TopicPartition { Topic = _backingTopic, Partition = 0 };
        var log = _logManager.GetLog(topicPartition);
        if (log is null)
            return;

        long offset = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var batches = await _logManager.ReadBatchesAsync(topicPartition, offset, cancellationToken: cancellationToken);
            if (batches.Count == 0)
                break;

            foreach (var batch in batches)
            {
                var entry = DeserializeEntry(batch);
                if (entry is null)
                    continue;

                ApplyEntry(entry);
                offset++;
            }
        }

        _logger?.LogInformation("Restored KV bucket {Bucket}: {Keys} keys, revision {Revision}", _name, _store.Count, _revision);
    }

    /// <summary>
    /// Get the value for a key. Returns null if the key does not exist or has been deleted.
    /// </summary>
    public KvEntry? Get(string key)
    {
        if (!_store.TryGetValue(key, out var entries) || entries.Count == 0)
            return null;

        var latest = entries[^1];
        return latest.Operation == KvOperation.Delete ? null : latest;
    }

    /// <summary>
    /// Put a value for a key. Returns the new entry with incremented revision.
    /// </summary>
    public async Task<KvEntry> PutAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        if (value.Length > _config.MaxValueSize)
            throw new ArgumentException($"Value size {value.Length} exceeds maximum {_config.MaxValueSize}");

        long revision;
        lock (_revisionLock)
        {
            revision = ++_revision;
        }

        var entry = new KvEntry(_name, key, value, revision, DateTimeOffset.UtcNow, KvOperation.Put);

        // Persist to backing topic
        var serialized = SerializeEntry(entry);
        var topicPartition = new TopicPartition { Topic = _backingTopic, Partition = 0 };
        await _logManager.AppendBatchAsync(topicPartition, serialized, cancellationToken);

        // Apply to in-memory store
        ApplyEntry(entry);

        // Notify watchers
        await NotifyWatchersAsync(entry);

        return entry;
    }

    /// <summary>
    /// Delete a key (creates a tombstone entry). Returns the tombstone entry.
    /// </summary>
    public async Task<KvEntry> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        long revision;
        lock (_revisionLock)
        {
            revision = ++_revision;
        }

        var entry = new KvEntry(_name, key, [], revision, DateTimeOffset.UtcNow, KvOperation.Delete);

        // Persist tombstone to backing topic
        var serialized = SerializeEntry(entry);
        var topicPartition = new TopicPartition { Topic = _backingTopic, Partition = 0 };
        await _logManager.AppendBatchAsync(topicPartition, serialized, cancellationToken);

        // Apply to in-memory store
        ApplyEntry(entry);

        // Notify watchers
        await NotifyWatchersAsync(entry);

        return entry;
    }

    /// <summary>
    /// List all keys that currently have a non-deleted value.
    /// </summary>
    public IReadOnlyList<string> Keys()
    {
        var result = new List<string>();
        foreach (var kvp in _store)
        {
            if (kvp.Value.Count > 0 && kvp.Value[^1].Operation != KvOperation.Delete)
            {
                result.Add(kvp.Key);
            }
        }
        return result;
    }

    /// <summary>
    /// Get the history of a key (up to MaxHistoryPerKey entries).
    /// </summary>
    public IReadOnlyList<KvEntry> History(string key)
    {
        if (!_store.TryGetValue(key, out var entries))
            return [];

        return entries.ToList();
    }

    /// <summary>
    /// Subscribe to changes for a specific key (or all keys if key is null).
    /// Returns a channel that receives change notifications.
    /// </summary>
    public (Guid SubscriptionId, ChannelReader<KvEntry> Reader) Watch(string? key = null)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateBounded<KvEntry>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = false,
            SingleWriter = false,
        });

        _watchers[id] = channel;
        _watcherKeyFilters[id] = key;

        return (id, channel.Reader);
    }

    /// <summary>
    /// Unsubscribe a watcher.
    /// </summary>
    public void Unwatch(Guid subscriptionId)
    {
        if (_watchers.TryRemove(subscriptionId, out var channel))
        {
            channel.Writer.TryComplete();
        }
        _watcherKeyFilters.TryRemove(subscriptionId, out _);
    }

    /// <summary>
    /// Purge all history for a key (remove from store entirely).
    /// </summary>
    public async Task PurgeAsync(string key, CancellationToken cancellationToken = default)
    {
        long revision;
        lock (_revisionLock)
        {
            revision = ++_revision;
        }

        var entry = new KvEntry(_name, key, [], revision, DateTimeOffset.UtcNow, KvOperation.Purge);

        // Persist purge marker
        var serialized = SerializeEntry(entry);
        var topicPartition = new TopicPartition { Topic = _backingTopic, Partition = 0 };
        await _logManager.AppendBatchAsync(topicPartition, serialized, cancellationToken);

        // Remove from in-memory store
        _store.TryRemove(key, out _);

        // Notify watchers
        await NotifyWatchersAsync(entry);
    }

    /// <summary>
    /// Get bucket info (key count, total size estimate).
    /// </summary>
    public KvBucketInfo GetInfo()
    {
        var keyCount = 0;
        long totalValueBytes = 0;
        foreach (var kvp in _store)
        {
            if (kvp.Value.Count > 0 && kvp.Value[^1].Operation != KvOperation.Delete)
            {
                keyCount++;
                totalValueBytes += kvp.Value[^1].Value.Length;
            }
        }

        return new KvBucketInfo(_name, keyCount, totalValueBytes, _revision, _config);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private void ApplyEntry(KvEntry entry)
    {
        if (entry.Operation == KvOperation.Purge)
        {
            _store.TryRemove(entry.Key, out _);
            return;
        }

        _store.AddOrUpdate(
            entry.Key,
            _ => [entry],
            (_, existing) =>
            {
                lock (existing)
                {
                    existing.Add(entry);
                    // Trim history to max
                    while (existing.Count > _config.MaxHistoryPerKey)
                    {
                        existing.RemoveAt(0);
                    }
                }
                return existing;
            });

        // Update revision high watermark
        lock (_revisionLock)
        {
            if (entry.Revision > _revision)
                _revision = entry.Revision;
        }
    }

    private async Task NotifyWatchersAsync(KvEntry entry)
    {
        foreach (var kvp in _watchers)
        {
            // Apply key filter
            if (_watcherKeyFilters.TryGetValue(kvp.Key, out var filter) && filter is not null && filter != entry.Key)
                continue;

            await kvp.Value.Writer.WriteAsync(entry).ConfigureAwait(false);
        }
    }

    private async Task TtlExpiryWorkerAsync(CancellationToken cancellationToken)
    {
        var ttl = _config.Ttl!.Value;
        var checkInterval = TimeSpan.FromSeconds(Math.Max(1, ttl.TotalSeconds / 4));

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(checkInterval, cancellationToken);

                var now = DateTimeOffset.UtcNow;
                foreach (var kvp in _store)
                {
                    if (kvp.Value.Count == 0) continue;
                    var latest = kvp.Value[^1];
                    if (latest.Operation != KvOperation.Delete && now - latest.Created > ttl)
                    {
                        await DeleteAsync(kvp.Key, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "TTL expiry worker error in bucket {Bucket}", _name);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Serialization — simple binary format for backing topic entries
    //
    // Format: [1 byte op] [2 byte key-len] [key bytes] [4 byte val-len] [val bytes]
    //         [8 byte revision] [8 byte timestamp-ticks]
    // -----------------------------------------------------------------------

    private static byte[] SerializeEntry(KvEntry entry)
    {
        var keyBytes = Encoding.UTF8.GetBytes(entry.Key);
        var totalSize = 1 + 2 + keyBytes.Length + 4 + entry.Value.Length + 8 + 8;
        var buffer = new byte[totalSize];
        var pos = 0;

        buffer[pos++] = (byte)entry.Operation;

        BinaryPrimitives.WriteInt16BigEndian(buffer.AsSpan(pos), (short)keyBytes.Length);
        pos += 2;
        keyBytes.CopyTo(buffer.AsSpan(pos));
        pos += keyBytes.Length;

        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(pos), entry.Value.Length);
        pos += 4;
        entry.Value.CopyTo(buffer.AsSpan(pos));
        pos += entry.Value.Length;

        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(pos), entry.Revision);
        pos += 8;

        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(pos), entry.Created.UtcTicks);

        return buffer;
    }

    private KvEntry? DeserializeEntry(byte[] data)
    {
        try
        {
            var span = data.AsSpan();
            var pos = 0;

            var op = (KvOperation)span[pos++];

            var keyLen = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
            pos += 2;
            var key = Encoding.UTF8.GetString(span.Slice(pos, keyLen));
            pos += keyLen;

            var valLen = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
            pos += 4;
            var value = span.Slice(pos, valLen).ToArray();
            pos += valLen;

            var revision = BinaryPrimitives.ReadInt64BigEndian(span[pos..]);
            pos += 8;

            var ticks = BinaryPrimitives.ReadInt64BigEndian(span[pos..]);
            var created = new DateTimeOffset(ticks, TimeSpan.Zero);

            return new KvEntry(_name, key, value, revision, created, op);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to deserialize KV entry in bucket {Bucket}", _name);
            return null;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _ttlCts.Cancel();
        _ttlCts.Dispose();

        foreach (var kvp in _watchers)
        {
            kvp.Value.Writer.TryComplete();
        }
        _watchers.Clear();
    }
}

/// <summary>
/// Summary information about a KV bucket.
/// </summary>
public sealed record KvBucketInfo(
    string Name,
    int KeyCount,
    long TotalValueBytes,
    long LatestRevision,
    KvBucketConfig Config);
