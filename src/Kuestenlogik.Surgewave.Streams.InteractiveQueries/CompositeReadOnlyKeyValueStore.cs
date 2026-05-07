using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Transparent composite key-value store that automatically routes queries
/// to the correct Streams application instance (local or remote).
///
/// For local keys (owned by this instance): queries the local store directly.
/// For remote keys: uses RemoteQueryClient to query the owning instance via TCP.
/// </summary>
public sealed class CompositeReadOnlyKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly string _storeName;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly StreamsMetadataState _metadataState;
    private readonly Func<string, IStateStore?> _localStoreResolver;

    internal CompositeReadOnlyKeyValueStore(
        string storeName,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        StreamsMetadataState metadataState,
        Func<string, IStateStore?> localStoreResolver)
    {
        _storeName = storeName;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _metadataState = metadataState;
        _localStoreResolver = localStoreResolver;
    }

    /// <summary>
    /// Get a value by key. Automatically routes to the correct instance.
    /// Returns default if the key is not found anywhere.
    /// </summary>
    public async Task<TValue?> GetAsync(TKey key, CancellationToken ct = default)
    {
        var keyBytes = _keySerde.Serialize(key);
        var partitionCount = _metadataState.GetMaxPartitionCount();

        if (partitionCount <= 0)
        {
            // No metadata available — try local store
            return GetLocal(key);
        }

        var partition = (int)(QueryProtocol.Murmur2(keyBytes) % (uint)partitionCount);

        if (_metadataState.IsLocalPartition(partition))
        {
            return GetLocal(key);
        }

        // Find remote instance
        var metadata = _metadataState.FindByPartitionAndStore(partition, _storeName);
        if (metadata == null) return default;

        using var client = new RemoteQueryClient(metadata.HostInfo);
        var valueBytes = await client.GetRawAsync(_storeName, keyBytes, ct);
        if (valueBytes == null) return default;
        return _valueSerde.Deserialize(valueBytes);
    }

    /// <summary>
    /// Get a range of entries. Queries all instances that have the store and merges results.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000", Justification = "RemoteQueryClient is disposed via using declaration")]
    public async Task<IReadOnlyList<KeyValue<TKey, TValue>>> RangeAsync(
        TKey from, TKey to, CancellationToken ct = default)
    {
        var fromBytes = _keySerde.Serialize(from);
        var toBytes = _keySerde.Serialize(to);
        var result = new List<KeyValue<TKey, TValue>>();

        var instances = _metadataState.ForStore(_storeName);
        foreach (var instance in instances)
        {
            if (_metadataState.IsLocal(instance.HostInfo))
            {
                var store = ResolveLocalStore();
                if (store != null)
                {
                    foreach (var kv in store.Range(from, to))
                        result.Add(kv);
                }
            }
            else
            {
                using var client = new RemoteQueryClient(instance.HostInfo);
                var entries = await client.RangeRawAsync(_storeName, fromBytes, toBytes, ct);
                foreach (var (k, v) in entries)
                {
                    result.Add(new KeyValue<TKey, TValue>(
                        _keySerde.Deserialize(k),
                        _valueSerde.Deserialize(v)));
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Get all entries from all instances.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000", Justification = "RemoteQueryClient is disposed via using declaration")]
    public async Task<IReadOnlyList<KeyValue<TKey, TValue>>> AllAsync(CancellationToken ct = default)
    {
        var result = new List<KeyValue<TKey, TValue>>();

        var instances = _metadataState.ForStore(_storeName);
        foreach (var instance in instances)
        {
            if (_metadataState.IsLocal(instance.HostInfo))
            {
                var store = ResolveLocalStore();
                if (store != null)
                {
                    foreach (var kv in store.All())
                        result.Add(kv);
                }
            }
            else
            {
                using var client = new RemoteQueryClient(instance.HostInfo);
                var entries = await client.AllRawAsync(_storeName, ct);
                foreach (var (k, v) in entries)
                {
                    result.Add(new KeyValue<TKey, TValue>(
                        _keySerde.Deserialize(k),
                        _valueSerde.Deserialize(v)));
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Get the total approximate entry count across all instances.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000", Justification = "RemoteQueryClient is disposed via using declaration")]
    public async Task<long> ApproximateNumEntriesAsync(CancellationToken ct = default)
    {
        long total = 0;

        var instances = _metadataState.ForStore(_storeName);
        foreach (var instance in instances)
        {
            if (_metadataState.IsLocal(instance.HostInfo))
            {
                var store = ResolveLocalStore();
                if (store != null)
                    total += store.ApproximateNumEntries;
            }
            else
            {
                using var client = new RemoteQueryClient(instance.HostInfo);
                total += await client.CountAsync(_storeName, ct);
            }
        }

        return total;
    }

    private TValue? GetLocal(TKey key)
    {
        var store = ResolveLocalStore();
        return store != null ? store.Get(key) : default;
    }

    private IKeyValueStore<TKey, TValue>? ResolveLocalStore()
    {
        return _localStoreResolver(_storeName) as IKeyValueStore<TKey, TValue>;
    }
}
