using System.Collections.Concurrent;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Persistent foreign key subscription store that wraps an <see cref="IKeyValueStore{TKey,TValue}"/> for durability.
/// Maintains FK→PK mappings in a backing key-value store using a write-through in-memory cache for reads.
/// </summary>
internal sealed class PersistentForeignKeySubscriptionStore<TPrimaryKey, TForeignKey> : IDisposable
    where TPrimaryKey : notnull
    where TForeignKey : notnull
{
    private readonly IKeyValueStore<string, string> _backingStore;
    private readonly Func<TPrimaryKey, string> _pkSerializer;
    private readonly Func<TForeignKey, string> _fkSerializer;
    private readonly Func<string, TPrimaryKey> _pkDeserializer;
    private readonly Func<string, TForeignKey> _fkDeserializer;

    // Write-through in-memory caches
    private readonly ConcurrentDictionary<string, HashSet<string>> _fkToPks = new();
    private readonly ConcurrentDictionary<string, string> _pkToFk = new();

    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private bool _disposed;

    private const string FkPrefix = "fk:";
    private const string PkPrefix = "pk:";

    /// <summary>
    /// Initializes a new persistent FK subscription store backed by the given key-value store.
    /// Uses JSON serialization for string keys; for non-string types supply custom serializers.
    /// </summary>
    public PersistentForeignKeySubscriptionStore(IKeyValueStore<string, string> backingStore)
        : this(backingStore, pk => JsonSerializer.Serialize(pk), fk => JsonSerializer.Serialize(fk),
               s => JsonSerializer.Deserialize<TPrimaryKey>(s)!, s => JsonSerializer.Deserialize<TForeignKey>(s)!)
    {
    }

    /// <summary>
    /// Initializes a new persistent FK subscription store with custom key serializers.
    /// </summary>
    public PersistentForeignKeySubscriptionStore(
        IKeyValueStore<string, string> backingStore,
        Func<TPrimaryKey, string> pkSerializer,
        Func<TForeignKey, string> fkSerializer,
        Func<string, TPrimaryKey> pkDeserializer,
        Func<string, TForeignKey> fkDeserializer)
    {
        _backingStore = backingStore ?? throw new ArgumentNullException(nameof(backingStore));
        _pkSerializer = pkSerializer ?? throw new ArgumentNullException(nameof(pkSerializer));
        _fkSerializer = fkSerializer ?? throw new ArgumentNullException(nameof(fkSerializer));
        _pkDeserializer = pkDeserializer ?? throw new ArgumentNullException(nameof(pkDeserializer));
        _fkDeserializer = fkDeserializer ?? throw new ArgumentNullException(nameof(fkDeserializer));
    }

    /// <summary>
    /// Records that the given primary key subscribes to the given foreign key.
    /// Writes through to the backing store.
    /// </summary>
    public void Subscribe(TPrimaryKey pk, TForeignKey fk)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var pkStr = _pkSerializer(pk);
        var fkStr = _fkSerializer(fk);

        _lock.EnterWriteLock();
        try
        {
            // Update PK → FK mapping
            _pkToFk[pkStr] = fkStr;
            _backingStore.Put(PkPrefix + pkStr, fkStr);

            // Update FK → PKs mapping
            var subscribers = _fkToPks.GetOrAdd(fkStr, _ => []);
            subscribers.Add(pkStr);
            _backingStore.Put(FkPrefix + fkStr, SerializePkSet(subscribers));
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes the subscription for the given primary key.
    /// Writes through to the backing store.
    /// </summary>
    public void Unsubscribe(TPrimaryKey pk, TForeignKey? oldFk)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var pkStr = _pkSerializer(pk);

        _lock.EnterWriteLock();
        try
        {
            _pkToFk.TryRemove(pkStr, out _);
            _backingStore.Delete(PkPrefix + pkStr);

            if (oldFk != null)
            {
                var fkStr = _fkSerializer(oldFk);
                if (_fkToPks.TryGetValue(fkStr, out var subscribers))
                {
                    subscribers.Remove(pkStr);
                    if (subscribers.Count == 0)
                    {
                        _fkToPks.TryRemove(fkStr, out _);
                        _backingStore.Delete(FkPrefix + fkStr);
                    }
                    else
                    {
                        _backingStore.Put(FkPrefix + fkStr, SerializePkSet(subscribers));
                    }
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Updates the subscription when a PK changes its FK reference.
    /// </summary>
    public void UpdateSubscription(TPrimaryKey pk, TForeignKey? oldFk, TForeignKey newFk)
    {
        if (oldFk != null && !oldFk.Equals(newFk))
        {
            Unsubscribe(pk, oldFk);
        }

        Subscribe(pk, newFk);
    }

    /// <summary>
    /// Returns all primary keys that currently reference the given foreign key.
    /// Falls back to in-memory cache first; reads from backing store on cache miss.
    /// </summary>
    public IReadOnlySet<TPrimaryKey> GetSubscribers(TForeignKey fk)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var fkStr = _fkSerializer(fk);

        _lock.EnterReadLock();
        try
        {
            if (_fkToPks.TryGetValue(fkStr, out var cached))
            {
                return cached.Select(s => _pkDeserializer(s)).ToHashSet();
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        // Cache miss: load from backing store
        _lock.EnterWriteLock();
        try
        {
            // Double-check after acquiring write lock
            if (_fkToPks.TryGetValue(fkStr, out var cached))
            {
                return cached.Select(s => _pkDeserializer(s)).ToHashSet();
            }

            var stored = _backingStore.Get(FkPrefix + fkStr);
            if (stored == null)
                return new HashSet<TPrimaryKey>();

            var pkStrSet = DeserializePkSet(stored);
            _fkToPks[fkStr] = pkStrSet;
            return pkStrSet.Select(s => _pkDeserializer(s)).ToHashSet();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Returns the current foreign key for the given primary key, or null if none.
    /// </summary>
    public TForeignKey? GetForeignKey(TPrimaryKey pk)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var pkStr = _pkSerializer(pk);

        _lock.EnterReadLock();
        try
        {
            if (_pkToFk.TryGetValue(pkStr, out var fkStr))
                return _fkDeserializer(fkStr);
        }
        finally
        {
            _lock.ExitReadLock();
        }

        // Cache miss: load from backing store
        var stored = _backingStore.Get(PkPrefix + pkStr);
        if (stored == null)
            return default;

        _lock.EnterWriteLock();
        try
        {
            _pkToFk[pkStr] = stored;
        }
        finally
        {
            _lock.ExitWriteLock();
        }

        return _fkDeserializer(stored);
    }

    /// <summary>Gets the total number of PK→FK subscriptions currently tracked.</summary>
    public long Count
    {
        get
        {
            _lock.EnterReadLock();
            try { return _pkToFk.Count; }
            finally { _lock.ExitReadLock(); }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _lock.Dispose();
    }

    private static string SerializePkSet(HashSet<string> pkSet)
        => JsonSerializer.Serialize(pkSet);

    private static HashSet<string> DeserializePkSet(string json)
        => JsonSerializer.Deserialize<HashSet<string>>(json) ?? [];
}
