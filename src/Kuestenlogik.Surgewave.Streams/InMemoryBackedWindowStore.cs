using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Abstract base class for <see cref="IWindowStore{TKey,TValue}"/> implementations that
/// keep the active window data in an in-memory <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// cache. Subclasses can add durability (write-ahead log, snapshots, changelog topic)
/// via the <see cref="OnPut"/> hook.
///
/// <para>
/// Why this class exists: both <see cref="InMemoryWindowStore{TKey,TValue}"/> (volatile)
/// and <see cref="PersistentWindowStore{TKey,TValue}"/> (WAL-backed) historically used the
/// same <c>ConcurrentDictionary&lt;(TKey, long), TValue&gt;</c> cache and identical
/// Fetch/FetchAll/ExpireOldWindows logic — with the durability code copy-pasted on top.
/// The base class eliminates that duplication: Put, Fetch, FetchAll and the retention
/// sweep live here exactly once; subclasses only implement what genuinely differs.
/// </para>
///
/// <para>Unlike <see cref="ByteBackedKeyValueStore{TKey,TValue}"/>, this is not a
/// byte-oriented backend abstraction — the underlying storage is always the in-memory
/// dictionary. There is no composition-path ("IByteWindowBackend") because the only two
/// existing window store implementations share the same storage model and none of the
/// hypothetical byte-oriented backends (RocksDB, SQLite, mapped-file) exist for windowed
/// state yet. If that ever changes, a composition path can be added analogously.</para>
/// </summary>
public abstract class InMemoryBackedWindowStore<TKey, TValue> : IWindowStore<TKey, TValue>
    where TKey : notnull
{
    /// <summary>
    /// The live window-keyed cache. Subclasses read from and write to this dictionary
    /// (e.g. when replaying a WAL on startup).
    /// </summary>
    protected ConcurrentDictionary<(TKey Key, long WindowStart), TValue> Store { get; } = new();

    /// <summary>Window duration — determines the <see cref="Window.EndMs"/> for fetch results.</summary>
    protected TimeSpan WindowSize { get; }

    /// <summary>How long to retain window entries before they are expired by the retention sweep.</summary>
    protected TimeSpan RetentionPeriod { get; }

    /// <summary>Processor context handed in at <see cref="Init"/> time.</summary>
    protected ProcessorContext? Context { get; private set; }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public abstract bool Persistent { get; }

    /// <summary>
    /// Initializes a new base window store. Subclasses forward their constructor arguments
    /// to this constructor before registering their own (serde, log path, ...) state.
    /// </summary>
    protected InMemoryBackedWindowStore(string name, TimeSpan windowSize, TimeSpan retentionPeriod)
    {
        Name = name;
        WindowSize = windowSize;
        RetentionPeriod = retentionPeriod;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Subclasses with durability should override this to open their WAL and replay it into
    /// <see cref="Store"/> before returning. Always call <c>base.Init(context)</c> first so
    /// <see cref="Context"/> is set.
    /// </remarks>
    public virtual void Init(ProcessorContext context)
    {
        Context = context;
    }

    /// <inheritdoc />
    public void Put(TKey key, TValue value, long windowStartMs)
    {
        Store[(key, windowStartMs)] = value;
        OnPut(key, value, windowStartMs);
        ExpireOldWindows();
    }

    /// <summary>
    /// Hook called after every successful <see cref="Put"/> so subclasses can persist the
    /// mutation (e.g. append to a write-ahead log). The default implementation is a no-op,
    /// which is the correct behaviour for volatile/in-memory-only stores.
    /// </summary>
    protected virtual void OnPut(TKey key, TValue value, long windowStartMs) { }

    /// <inheritdoc />
    public TValue? Fetch(TKey key, long windowStartMs)
    {
        Store.TryGetValue((key, windowStartMs), out var value);
        return value;
    }

    /// <inheritdoc />
    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> Fetch(TKey key, long timeFrom, long timeTo)
    {
        return Store
            .Where(kv => EqualityComparer<TKey>.Default.Equals(kv.Key.Key, key) &&
                         kv.Key.WindowStart >= timeFrom &&
                         kv.Key.WindowStart <= timeTo)
            .Select(kv => ToWindowed(kv.Key, kv.Value));
    }

    /// <inheritdoc />
    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> FetchAll(long timeFrom, long timeTo)
    {
        return Store
            .Where(kv => kv.Key.WindowStart >= timeFrom && kv.Key.WindowStart <= timeTo)
            .Select(kv => ToWindowed(kv.Key, kv.Value));
    }

    private KeyValue<Windowed<TKey>, TValue> ToWindowed(
        (TKey Key, long WindowStart) storeKey, TValue value)
    {
        var window = new Window(storeKey.WindowStart, storeKey.WindowStart + (long)WindowSize.TotalMilliseconds);
        return new KeyValue<Windowed<TKey>, TValue>(new Windowed<TKey>(storeKey.Key, window), value);
    }

    /// <summary>
    /// Removes all entries whose <see cref="Window.StartMs"/> is older than
    /// (now - <see cref="RetentionPeriod"/>). Called after every <see cref="Put"/>.
    /// </summary>
    protected void ExpireOldWindows()
    {
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)RetentionPeriod.TotalMilliseconds;
        foreach (var key in Store.Keys)
        {
            if (key.WindowStart < cutoff)
                Store.TryRemove(key, out _);
        }
    }

    /// <inheritdoc />
    public virtual void Flush() { }

    /// <inheritdoc />
    public virtual void Close()
    {
        Store.Clear();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes managed resources by closing the store. Subclasses that hold additional
    /// unmanaged resources (file handles, streams) should override and call
    /// <c>base.Dispose(disposing)</c>.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
            Close();
    }
}
