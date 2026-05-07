using Kuestenlogik.Surgewave.Streams.Monitoring;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Abstract base class for persistent <see cref="IKeyValueStore{TKey,TValue}"/> implementations
/// that store serialized byte arrays in an underlying backend (RocksDB, SQLite, mapped file,
/// etc.). The generic <typeparamref name="TKey"/> / <typeparamref name="TValue"/> path is
/// implemented once here — subclasses only provide byte-level backend operations.
///
/// <para>
/// Design: template method pattern. The public <see cref="IKeyValueStore{TKey,TValue}"/>
/// surface handles serialization, metrics and approximate-entry counting; the protected
/// abstract methods (<see cref="GetBytes"/>, <see cref="PutBytes"/>, <see cref="DeleteBytes"/>,
/// <see cref="RangeBytes"/>, <see cref="AllBytes"/>, <see cref="InitBackend"/>,
/// <see cref="FlushBackend"/>, <see cref="CloseBackend"/>) describe a minimal byte-oriented
/// backend contract.
/// </para>
///
/// <para>
/// Subclasses keep the public API unchanged — e.g. <c>RocksDbKeyValueStore&lt;TKey,TValue&gt;</c>
/// retains its original constructor and all its existing call sites continue to work. What
/// goes away is the ~200 lines of serialize/deserialize/metrics boilerplate that was duplicated
/// across every backend.
/// </para>
/// </summary>
public abstract class ByteBackedKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    /// <summary>Serde used to encode/decode keys before/after hitting the backend.</summary>
    protected ISerde<TKey> KeySerde { get; }

    /// <summary>Serde used to encode/decode values before/after hitting the backend.</summary>
    protected ISerde<TValue> ValueSerde { get; }

    /// <summary>
    /// Metrics sink for this store. <c>null</c> until <see cref="Init"/> has been called.
    /// Subclasses can record additional metrics on top of the Get/Put/Delete counters
    /// recorded automatically by the base class.
    /// </summary>
    protected StateStoreMetrics? Metrics { get; private set; }

    /// <summary>Processor context handed in at <see cref="Init"/> time.</summary>
    protected ProcessorContext? Context { get; private set; }

    private long _approximateEntries;

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public abstract bool Persistent { get; }

    /// <summary>
    /// Best-effort approximation of the number of entries currently in the store. The default
    /// implementation returns the internal interlocked counter maintained via
    /// <see cref="IncrementEntries"/> / <see cref="DecrementEntries"/> / <see cref="AddEntries"/>.
    /// Backends that can query the underlying store cheaply (e.g. SQLite COUNT, RocksDB
    /// `estimate-num-keys`) can override to return a fresher value.
    /// </summary>
    public virtual long ApproximateNumEntries => Interlocked.Read(ref _approximateEntries);

    /// <summary>
    /// Directly replaces the internal approximate-entry counter. Backends use this from
    /// <see cref="InitBackend"/> or <see cref="FlushBackend"/> after reconciling with the
    /// underlying store's actual size.
    /// </summary>
    protected void SetApproximateEntries(long value) => Interlocked.Exchange(ref _approximateEntries, value);

    /// <summary>Atomically adds one to the entry count.</summary>
    protected void IncrementEntries() => Interlocked.Increment(ref _approximateEntries);

    /// <summary>Atomically subtracts one from the entry count.</summary>
    protected void DecrementEntries() => Interlocked.Decrement(ref _approximateEntries);

    /// <summary>Atomically adds <paramref name="delta"/> to the entry count.</summary>
    protected void AddEntries(long delta) => Interlocked.Add(ref _approximateEntries, delta);

    /// <summary>
    /// Constructs a new byte-backed store. Concrete subclasses should usually expose a
    /// richer constructor that accepts backend-specific configuration and then chain to this one.
    /// </summary>
    protected ByteBackedKeyValueStore(string name, ISerde<TKey> keySerde, ISerde<TValue> valueSerde)
    {
        Name = name;
        KeySerde = keySerde;
        ValueSerde = valueSerde;
    }

    /// <inheritdoc />
    public void Init(ProcessorContext context)
    {
        Context = context;
        InitBackend(context);
        Metrics = context.Metrics.GetOrCreateStoreMetrics(Name, () => ApproximateNumEntries);
    }

    /// <summary>
    /// Opens the underlying backend (database, file, etc.). Called once from <see cref="Init"/>.
    /// Subclasses should use this hook to open connections, create directories, and to
    /// reconcile the entry counter via <see cref="SetApproximateEntries"/>.
    /// </summary>
    protected abstract void InitBackend(ProcessorContext context);

    /// <inheritdoc />
    public TValue? Get(TKey key)
    {
        var keyBytes = KeySerde.Serialize(key);
        var valueBytes = GetBytes(keyBytes);
        Metrics?.RecordGet();
        return valueBytes is null ? default : ValueSerde.Deserialize(valueBytes);
    }

    /// <summary>
    /// Reads the raw bytes associated with <paramref name="keyBytes"/> from the backend, or
    /// <c>null</c> when the key is not present.
    /// </summary>
    protected abstract byte[]? GetBytes(byte[] keyBytes);

    /// <inheritdoc />
    public void Put(TKey key, TValue value)
    {
        var keyBytes = KeySerde.Serialize(key);
        var valueBytes = ValueSerde.Serialize(value);
        var wasNew = PutBytes(keyBytes, valueBytes);
        if (wasNew)
            IncrementEntries();
        Metrics?.RecordPut();
    }

    /// <summary>
    /// Writes the key/value pair to the backend. Returns <c>true</c> if this was a new key
    /// (the counter should increment), <c>false</c> if it overwrote an existing entry.
    /// Backends that can't easily tell the difference may return <c>true</c> optimistically;
    /// callers should then maintain the counter themselves or override
    /// <see cref="ApproximateNumEntries"/> to query the backend directly.
    /// </summary>
    protected abstract bool PutBytes(byte[] keyBytes, byte[] valueBytes);

    /// <inheritdoc />
    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var keyBytes = KeySerde.Serialize(key);
        var existing = GetBytes(keyBytes);
        if (existing is not null)
            return ValueSerde.Deserialize(existing);

        var valueBytes = ValueSerde.Serialize(value);
        if (PutBytes(keyBytes, valueBytes))
            IncrementEntries();
        Metrics?.RecordPut();
        return default;
    }

    /// <inheritdoc />
    public virtual void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        var count = 0;
        foreach (var entry in entries)
        {
            var keyBytes = KeySerde.Serialize(entry.Key);
            var valueBytes = ValueSerde.Serialize(entry.Value);
            if (PutBytes(keyBytes, valueBytes))
                IncrementEntries();
            count++;
        }
        if (count > 0)
            Metrics?.RecordPut(count);
    }

    /// <inheritdoc />
    public TValue? Delete(TKey key)
    {
        var keyBytes = KeySerde.Serialize(key);
        var existing = DeleteBytes(keyBytes);
        if (existing is null)
            return default;

        DecrementEntries();
        Metrics?.RecordDelete();
        return ValueSerde.Deserialize(existing);
    }

    /// <summary>
    /// Removes <paramref name="keyBytes"/> from the backend and returns the previously stored
    /// value bytes (so the caller can deserialize and return them), or <c>null</c> when the
    /// key was not present. Implementations should do Get+Delete in whichever form their
    /// backend supports — no atomicity is required between the Get and the Delete.
    /// </summary>
    protected abstract byte[]? DeleteBytes(byte[] keyBytes);

    /// <inheritdoc />
    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        var fromBytes = KeySerde.Serialize(from);
        var toBytes = KeySerde.Serialize(to);
        foreach (var (keyBytes, valueBytes) in RangeBytes(fromBytes, toBytes))
        {
            yield return new KeyValue<TKey, TValue>(
                KeySerde.Deserialize(keyBytes),
                ValueSerde.Deserialize(valueBytes));
        }
    }

    /// <summary>
    /// Yields all <c>(key, value)</c> pairs whose keys lie in the byte-lexicographic range
    /// <c>[fromBytes, toBytes]</c> (inclusive). Result order is backend-specific but the
    /// contract assumes byte-sorted output.
    /// </summary>
    protected abstract IEnumerable<(byte[] key, byte[] value)> RangeBytes(byte[] fromBytes, byte[] toBytes);

    /// <inheritdoc />
    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        foreach (var (keyBytes, valueBytes) in AllBytes())
        {
            yield return new KeyValue<TKey, TValue>(
                KeySerde.Deserialize(keyBytes),
                ValueSerde.Deserialize(valueBytes));
        }
    }

    /// <summary>Yields all <c>(key, value)</c> pairs currently in the backend.</summary>
    protected abstract IEnumerable<(byte[] key, byte[] value)> AllBytes();

    /// <inheritdoc />
    public void Flush() => FlushBackend();

    /// <summary>Flushes pending writes to the backend. Called by <see cref="Flush"/>.</summary>
    protected abstract void FlushBackend();

    /// <inheritdoc />
    public void Close() => CloseBackend();

    /// <summary>Closes the backend. Called by <see cref="Close"/> and <see cref="Dispose"/>.</summary>
    protected abstract void CloseBackend();

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes managed resources by closing the backend. Subclasses that hold additional
    /// unmanaged resources should override and call <c>base.Dispose(disposing)</c>.
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
            CloseBackend();
    }
}
