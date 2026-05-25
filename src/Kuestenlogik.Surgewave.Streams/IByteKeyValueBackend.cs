namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Generic-free, byte-level storage contract for a key-value backend. Together with
/// <see cref="SerdeBackedKeyValueStore{TKey,TValue}"/> this is the composition-friendly
/// alternative to subclassing <see cref="ByteBackedKeyValueStore{TKey,TValue}"/>.
///
/// <para>
/// Implement this interface if you want to plug a new storage backend into Surgewave Streams
/// without dealing with generics, serialization or metrics — those concerns live in
/// <see cref="SerdeBackedKeyValueStore{TKey,TValue}"/>, which wraps any
/// <see cref="IByteKeyValueBackend"/> and exposes it as a full
/// <see cref="IKeyValueStore{TKey,TValue}"/>.
/// </para>
///
/// <para>Example — a trivial in-memory backend:</para>
/// <code>
/// public sealed class InMemoryBackend : IByteKeyValueBackend
/// {
///     private readonly SortedDictionary&lt;byte[], byte[]&gt; _store = new(ByteArrayComparer.Instance);
///     public long ApproximateNumEntries => _store.Count;
///     public void Open(BackendOpenContext ctx) { }
///     public byte[]? Get(byte[] keyBytes) =&gt; _store.TryGetValue(keyBytes, out var v) ? v : null;
///     public bool Put(byte[] keyBytes, byte[] valueBytes) { var fresh = !_store.ContainsKey(keyBytes); _store[keyBytes] = valueBytes; return fresh; }
///     public byte[]? Delete(byte[] keyBytes) { if (_store.TryGetValue(keyBytes, out var v)) { _store.Remove(keyBytes); return v; } return null; }
///     public IEnumerable&lt;(byte[] key, byte[] value)&gt; Range(byte[] from, byte[] to) =&gt; ...;
///     public IEnumerable&lt;(byte[] key, byte[] value)&gt; All() =&gt; _store.Select(kv =&gt; (kv.Key, kv.Value));
///     public void Flush() { }
///     public void Dispose() =&gt; _store.Clear();
/// }
///
/// var store = new SerdeBackedKeyValueStore&lt;string, int&gt;("counts", Serdes.String(), Serdes.Int(), new InMemoryBackend());
/// </code>
/// </summary>
public interface IByteKeyValueBackend : IDisposable
{
    /// <summary>
    /// Current entry count estimate. May be inaccurate under concurrent updates — this is a
    /// "best-effort" number used for metrics and capacity hints, not for correctness.
    /// </summary>
    long ApproximateNumEntries { get; }

    /// <summary>
    /// Opens the backend. Called exactly once per backend instance, before any other method.
    /// Implementations typically open files/connections and reconcile their entry counter here.
    /// </summary>
    void Open(BackendOpenContext context);

    /// <summary>
    /// Returns the raw value bytes for the given key, or <c>null</c> when the key is not present.
    /// </summary>
    byte[]? Get(byte[] keyBytes);

    /// <summary>
    /// Writes the key/value pair. Returns <c>true</c> if this was a new key (the counter
    /// should be incremented), <c>false</c> if it overwrote an existing entry. Backends that
    /// can't easily tell the difference may return <c>true</c> optimistically and reconcile
    /// the counter on <see cref="Flush"/>.
    /// </summary>
    bool Put(byte[] keyBytes, byte[] valueBytes);

    /// <summary>
    /// Removes the key and returns the previously stored value bytes, or <c>null</c> if the
    /// key was not present. Implementations may do a Get+Delete in whichever form their
    /// backend supports — no atomicity is required between the Get and the Delete.
    /// </summary>
    byte[]? Delete(byte[] keyBytes);

    /// <summary>
    /// Yields all <c>(key, value)</c> pairs whose keys lie in the byte-lexicographic range
    /// <c>[fromBytes, toBytes]</c> (inclusive).
    /// </summary>
    IEnumerable<(byte[] key, byte[] value)> Range(byte[] fromBytes, byte[] toBytes);

    /// <summary>Yields all <c>(key, value)</c> pairs currently in the backend.</summary>
    IEnumerable<(byte[] key, byte[] value)> All();

    /// <summary>Flushes pending writes to the backend's durable layer.</summary>
    void Flush();
}

/// <summary>
/// Context handed to <see cref="IByteKeyValueBackend.Open"/> so backends can locate their
/// state directory and identify the owning store without taking a dependency on Surgewave's
/// <c>ProcessorContext</c> type directly.
/// </summary>
/// <param name="StoreName">The logical name of the store.</param>
/// <param name="StateDirectory">Absolute path to the directory the backend should use for its files.</param>
/// <param name="ApplicationId">The Streams application id — useful for partitioning per-app state.</param>
/// <param name="TaskId">Optional task id — multiple parallel tasks on the same host need isolated directories.</param>
public sealed record BackendOpenContext(
    string StoreName,
    string StateDirectory,
    string ApplicationId,
    string? TaskId);
