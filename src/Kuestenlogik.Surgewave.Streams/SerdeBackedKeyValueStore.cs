namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Composition-path <see cref="IKeyValueStore{TKey,TValue}"/> implementation. Wraps any
/// <see cref="IByteKeyValueBackend"/> and exposes it as a full generic Surgewave state store
/// with serialization, metrics, approximate-entry counting and <see cref="ProcessorContext"/>
/// integration.
///
/// <para>
/// Compared to subclassing <see cref="ByteBackedKeyValueStore{TKey,TValue}"/> directly:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <b>Inheritance path</b> (<see cref="ByteBackedKeyValueStore{TKey,TValue}"/>) is the
///     right choice for in-tree stores that need deep integration with Surgewave runtime types
///     (RocksDB, SQLite, memory-mapped file stores) and complex per-store configuration.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Composition path</b> (<see cref="SerdeBackedKeyValueStore{TKey,TValue}"/> +
///     <see cref="IByteKeyValueBackend"/>) is the right choice for third-party backends,
///     experimental stores and test doubles that want the minimum possible surface area —
///     just the byte-level CRUD operations, no generics, no Surgewave runtime knowledge.
///     </description>
///   </item>
/// </list>
/// </summary>
public sealed class SerdeBackedKeyValueStore<TKey, TValue> : ByteBackedKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage", "CA2213:Disposable fields should be disposed",
        Justification = "Disposed in CloseBackend via the base class Dispose pattern.")]
    private readonly IByteKeyValueBackend _backend;

    /// <inheritdoc />
    public override bool Persistent { get; }

    /// <inheritdoc />
    public override long ApproximateNumEntries => _backend.ApproximateNumEntries;

    /// <summary>
    /// Wraps <paramref name="backend"/> as a generic key-value store named <paramref name="name"/>.
    /// </summary>
    /// <param name="name">The logical store name. Used for metrics and state-directory paths.</param>
    /// <param name="keySerde">Serde for encoding/decoding keys.</param>
    /// <param name="valueSerde">Serde for encoding/decoding values.</param>
    /// <param name="backend">The byte-level backend to wrap.</param>
    /// <param name="persistent">
    /// Whether this store should advertise itself as persistent. Typically <c>true</c> for
    /// disk-backed backends and <c>false</c> for purely in-memory ones.
    /// </param>
    public SerdeBackedKeyValueStore(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        IByteKeyValueBackend backend,
        bool persistent = true)
        : base(name, keySerde, valueSerde)
    {
        ArgumentNullException.ThrowIfNull(backend);
        _backend = backend;
        Persistent = persistent;
    }

    /// <inheritdoc />
    protected override void InitBackend(ProcessorContext context)
    {
        var stateDir = context.Config.StateDir ?? Path.Combine(Path.GetTempPath(), "surgewave-streams");
        var storeDir = Path.Combine(stateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(storeDir);

        _backend.Open(new BackendOpenContext(
            StoreName: Name,
            StateDirectory: storeDir,
            ApplicationId: context.ApplicationId,
            TaskId: context.TaskId));
    }

    /// <inheritdoc />
    protected override byte[]? GetBytes(byte[] keyBytes) => _backend.Get(keyBytes);

    /// <inheritdoc />
    protected override bool PutBytes(byte[] keyBytes, byte[] valueBytes)
        => _backend.Put(keyBytes, valueBytes);

    /// <inheritdoc />
    protected override byte[]? DeleteBytes(byte[] keyBytes) => _backend.Delete(keyBytes);

    /// <inheritdoc />
    protected override IEnumerable<(byte[] key, byte[] value)> RangeBytes(byte[] fromBytes, byte[] toBytes)
        => _backend.Range(fromBytes, toBytes);

    /// <inheritdoc />
    protected override IEnumerable<(byte[] key, byte[] value)> AllBytes() => _backend.All();

    /// <inheritdoc />
    protected override void FlushBackend() => _backend.Flush();

    /// <inheritdoc />
    protected override void CloseBackend() => _backend.Dispose();
}
