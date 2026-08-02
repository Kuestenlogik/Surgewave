namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// A contiguous read of one or more record batches, together with the resource that keeps
/// <see cref="Data"/> alive (#78).
///
/// <para><b>Why this exists.</b> Storage engines can serve reads straight out of a pooled or
/// memory-mapped buffer, but the classic tuple-returning read has no way to express "this memory
/// is only valid while I hold a lease" — so it had to materialize the lease into a fresh array,
/// one payload-sized GC allocation per fetch per partition. This carrier hands the borrowed memory
/// out together with its lifetime.</para>
///
/// <para><b>Contract.</b> <see cref="Data"/> is only valid until <see cref="Dispose"/>. Consume it
/// (copy, re-frame, or write it) before disposing, and never let it escape the <c>using</c> scope
/// — a lease returns its buffer to a pool, so reading after disposal can observe an unrelated
/// partition's bytes. Callers that need memory outliving the scope must copy explicitly.</para>
///
/// <para>The lifetime is held as a plain <see cref="IDisposable"/> on purpose: the engine-specific
/// lease type lives above this layer, and Core must not take a dependency on it.</para>
/// </summary>
public readonly struct ContiguousBatchRead : IDisposable
{
    private readonly IDisposable? _lifetime;

    /// <summary>The contiguous batch bytes. Only valid until <see cref="Dispose"/>.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    /// <summary>Start index of each batch within <see cref="Data"/>.</summary>
    public List<int> BatchOffsets { get; }

    /// <summary>
    /// The resource that keeps <see cref="Data"/> alive, or <c>null</c> when the memory is already
    /// owned by the caller and nothing has to be released.
    ///
    /// <para>Exposed for consumers whose lifetime is not a <c>using</c> scope: the Kafka fetch path
    /// hands the bytes to a response object that is serialized later, so it takes the lifetime over
    /// and releases it after the write (#78). Taking it over means <b>not</b> disposing the read as
    /// well — ownership moves, it is not shared. A <c>null</c> here means there is nothing to move,
    /// which is why this is a property and not a boxed <see cref="IDisposable"/> handed out
    /// unconditionally.</para>
    /// </summary>
    public IDisposable? Lifetime => _lifetime;

    /// <summary>
    /// Wraps memory that is already owned by the caller (no lease). Disposing is a no-op — this is
    /// what the compatibility path returns after it has copied into a plain array.
    /// </summary>
    public ContiguousBatchRead(ReadOnlyMemory<byte> data, List<int> batchOffsets)
        : this(data, batchOffsets, lifetime: null)
    {
    }

    /// <summary>
    /// Wraps borrowed memory whose validity is bounded by <paramref name="lifetime"/>.
    /// </summary>
    public ContiguousBatchRead(ReadOnlyMemory<byte> data, List<int> batchOffsets, IDisposable? lifetime)
    {
        Data = data;
        BatchOffsets = batchOffsets;
        _lifetime = lifetime;
    }

    /// <summary>An empty read; disposing is a no-op.</summary>
    public static ContiguousBatchRead Empty => new(ReadOnlyMemory<byte>.Empty, []);

    /// <summary>Releases the underlying lease, if any. <see cref="Data"/> is invalid afterwards.</summary>
    public void Dispose() => _lifetime?.Dispose();
}
