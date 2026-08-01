using Microsoft.Win32.SafeHandles;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Interface for log segment implementations.
/// Supports both file-based and memory-based storage backends.
/// </summary>
public interface ILogSegment : IDisposable
{
    /// <summary>Default segment size: 1GB</summary>
    const long DefaultMaxSegmentSize = KafkaConstants.Defaults.MaxSegmentSize;

    /// <summary>Base offset of this segment</summary>
    long BaseOffset { get; }

    /// <summary>Current offset (next offset to be written)</summary>
    long CurrentOffset { get; }

    /// <summary>Size of the segment in bytes</summary>
    long Size { get; }

    /// <summary>Whether the segment has reached its maximum size</summary>
    bool IsFull { get; }

    /// <summary>When this segment was created</summary>
    DateTime CreatedAt { get; }

    /// <summary>Maximum timestamp in this segment</summary>
    long MaxTimestamp { get; }

    /// <summary>
    /// Get the offset of the first message in this segment, or null if segment is empty
    /// </summary>
    long? GetFirstMessageOffset();

    /// <summary>
    /// Append a raw Kafka RecordBatch to the log
    /// </summary>
    ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] recordBatch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Append a slice of a Kafka RecordBatch buffer to the log.
    /// Zero-copy for ArrayPool buffers - no intermediate allocation.
    /// Default implementation uses ReadOnlyMemory overload.
    /// </summary>
    ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
        => AppendBatchAsync(buffer.AsMemory(offset, length), cancellationToken);

    /// <summary>
    /// Append a raw Kafka RecordBatch using ReadOnlyMemory for zero-copy scenarios.
    /// Default implementation converts to array; override for true zero-copy.
    /// </summary>
    ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
        => AppendBatchAsync(recordBatch.ToArray(), cancellationToken);

    /// <summary>
    /// Force flush all pending writes
    /// </summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Read raw RecordBatch bytes starting from an offset
    /// </summary>
    ValueTask<List<byte[]>> ReadBatchesAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Read raw RecordBatch bytes as a single contiguous array for zero-copy fetch.
    /// </summary>
    ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether the bytes behind <see cref="GetFilePositionForOffset"/> are the stored record
    /// batches themselves, so the core may read that file region directly (memory-mapped reads
    /// today, a kernel-side send later) instead of going through the segment's own read.
    ///
    /// <para><b>Opt in only if the file really is the batch stream.</b> The default is
    /// <see langword="false"/>, which is always safe: the core then uses
    /// <see cref="ReadContiguousAsync"/> and the engine stays in control of its own format.
    /// An engine that stores a different layout — columnar, compressed, encrypted, or with its
    /// own framing — must leave this <see langword="false"/>, otherwise the core would hand raw
    /// file bytes to a consumer expecting record batches.</para>
    ///
    /// <para>This exists because the core used to infer the answer from
    /// <c>is IFileLogSegment</c>, which is wrong for exactly those engines (#78).</para>
    /// </summary>
    bool SupportsCoreByteRangeReads => false;

    /// <summary>
    /// Read contiguous batches, keeping the underlying storage lease alive instead of copying it
    /// into a fresh array (#78). The returned <see cref="ContiguousBatchRead.Data"/> is only valid
    /// until the read is disposed — see <see cref="ContiguousBatchRead"/> for the contract.
    ///
    /// <para><b>This is the hook for storage plugins.</b> The default implementation delegates to
    /// <see cref="ReadBatchesContiguousAsync"/> and yields a read with no lease, so every existing
    /// segment keeps working unchanged. Override it to serve reads from your own buffers — pooled,
    /// memory-mapped, or otherwise borrowed — by returning a <see cref="ContiguousBatchRead"/> that
    /// carries the lifetime keeping that memory valid. The core does not need to know the engine
    /// to benefit from it.</para>
    /// </summary>
    async ValueTask<ContiguousBatchRead> ReadContiguousAsync(long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        var (data, batchOffsets) = await ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken).ConfigureAwait(false);
        return new ContiguousBatchRead(data, batchOffsets);
    }

    /// <summary>
    /// Get the file position for reading batches starting at the given offset.
    /// Returns null if no batch contains this offset.
    /// </summary>
    long? GetFilePositionForOffset(long startOffset);

    /// <summary>
    /// Find the offset of the first batch with timestamp >= targetTimestamp
    /// </summary>
    long? FindOffsetByTimestamp(long targetTimestamp);

    /// <summary>
    /// Delete all files associated with this segment.
    /// Must be called after Dispose().
    /// </summary>
    void DeleteFiles();
}

/// <summary>
/// Data source for zero-copy reads. Can be backed by either memory or a file handle.
/// <para>
/// The file-backed form describes a region <c>[FilePosition, FilePosition + Length)</c> of a
/// segment file — the shape a kernel-side send (sendfile/TransmitFile) needs. It carries no
/// ownership of the handle: whoever builds it must keep the handle alive for as long as the
/// source is used.
/// </para>
/// </summary>
public readonly struct DataSource
{
    private readonly ReadOnlyMemory<byte> _memory;
    private readonly SafeFileHandle? _fileHandle;
    private readonly long _filePosition;
    private readonly int _length;

    /// <summary>Whether this data source is backed by memory (vs file)</summary>
    public bool IsMemoryBacked => _fileHandle == null;

    /// <summary>Memory slice for memory-backed sources</summary>
    public ReadOnlyMemory<byte> Memory => _memory;

    /// <summary>File handle for file-backed sources</summary>
    public SafeFileHandle FileHandle => _fileHandle!;

    /// <summary>File position for file-backed sources</summary>
    public long FilePosition => _filePosition;

    /// <summary>
    /// Length of the data in bytes — the memory slice length for memory-backed sources, the
    /// region length for file-backed ones. The file-backed length used to be dropped on
    /// construction, so every file-backed source reported 0 bytes (#81).
    /// </summary>
    public int Length => _length;

    private DataSource(ReadOnlyMemory<byte> memory, SafeFileHandle? fileHandle, long filePosition, int length)
    {
        _memory = memory;
        _fileHandle = fileHandle;
        _filePosition = filePosition;
        _length = length;
    }

    /// <summary>Create a memory-backed data source</summary>
    public static DataSource FromMemory(ReadOnlyMemory<byte> memory)
        => new(memory, null, 0, memory.Length);

    /// <summary>Create a file-backed data source describing the region [position, position + length).</summary>
    public static DataSource FromFile(SafeFileHandle handle, long position, int length)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentOutOfRangeException.ThrowIfNegative(position);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        return new DataSource(default, handle, position, length);
    }

    /// <summary>Create an empty data source</summary>
    public static DataSource Empty => new(ReadOnlyMemory<byte>.Empty, null, 0, 0);
}

/// <summary>
/// Extended interface for file-based segments.
///
/// <para><b>Extension point — implemented outside this repository.</b> Storage plugins
/// (e.g. <c>Surgewave.Storage.Arrow</c>, <c>Surgewave.Storage.NvmeDirect</c>) implement this
/// alongside the in-tree segments. In-tree call sites may therefore look unused; they are not.</para>
///
/// <para><b>Do not gate engine-specific optimizations on this type.</b> Implementing
/// <see cref="IFileLogSegment"/> only says "there is a file behind me" — it says nothing about
/// what that file contains. Arrow, for instance, exposes a real handle to a columnar
/// <c>.arrow</c> file, not to verbatim stored record batches, so an <c>is IFileLogSegment</c>
/// check would wrongly select it for byte-level fast paths such as memory-mapped reads or a
/// future kernel-side send. Ask <see cref="ILogSegment.SupportsCoreByteRangeReads"/> instead
/// (#78).</para>
/// </summary>
public interface IFileLogSegment : ILogSegment
{
    /// <summary>Path to the log file</summary>
    string LogFilePath { get; }

    /// <summary>Safe file handle for zero-copy operations</summary>
    SafeFileHandle SafeFileHandle { get; }
}

/// <summary>
/// Extended interface for memory-based segments that support direct memory access.
///
/// <para><b>Extension point — also implemented by storage plugins.</b> See the note on
/// <see cref="IFileLogSegment"/>: in-tree usages that look dead are load-bearing for
/// out-of-tree engines.</para>
/// </summary>
public interface IMemoryLogSegment : ILogSegment
{
    /// <summary>
    /// Get a direct memory slice of the data. Zero-copy for memory segments.
    /// </summary>
    ReadOnlyMemory<byte> GetMemorySlice(long position, int length);
}
