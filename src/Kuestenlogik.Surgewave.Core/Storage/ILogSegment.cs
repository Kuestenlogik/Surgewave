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
/// </summary>
public readonly struct DataSource
{
    private readonly ReadOnlyMemory<byte> _memory;
    private readonly SafeFileHandle? _fileHandle;
    private readonly long _filePosition;

    /// <summary>Whether this data source is backed by memory (vs file)</summary>
    public bool IsMemoryBacked => _fileHandle == null;

    /// <summary>Memory slice for memory-backed sources</summary>
    public ReadOnlyMemory<byte> Memory => _memory;

    /// <summary>File handle for file-backed sources</summary>
    public SafeFileHandle FileHandle => _fileHandle!;

    /// <summary>File position for file-backed sources</summary>
    public long FilePosition => _filePosition;

    /// <summary>Length of data</summary>
    public int Length => _memory.Length;

    private DataSource(ReadOnlyMemory<byte> memory, SafeFileHandle? fileHandle, long filePosition)
    {
        _memory = memory;
        _fileHandle = fileHandle;
        _filePosition = filePosition;
    }

    /// <summary>Create a memory-backed data source</summary>
    public static DataSource FromMemory(ReadOnlyMemory<byte> memory)
        => new(memory, null, 0);

    /// <summary>Create a file-backed data source</summary>
    public static DataSource FromFile(SafeFileHandle handle, long position, int length)
        => new(default, handle, position) { };

    /// <summary>Create an empty data source</summary>
    public static DataSource Empty => new(ReadOnlyMemory<byte>.Empty, null, 0);
}

/// <summary>
/// Extended interface for file-based segments that support memory-mapped reads
/// </summary>
public interface IFileLogSegment : ILogSegment
{
    /// <summary>Path to the log file</summary>
    string LogFilePath { get; }

    /// <summary>Safe file handle for zero-copy operations</summary>
    SafeFileHandle SafeFileHandle { get; }
}

/// <summary>
/// Extended interface for memory-based segments that support direct memory access
/// </summary>
public interface IMemoryLogSegment : ILogSegment
{
    /// <summary>
    /// Get a direct memory slice of the data. Zero-copy for memory segments.
    /// </summary>
    ReadOnlyMemory<byte> GetMemorySlice(long position, int length);
}
