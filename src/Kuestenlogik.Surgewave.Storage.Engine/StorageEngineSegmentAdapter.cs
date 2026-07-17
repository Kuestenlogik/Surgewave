using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Win32.SafeHandles;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Adapts ISurgewaveStorageEngine to ILogSegment interface.
/// Enables gradual migration from legacy ILogSegment to new zero-copy storage.
/// </summary>
public sealed class StorageEngineSegmentAdapter : IFileLogSegment, IMemoryLogSegment
{
    private readonly ISurgewaveStorageEngine _engine;
    private readonly string _logFilePath;
    private readonly SafeFileHandle? _safeFileHandle;
    private readonly bool _isFileBased;
    private bool _disposed;

    public long BaseOffset => _engine.BaseOffset;
    public long CurrentOffset => _engine.CurrentOffset;
    public long Size => _engine.Size;
    public bool IsFull => _engine.IsFull;
    public DateTime CreatedAt => _engine.CreatedAt;
    public long MaxTimestamp => _engine.MaxTimestamp;

    // IFileLogSegment
    public string LogFilePath => _logFilePath;
    public SafeFileHandle SafeFileHandle => _safeFileHandle ?? throw new NotSupportedException("Not a file-based segment");

    public StorageEngineSegmentAdapter(
        ISurgewaveStorageEngine engine,
        string? logFilePath = null,
        SafeFileHandle? safeFileHandle = null)
    {
        _engine = engine;
        _logFilePath = logFilePath ?? string.Empty;
        _safeFileHandle = safeFileHandle;
        _isFileBased = safeFileHandle != null;
    }

    public long? GetFirstMessageOffset()
    {
        return _engine.FirstOffset;
    }

    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        byte[] recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _engine.AppendAsync(recordBatch.AsSpan(), cancellationToken);
    }

    /// <summary>
    /// Append a slice of a pooled buffer. Forwards straight to the engine's span-based
    /// positioned write — without this overload the default-interface chain in
    /// <see cref="ILogSegment"/> would ToArray() the slice into a fresh array right before
    /// the file write, one full copy per produced batch.
    /// </summary>
    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        byte[] buffer,
        int offset,
        int length,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _engine.AppendAsync(buffer.AsSpan(offset, length), cancellationToken);
    }

    /// <summary>
    /// Append from <see cref="ReadOnlyMemory{T}"/>. The engine copies synchronously out of the
    /// span (<see cref="ISurgewaveStorageEngine"/> contract), so the caller keeps buffer
    /// ownership for the duration of the call and nothing is retained afterwards.
    /// </summary>
    public ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        ReadOnlyMemory<byte> recordBatch,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _engine.AppendAsync(recordBatch.Span, cancellationToken);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _engine.FlushAsync(cancellationToken);
    }

    public async ValueTask<List<byte[]>> ReadBatchesAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var lease = await _engine.ReadAsync(startOffset, maxBytes, cancellationToken);

        if (lease.IsEmpty)
            return [];

        var batches = new List<byte[]>(lease.BatchCount);
        for (int i = 0; i < lease.BatchCount; i++)
        {
            using var batch = lease.GetBatch(i);
            batches.Add(batch.ToArray());
        }

        return batches;
    }

    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var lease = await _engine.ReadAsync(startOffset, maxBytes, cancellationToken);

        if (lease.IsEmpty)
            return (ReadOnlyMemory<byte>.Empty, []);

        // Copy data and batch offsets
        var data = lease.Data.ToArray();
        var batchOffsets = new List<int>(lease.BatchOffsets);

        return (data, batchOffsets);
    }

    public long? GetFilePositionForOffset(long startOffset)
    {
        // For storage engines, we don't expose file positions directly
        // The read methods handle offset-based access internally
        // Return null to indicate the caller should use ReadBatchesAsync
        return null;
    }

    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        return _engine.FindOffsetByTimestamp(targetTimestamp);
    }

    public void DeleteFiles()
    {
        _engine.DeleteStorage();
    }

    // IMemoryLogSegment implementation
    public ReadOnlyMemory<byte> GetMemorySlice(long position, int length)
    {
        // For memory-based engines, we could potentially expose direct access
        // For now, return empty - the ReadBatchesAsync path will be used instead
        return ReadOnlyMemory<byte>.Empty;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _engine.Dispose();
    }
}
