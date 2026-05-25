using System.Buffers;
using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// High-performance log reader using memory-mapped files.
/// Provides zero-copy reads from log segments by mapping files directly into process memory.
/// The OS handles page caching, eliminating explicit read() syscalls for cached data.
/// </summary>
public sealed class MemoryMappedLogReader : IDisposable
{
    private readonly string _logFilePath;
    private MemoryMappedFile? _mappedFile;
    private MemoryMappedViewAccessor? _accessor;
    private readonly Lock _lock = new();
    private long _fileSize;
    private bool _disposed;

    public string LogFilePath => _logFilePath;
    public long FileSize => _fileSize;

    public MemoryMappedLogReader(string logFilePath)
    {
        _logFilePath = logFilePath;
        RefreshMapping();
    }

    /// <summary>
    /// Refresh the memory mapping if the file has grown.
    /// Should be called periodically or when reads return empty at expected positions.
    /// </summary>
    public void RefreshMapping()
    {
        lock (_lock)
        {
            if (!File.Exists(_logFilePath))
            {
                return;
            }

            var fileInfo = new FileInfo(_logFilePath);
            var newSize = fileInfo.Length;

            if (newSize == 0)
            {
                return;
            }

            // Only remap if file has grown
            if (newSize > _fileSize || _mappedFile == null)
            {
                _accessor?.Dispose();
                _mappedFile?.Dispose();

                // Open file with FileShare.ReadWrite to allow concurrent access
                // Note: FileStream ownership is transferred to MemoryMappedFile (leaveOpen: false)
#pragma warning disable CA2000 // FileStream ownership transferred to MemoryMappedFile
                var fileStream = new FileStream(
                    _logFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite);
#pragma warning restore CA2000

                _mappedFile = MemoryMappedFile.CreateFromFile(
                    fileStream,
                    mapName: null,
                    capacity: 0,
                    MemoryMappedFileAccess.Read,
                    HandleInheritability.None,
                    leaveOpen: false);

                _accessor = _mappedFile.CreateViewAccessor(0, newSize, MemoryMappedFileAccess.Read);
                _fileSize = newSize;
            }
        }
    }

    /// <summary>
    /// Read record batches starting from the given file position.
    /// Returns raw batch bytes suitable for sending directly to clients.
    /// </summary>
    public List<byte[]> ReadBatches(long filePosition, int maxBytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_accessor == null || filePosition >= _fileSize)
        {
            return [];
        }

        var batches = new List<byte[]>();
        var totalBytes = 0;
        var position = filePosition;

        // Allocate header buffer outside the loop
        var header = new byte[12];

        while (position < _fileSize && totalBytes < maxBytes)
        {
            // Need at least 12 bytes for header (baseOffset + batchLength)
            if (position + 12 > _fileSize)
            {
                break;
            }

            // Read header: baseOffset (8 bytes) + batchLength (4 bytes)
            var headerRead = _accessor.ReadArray(position, header, 0, 12);
            if (headerRead < 12)
            {
                break;
            }

            var batchLength = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(8));
            var totalBatchSize = 12 + batchLength;

            if (position + totalBatchSize > _fileSize)
            {
                break;
            }

            // Allocate and read the entire batch
            var batch = new byte[totalBatchSize];
            _accessor.ReadArray(position, batch, 0, totalBatchSize);

            batches.Add(batch);
            totalBytes += totalBatchSize;
            position += totalBatchSize;
        }

        return batches;
    }

    /// <summary>
    /// Read record batches starting from the given file position as a single contiguous byte array.
    /// This is more efficient than ReadBatches when you need to combine batches anyway,
    /// as it performs only a single allocation and copy operation.
    /// </summary>
    /// <returns>
    /// A tuple containing:
    /// - The contiguous byte array with all batches
    /// - A list of offsets within the array where each batch starts (for filtering if needed)
    /// </returns>
    public (byte[] Data, List<int> BatchOffsets) ReadBatchesContiguous(long filePosition, int maxBytes)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_accessor == null || filePosition >= _fileSize)
        {
            return ([], []);
        }

        // Calculate how much we can read
        var availableBytes = (int)Math.Min(maxBytes, _fileSize - filePosition);
        if (availableBytes < 12)
        {
            return ([], []);
        }

        // Single read operation - read all available data up to maxBytes
        var buffer = new byte[availableBytes];
        var bytesRead = _accessor.ReadArray(filePosition, buffer, 0, availableBytes);
        if (bytesRead < 12)
        {
            return ([], []);
        }

        // Parse batch boundaries from the already-read buffer
        var batchOffsets = new List<int>();
        var position = 0;
        var validBytes = 0;

        while (position + 12 <= bytesRead)
        {
            // Read batch length from buffer (not from mmap - already in memory)
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(position + 8, 4));
            var totalBatchSize = 12 + batchLength;

            // Check if we have the complete batch
            if (position + totalBatchSize > bytesRead)
            {
                break;
            }

            batchOffsets.Add(position);
            validBytes = position + totalBatchSize;
            position += totalBatchSize;
        }

        if (validBytes == 0)
        {
            return ([], []);
        }

        // If we read more than needed, return only valid batches
        if (validBytes < bytesRead)
        {
            var trimmedData = new byte[validBytes];
            Buffer.BlockCopy(buffer, 0, trimmedData, 0, validBytes);
            return (trimmedData, batchOffsets);
        }

        return (buffer, batchOffsets);
    }

    public void Dispose()
    {
        if (_disposed) return;

        _accessor?.Dispose();
        _mappedFile?.Dispose();
        _disposed = true;
    }
}
