using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Handles optimized read operations for PartitionLog, including
/// memory-mapped I/O and multi-segment parallel reads.
/// </summary>
internal sealed class PartitionLogReader : IDisposable
{
    private readonly ConcurrentDictionary<long, MemoryMappedLogReader> _mmapReaders = new();

    /// <summary>
    /// Read batches using memory-mapped I/O for high performance (file-based segments only).
    /// For memory segments, reads directly from the segment.
    /// </summary>
    public List<byte[]> ReadBatchesWithMmap(ILogSegment segment, long startOffset, int maxBytes)
    {
        // For memory segments, read directly (already in memory, no mmap needed)
        if (segment is IMemoryLogSegment memorySegment)
        {
            return ReadBatchesFromMemorySegment(memorySegment, startOffset, maxBytes);
        }

        // For file segments, use mmap
        if (segment is not IFileLogSegment fileSegment)
        {
            return [];
        }

        var reader = GetOrCreateMmapReader(fileSegment);
        if (reader == null)
        {
            return [];
        }

        var filePosition = segment.GetFilePositionForOffset(startOffset);
        if (filePosition == null)
        {
            return [];
        }

        return reader.ReadBatches(filePosition.Value, maxBytes);
    }

    /// <summary>
    /// Read single segment contiguously.
    /// </summary>
    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadSingleSegmentContiguousAsync(
        ILogSegment segment, ILogSegment? activeSegment, long startOffset, int maxBytes, CancellationToken cancellationToken)
    {
        if (segment == activeSegment)
        {
            // Active segment: use async I/O
            return await segment.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);
        }
        else
        {
            // Closed segment: use mmap for file segments, direct access for memory segments
            if (segment is IMemoryLogSegment memorySegment)
            {
                return ReadBatchesContiguousFromMemorySegment(memorySegment, startOffset, maxBytes);
            }

            if (segment is not IFileLogSegment fileSegment)
            {
                return (ReadOnlyMemory<byte>.Empty, []);
            }

            var reader = GetOrCreateMmapReader(fileSegment);
            if (reader == null)
            {
                return (ReadOnlyMemory<byte>.Empty, []);
            }

            var filePosition = segment.GetFilePositionForOffset(startOffset);
            if (filePosition == null)
            {
                return (ReadOnlyMemory<byte>.Empty, []);
            }

            return reader.ReadBatchesContiguous(filePosition.Value, maxBytes);
        }
    }

    /// <summary>
    /// Read multiple segments in parallel and combine results.
    /// </summary>
    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadMultiSegmentContiguousAsync(
        List<(ILogSegment segment, long offset)> segmentsToRead, ILogSegment? activeSegment, int maxBytes, CancellationToken cancellationToken)
    {
        // Prepare parallel read tasks
        var readTasks = new List<Task<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets, int SegmentIndex)>>();
        var remainingBytes = maxBytes;

        for (int i = 0; i < segmentsToRead.Count && remainingBytes > 0; i++)
        {
            var (currentSegment, segmentStartOffset) = segmentsToRead[i];
            var bytesToRead = remainingBytes;
            var idx = i;

            // Fire parallel read tasks
            if (currentSegment == activeSegment)
            {
                // Active segment: async file I/O
                readTasks.Add(Task.Run(async () =>
                {
                    var result = await currentSegment.ReadBatchesContiguousAsync(segmentStartOffset, bytesToRead, cancellationToken);
                    return (result.Data, result.BatchOffsets, idx);
                }, cancellationToken));
            }
            else if (currentSegment is IMemoryLogSegment memorySegment)
            {
                // Memory segment: direct memory access (synchronous, no I/O)
                var seg = memorySegment;
                var startOff = segmentStartOffset;
                var toRead = bytesToRead;
                readTasks.Add(Task.Run(() =>
                {
                    var result = ReadBatchesContiguousFromMemorySegment(seg, startOff, toRead);
                    return (result.Data, result.BatchOffsets, idx);
                }, cancellationToken));
            }
            else if (currentSegment is IFileLogSegment fileSegment)
            {
                // File segment: use mmap (synchronous but fast)
                var reader = GetOrCreateMmapReader(fileSegment);
                if (reader != null)
                {
                    var filePosition = currentSegment.GetFilePositionForOffset(segmentStartOffset);
                    if (filePosition != null)
                    {
                        var fp = filePosition.Value;
                        readTasks.Add(Task.Run(() =>
                        {
                            var result = reader.ReadBatchesContiguous(fp, bytesToRead);
                            return ((ReadOnlyMemory<byte>)result.Data, result.BatchOffsets, idx);
                        }, cancellationToken));
                    }
                }
            }

            // Estimate bytes we'll get (we don't know exact until read completes)
            remainingBytes -= Math.Min(remainingBytes, (int)Math.Min(currentSegment.Size, int.MaxValue));
        }

        if (readTasks.Count == 0)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        // Wait for all reads to complete
        var results = await Task.WhenAll(readTasks);

        // Sort by segment index and combine
        var sortedResults = results.Where(r => r.Data.Length > 0).OrderBy(r => r.SegmentIndex).ToList();

        if (sortedResults.Count == 0)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        if (sortedResults.Count == 1)
        {
            return (sortedResults[0].Data, sortedResults[0].BatchOffsets);
        }

        // Combine multiple segments into one contiguous buffer
        return CombineSegmentResults(sortedResults, maxBytes);
    }

    private static (ReadOnlyMemory<byte> Data, List<int> BatchOffsets) CombineSegmentResults(
        List<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets, int SegmentIndex)> sortedResults, int maxBytes)
    {
        var totalSize = sortedResults.Sum(r => r.Data.Length);
        var combinedData = new byte[Math.Min(totalSize, maxBytes)];
        var combinedOffsets = new List<int>();
        var currentOffset = 0;

        foreach (var (data, offsets, _) in sortedResults)
        {
            if (currentOffset + data.Length > maxBytes)
            {
                // Partial copy to stay within maxBytes
                var bytesToCopy = maxBytes - currentOffset;
                data.Span.Slice(0, bytesToCopy).CopyTo(combinedData.AsSpan(currentOffset));

                // Add offsets for batches that fit
                foreach (var offset in offsets)
                {
                    if (currentOffset + offset < maxBytes)
                    {
                        combinedOffsets.Add(currentOffset + offset);
                    }
                }
                break;
            }

            data.Span.CopyTo(combinedData.AsSpan(currentOffset));

            // Adjust batch offsets for combined buffer
            foreach (var offset in offsets)
            {
                combinedOffsets.Add(currentOffset + offset);
            }

            currentOffset += data.Length;
        }

        return (combinedData, combinedOffsets);
    }

    public MemoryMappedLogReader? GetOrCreateMmapReader(IFileLogSegment segment)
    {
        var logPath = segment.LogFilePath;
        if (!File.Exists(logPath))
        {
            return null;
        }

        var reader = _mmapReaders.GetOrAdd(segment.BaseOffset, _ => new MemoryMappedLogReader(logPath));
        reader.RefreshMapping();
        return reader;
    }

    public void RemoveMmapReader(long baseOffset)
    {
        if (_mmapReaders.TryRemove(baseOffset, out var reader))
        {
            reader.Dispose();
        }
    }

    private static List<byte[]> ReadBatchesFromMemorySegment(IMemoryLogSegment segment, long startOffset, int maxBytes)
    {
        var filePosition = segment.GetFilePositionForOffset(startOffset);
        if (filePosition == null)
        {
            return [];
        }

        var batches = new List<byte[]>();
        var totalBytes = 0;
        var position = (int)filePosition.Value;

        while (totalBytes < maxBytes)
        {
            // Read header to get batch size (need at least 12 bytes: offset + length)
            var headerSlice = segment.GetMemorySlice(position, 12);
            if (headerSlice.Length < 12)
                break;

            var headerSpan = headerSlice.Span;
            var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(headerSpan.Slice(8, 4));
            var totalBatchSize = 12 + batchLength;

            // Ensure at least first batch is returned (Kafka fetch semantics)
            if (totalBytes > 0 && totalBytes + totalBatchSize > maxBytes)
                break;

            // Read the full batch
            var batchSlice = segment.GetMemorySlice(position, totalBatchSize);
            if (batchSlice.Length < totalBatchSize)
                break;

            batches.Add(batchSlice.ToArray());
            totalBytes += totalBatchSize;
            position += totalBatchSize;
        }

        return batches;
    }

    private static (ReadOnlyMemory<byte> Data, List<int> BatchOffsets) ReadBatchesContiguousFromMemorySegment(
        IMemoryLogSegment segment, long startOffset, int maxBytes)
    {
        var filePosition = segment.GetFilePositionForOffset(startOffset);
        if (filePosition == null)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        var batchOffsets = new List<int>();
        var position = (int)filePosition.Value;
        var validBytes = 0;
        var startPosition = position;

        // First pass: find all batches and their total size
        while (true)
        {
            var headerSlice = segment.GetMemorySlice(position, 12);
            if (headerSlice.Length < 12)
                break;

            var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(headerSlice.Span.Slice(8, 4));
            var totalBatchSize = 12 + batchLength;

            // Verify we can read the full batch
            var batchSlice = segment.GetMemorySlice(position, totalBatchSize);
            if (batchSlice.Length < totalBatchSize)
                break;

            // Ensure at least first batch is returned
            if (validBytes > 0 && validBytes + totalBatchSize > maxBytes)
                break;

            batchOffsets.Add(validBytes);
            validBytes += totalBatchSize;
            position += totalBatchSize;
        }

        if (validBytes == 0)
        {
            return (ReadOnlyMemory<byte>.Empty, []);
        }

        // Zero-copy: return memory slice directly without .ToArray() copy
        var dataSlice = segment.GetMemorySlice(startPosition, validBytes);
        return (dataSlice, batchOffsets);
    }

    public void Dispose()
    {
        foreach (var reader in _mmapReaders.Values)
        {
            reader.Dispose();
        }
        _mmapReaders.Clear();
    }
}
