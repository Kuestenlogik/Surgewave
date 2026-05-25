using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Log compactor - keeps only the latest value for each key in a partition.
/// Rewrites log segments to remove superseded records (older values for same key).
/// </summary>
public sealed class LogCompactor
{
    private readonly CompactionConfig _config;

    public LogCompactor(CompactionConfig? config = null)
    {
        _config = config ?? CompactionConfig.Default;
    }

    /// <summary>
    /// Compact a partition log by keeping only the latest value for each key
    /// </summary>
    /// <returns>Number of records removed during compaction</returns>
    public async Task<CompactionResult> CompactAsync(
        PartitionLog log,
        CancellationToken cancellationToken = default)
    {
        var segments = log.Segments;
        if (segments.Count < 2)
        {
            // Need at least 2 segments - active segment is never compacted
            return new CompactionResult(0, 0, 0);
        }

        // Build key -> latest offset map by scanning from newest to oldest
        // Skip the last (active) segment
        var keyOffsets = await BuildKeyOffsetMapAsync(segments.Take(segments.Count - 1), cancellationToken);

        if (keyOffsets.Count == 0)
        {
            return new CompactionResult(0, 0, 0);
        }

        // Compact each file-based segment (except the active one)
        // Memory segments don't support compaction (ephemeral data)
        var totalRecordsRemoved = 0L;
        var totalBytesRemoved = 0L;
        var segmentsCompacted = 0;

        foreach (var segment in segments.Take(segments.Count - 1).OfType<IFileLogSegment>())
        {
            var (recordsRemoved, bytesRemoved) = await CompactSegmentAsync(
                segment, keyOffsets, cancellationToken);

            totalRecordsRemoved += recordsRemoved;
            totalBytesRemoved += bytesRemoved;

            if (recordsRemoved > 0)
            {
                segmentsCompacted++;
            }
        }

        return new CompactionResult(totalRecordsRemoved, totalBytesRemoved, segmentsCompacted);
    }

    /// <summary>
    /// Build a map of key -> latest offset by scanning all segments from newest to oldest
    /// </summary>
    private async Task<Dictionary<byte[], long>> BuildKeyOffsetMapAsync(
        IEnumerable<ILogSegment> segments,
        CancellationToken cancellationToken)
    {
        var keyOffsets = new Dictionary<byte[], long>(SimdByteArrayComparer.Instance);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Process segments from newest to oldest
        foreach (var segment in segments.Reverse())
        {
            // Read all batches from the segment
            var batches = await segment.ReadBatchesAsync(segment.BaseOffset, int.MaxValue, cancellationToken);

            foreach (var batch in batches.AsEnumerable().Reverse())
            {
                // Parse each record in the batch
                var records = ParseRecordsFromBatch(batch);

                foreach (var record in records.AsEnumerable().Reverse())
                {
                    // Check if this key already has a later offset
                    if (record.Key != null && !keyOffsets.ContainsKey(record.Key))
                    {
                        // Check compaction lag
                        if (now - record.Timestamp >= _config.MinCompactionLagMs)
                        {
                            keyOffsets[record.Key] = record.Offset;
                        }
                    }
                }
            }
        }

        return keyOffsets;
    }

    /// <summary>
    /// Compact a single segment by rewriting it without superseded records.
    /// Creates a new segment file with only the latest version of each key.
    /// </summary>
    private async Task<(long recordsRemoved, long bytesRemoved)> CompactSegmentAsync(
        IFileLogSegment segment,
        Dictionary<byte[], long> keyOffsets,
        CancellationToken cancellationToken)
    {
        var recordsRemoved = 0L;
        var bytesRemoved = 0L;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var batches = await segment.ReadBatchesAsync(segment.BaseOffset, int.MaxValue, cancellationToken);
        var compactedBatches = new List<byte[]>();

        foreach (var batch in batches)
        {
            var records = ParseRecordsFromBatch(batch);
            var recordsToKeep = new List<ParsedRecord>();

            foreach (var record in records)
            {
                var shouldKeep = true;

                if (record.Key != null)
                {
                    if (keyOffsets.TryGetValue(record.Key, out var latestOffset))
                    {
                        if (record.Offset < latestOffset)
                        {
                            // Older version - remove
                            shouldKeep = false;
                            recordsRemoved++;
                            bytesRemoved += record.Size;
                        }
                        else if (record.Value == null && now - record.Timestamp >= _config.DeleteRetentionMs)
                        {
                            // Expired tombstone - remove
                            shouldKeep = false;
                            recordsRemoved++;
                            bytesRemoved += record.Size;
                        }
                    }
                }

                if (shouldKeep)
                {
                    recordsToKeep.Add(record);
                }
            }

            // If we're keeping all records, use original batch
            if (recordsToKeep.Count == records.Count)
            {
                compactedBatches.Add(batch);
            }
            else if (recordsToKeep.Count > 0)
            {
                // Rebuild batch with only kept records
                var newBatch = RebuildBatch(batch, recordsToKeep);
                compactedBatches.Add(newBatch);
            }
            // If no records to keep, skip the entire batch
        }

        // If nothing was removed, no need to rewrite
        if (recordsRemoved == 0)
        {
            return (0, 0);
        }

        // Write compacted data to temporary file, then swap
        await RewriteSegmentAsync(segment, compactedBatches, cancellationToken);

        return (recordsRemoved, bytesRemoved);
    }

    /// <summary>
    /// Rewrite a segment with compacted batches using atomic file swap
    /// </summary>
    private static async Task RewriteSegmentAsync(
        IFileLogSegment segment,
        List<byte[]> compactedBatches,
        CancellationToken cancellationToken)
    {
        var baseOffset = segment.BaseOffset;
        var segmentDir = Path.GetDirectoryName(segment.LogFilePath) ?? ".";

        // Create temp file path
        var logPath = Path.Combine(segmentDir, $"{baseOffset:D20}.log");
        var tempPath = Path.Combine(segmentDir, $"{baseOffset:D20}.log.compacting");

        // Write to temp file
        await using (var tempFile = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536, FileOptions.Asynchronous))
        {
            foreach (var batch in compactedBatches)
            {
                await tempFile.WriteAsync(batch, cancellationToken);
            }
            await tempFile.FlushAsync(cancellationToken);
        }

        // Atomic swap: rename temp file to actual file
        // Note: On Windows, need to delete target first; on Linux can use rename
        var backupPath = logPath + ".old";

        try
        {
            // Move original to backup
            if (File.Exists(logPath))
            {
                File.Move(logPath, backupPath, overwrite: true);
            }

            // Move temp to actual
            File.Move(tempPath, logPath);

            // Delete backup
            if (File.Exists(backupPath))
            {
                File.Delete(backupPath);
            }
        }
        catch
        {
            // Restore backup if something failed
            if (File.Exists(backupPath) && !File.Exists(logPath))
            {
                File.Move(backupPath, logPath);
            }
            throw;
        }
        finally
        {
            // Clean up temp file if it still exists
            // Deletion may fail if file is locked or already deleted - safe to ignore
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); }
                catch (IOException) { /* File locked or already deleted during cleanup */ }
                catch (UnauthorizedAccessException) { /* Permissions changed - best effort cleanup */ }
            }
        }
    }

    /// <summary>
    /// Rebuild a RecordBatch with only the specified records
    /// </summary>
    private static byte[] RebuildBatch(byte[] originalBatch, List<ParsedRecord> recordsToKeep)
    {
        if (originalBatch.Length < 61 || recordsToKeep.Count == 0)
        {
            return [];
        }

        // Parse original batch header
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(originalBatch.AsSpan(0, 8));
        var partitionLeaderEpoch = BinaryPrimitives.ReadInt32BigEndian(originalBatch.AsSpan(12, 4));
        var magic = originalBatch[16];
        var attributes = BinaryPrimitives.ReadInt16BigEndian(originalBatch.AsSpan(21, 2));
        var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(originalBatch.AsSpan(27, 8));
        var producerId = BinaryPrimitives.ReadInt64BigEndian(originalBatch.AsSpan(43, 8));
        var producerEpoch = BinaryPrimitives.ReadInt16BigEndian(originalBatch.AsSpan(51, 2));
        var baseSequence = BinaryPrimitives.ReadInt32BigEndian(originalBatch.AsSpan(53, 4));

        // Build new records section
        using var recordsStream = new MemoryStream();

        var lastOffsetDelta = 0;
        var maxTimestamp = baseTimestamp;

        foreach (var record in recordsToKeep)
        {
            var offsetDelta = (int)(record.Offset - baseOffset);
            lastOffsetDelta = offsetDelta;

            var timestampDelta = record.Timestamp - baseTimestamp;
            if (record.Timestamp > maxTimestamp)
            {
                maxTimestamp = record.Timestamp;
            }

            // Build individual record
            using var recordStream = new MemoryStream();

            // Attributes (varint, typically 0)
            WriteVarint(recordStream, 0);

            // Timestamp delta (varint)
            WriteVarint(recordStream, timestampDelta);

            // Offset delta (varint)
            WriteVarint(recordStream, offsetDelta);

            // Key length and key
            if (record.Key == null)
            {
                WriteVarint(recordStream, -1);
            }
            else
            {
                WriteVarint(recordStream, record.Key.Length);
                recordStream.Write(record.Key);
            }

            // Value length and value
            if (record.Value == null)
            {
                WriteVarint(recordStream, -1);
            }
            else
            {
                WriteVarint(recordStream, record.Value.Length);
                recordStream.Write(record.Value);
            }

            // Headers count (varint, 0 for simplicity - we don't preserve headers)
            WriteVarint(recordStream, 0);

            // Write record with length prefix
            var recordBytes = recordStream.ToArray();
            WriteVarint(recordsStream, recordBytes.Length);
            recordsStream.Write(recordBytes);
        }

        var recordsBytes = recordsStream.ToArray();

        // Build complete batch
        using var batchStream = new MemoryStream();

        // baseOffset (8 bytes)
        var buffer = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buffer, baseOffset);
        batchStream.Write(buffer);

        // batchLength placeholder (4 bytes) - will be filled later
        var batchLengthPosition = batchStream.Position;
        batchStream.Write(new byte[4]);

        // partitionLeaderEpoch (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, partitionLeaderEpoch);
        batchStream.Write(buffer, 0, 4);

        // magic (1 byte)
        batchStream.WriteByte(magic);

        // CRC placeholder (4 bytes) - will be filled later
        var crcPosition = batchStream.Position;
        batchStream.Write(new byte[4]);

        // Mark start of CRC-covered region
        var crcStartPosition = batchStream.Position;

        // attributes (2 bytes)
        BinaryPrimitives.WriteInt16BigEndian(buffer, attributes);
        batchStream.Write(buffer, 0, 2);

        // lastOffsetDelta (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, lastOffsetDelta);
        batchStream.Write(buffer, 0, 4);

        // baseTimestamp (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, baseTimestamp);
        batchStream.Write(buffer);

        // maxTimestamp (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, maxTimestamp);
        batchStream.Write(buffer);

        // producerId (8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(buffer, producerId);
        batchStream.Write(buffer);

        // producerEpoch (2 bytes)
        BinaryPrimitives.WriteInt16BigEndian(buffer, producerEpoch);
        batchStream.Write(buffer, 0, 2);

        // baseSequence (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, baseSequence);
        batchStream.Write(buffer, 0, 4);

        // recordCount (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(buffer, recordsToKeep.Count);
        batchStream.Write(buffer, 0, 4);

        // Records
        batchStream.Write(recordsBytes);

        var batchBytes = batchStream.ToArray();

        // Fill in batchLength (everything after batchLength field)
        var batchLength = batchBytes.Length - 12; // minus baseOffset(8) and batchLength(4)
        BinaryPrimitives.WriteInt32BigEndian(batchBytes.AsSpan(8, 4), batchLength);

        // Calculate and fill in CRC32-C (covers bytes from attributes onwards)
        var crcData = batchBytes.AsSpan(21);
        var crc = Crc32C.Compute(crcData);
        BinaryPrimitives.WriteUInt32BigEndian(batchBytes.AsSpan(17, 4), crc);

        return batchBytes;
    }

    /// <summary>
    /// Write a zigzag-encoded varint to a stream
    /// </summary>
    private static void WriteVarint(Stream stream, long value)
    {
        // Zigzag encode
        var encoded = (ulong)((value << 1) ^ (value >> 63));

        while (encoded > 0x7F)
        {
            stream.WriteByte((byte)((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        stream.WriteByte((byte)encoded);
    }

    /// <summary>
    /// Parse individual records from a RecordBatch
    /// </summary>
    private List<ParsedRecord> ParseRecordsFromBatch(byte[] batch)
    {
        var records = new List<ParsedRecord>();

        if (batch.Length < 61) return records;

        try
        {
            // Parse batch header
            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(0, 8));
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(8, 4));
            var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(batch.AsSpan(29, 8));
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(batch.AsSpan(57, 4));

            // Records start at offset 61
            var position = 61;

            for (int i = 0; i < recordCount && position < batch.Length; i++)
            {
                // Parse record length (varint)
                var (recordLength, lenBytes) = ReadVarint(batch, position);
                position += lenBytes;

                if (position + recordLength > batch.Length)
                    break;

                var recordStart = position;

                // Skip attributes (1 byte as varint)
                var (_, attrBytes) = ReadVarint(batch, position);
                position += attrBytes;

                // Read timestamp delta (varint)
                var (timestampDelta, tsBytes) = ReadVarint(batch, position);
                position += tsBytes;

                // Read offset delta (varint)
                var (offsetDelta, odBytes) = ReadVarint(batch, position);
                position += odBytes;

                // Read key length (varint, -1 for null)
                var (keyLength, klBytes) = ReadVarint(batch, position);
                position += klBytes;

                byte[]? key = null;
                if (keyLength >= 0)
                {
                    key = new byte[keyLength];
                    Array.Copy(batch, position, key, 0, keyLength);
                    position += (int)keyLength;
                }

                // Read value length (varint, -1 for null)
                var (valueLength, vlBytes) = ReadVarint(batch, position);
                position += vlBytes;

                byte[]? value = null;
                if (valueLength >= 0)
                {
                    value = new byte[valueLength];
                    Array.Copy(batch, position, value, 0, valueLength);
                    position += (int)valueLength;
                }

                // Skip headers count and headers
                var (headersCount, hcBytes) = ReadVarint(batch, position);
                position += hcBytes;

                for (int h = 0; h < headersCount; h++)
                {
                    var (headerKeyLen, hklBytes) = ReadVarint(batch, position);
                    position += hklBytes + (int)headerKeyLen;

                    var (headerValueLen, hvlBytes) = ReadVarint(batch, position);
                    position += hvlBytes;
                    if (headerValueLen >= 0)
                    {
                        position += (int)headerValueLen;
                    }
                }

                records.Add(new ParsedRecord
                {
                    Offset = baseOffset + offsetDelta,
                    Timestamp = baseTimestamp + timestampDelta,
                    Key = key,
                    Value = value,
                    Size = position - recordStart + lenBytes
                });
            }
        }
        catch
        {
            // If parsing fails, return what we have
        }

        return records;
    }

    /// <summary>
    /// Read a variable-length integer (zigzag encoded)
    /// </summary>
    private static (long value, int bytesRead) ReadVarint(byte[] data, int offset)
    {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;

        while (offset + bytesRead < data.Length)
        {
            byte b = data[offset + bytesRead];
            bytesRead++;

            result |= (long)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
            {
                // Zigzag decode
                return ((result >> 1) ^ -(result & 1), bytesRead);
            }

            shift += 7;
        }

        return (result, bytesRead);
    }

    private sealed class ParsedRecord
    {
        public long Offset { get; init; }
        public long Timestamp { get; init; }
        public byte[]? Key { get; init; }
        public byte[]? Value { get; init; }
        public int Size { get; init; }
    }
}

/// <summary>
/// Result of a compaction operation
/// </summary>
public sealed record CompactionResult(
    long RecordsRemoved,
    long BytesRemoved,
    int SegmentsCompacted)
{
    /// <summary>
    /// Number of partitions that were compacted
    /// </summary>
    public int PartitionsCompacted { get; init; }

    /// <summary>
    /// Number of partitions skipped due to dirty ratio threshold
    /// </summary>
    public int PartitionsSkipped { get; init; }

    /// <summary>
    /// Whether compaction was limited by MaxCompactionBytes
    /// </summary>
    public bool WasLimitedByMaxBytes { get; init; }

    /// <summary>
    /// Timestamp when compaction started
    /// </summary>
    public DateTimeOffset StartTime { get; init; }

    /// <summary>
    /// Duration of the compaction operation
    /// </summary>
    public TimeSpan Duration { get; init; }
}
