using System.Buffers.Binary;
using System.IO.Hashing;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// High-performance content hashing for record batches using XxHash64.
/// Hashes only the content portion (records data after the 61-byte header),
/// excluding metadata fields that change between retries (offset, sequence, timestamp).
/// </summary>
public static class RecordBatchHasher
{
    /// <summary>
    /// Compute a content hash for a record batch, covering only the records data
    /// (everything after the 61-byte header). This means two identical messages sent
    /// at different times with different offsets will produce the same hash.
    /// </summary>
    public static ulong ComputeContentHash(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length <= KafkaConstants.RecordBatch.HeaderSize)
            return 0;

        // Hash only the records portion (after 61-byte header)
        var recordsData = recordBatch[KafkaConstants.RecordBatch.HeaderSize..];
        return XxHash64.HashToUInt64(recordsData);
    }

    /// <summary>
    /// Compute a hash that includes key content fields from the header
    /// (attributes, producerId) plus the records data, but excludes
    /// offset, timestamp, sequence, and epoch which change on retries.
    /// </summary>
    public static ulong ComputeStableHash(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.HeaderSize)
            return 0;

        var hasher = new XxHash64();

        // Include attributes (compression, transactional flags) — offset 21, 2 bytes
        hasher.Append(recordBatch.Slice(KafkaConstants.RecordBatch.AttributesOffset, 2));

        // Include records data (after 61-byte header)
        if (recordBatch.Length > KafkaConstants.RecordBatch.HeaderSize)
        {
            hasher.Append(recordBatch[KafkaConstants.RecordBatch.HeaderSize..]);
        }

        return hasher.GetCurrentHashAsUInt64();
    }
}
