using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Specifies how the broker should handle corrupted record batches.
/// </summary>
public enum CorruptionRecoveryMode
{
    /// <summary>
    /// Skip corrupted batches and continue reading. Consumers will see a gap in offsets.
    /// This is the default mode for production environments.
    /// </summary>
    SkipAndContinue,

    /// <summary>
    /// Return an error immediately when corruption is detected.
    /// Requires manual intervention to recover.
    /// </summary>
    FailFast
}

/// <summary>
/// Information about a corrupted record batch.
/// </summary>
/// <param name="Topic">The topic containing the corrupted batch</param>
/// <param name="Partition">The partition containing the corrupted batch</param>
/// <param name="BaseOffset">The base offset of the corrupted batch</param>
/// <param name="ExpectedCrc">The CRC value stored in the batch header</param>
/// <param name="ActualCrc">The CRC value computed from the batch data</param>
/// <param name="BatchLength">The length of the corrupted batch in bytes</param>
public readonly record struct CorruptedBatchInfo(
    string Topic,
    int Partition,
    long BaseOffset,
    uint ExpectedCrc,
    uint ActualCrc,
    int BatchLength);

/// <summary>
/// Handler for corruption detection events.
/// </summary>
public interface ICorruptionHandler
{
    /// <summary>
    /// Called when a corrupted batch is detected during read operations.
    /// </summary>
    /// <param name="info">Information about the corrupted batch</param>
    void OnCorruptionDetected(CorruptedBatchInfo info);
}

/// <summary>
/// Validates record batch integrity using CRC32-C checksums.
/// </summary>
public static class RecordBatchValidator
{
    /// <summary>
    /// Minimum size of a valid RecordBatch header.
    /// </summary>
    public const int MinBatchHeaderSize = 61;

    /// <summary>
    /// Offset of the CRC field in the batch header.
    /// </summary>
    public const int CrcOffset = 17;

    /// <summary>
    /// Offset where CRC calculation starts (attributes field).
    /// </summary>
    public const int CrcDataOffset = 21;

    /// <summary>
    /// Validates the CRC checksum of a record batch.
    /// </summary>
    /// <param name="batch">The record batch bytes to validate</param>
    /// <param name="expected">The CRC value stored in the batch header</param>
    /// <param name="actual">The CRC value computed from the batch data</param>
    /// <returns>True if the CRC is valid, false otherwise</returns>
    public static bool ValidateCrc(ReadOnlySpan<byte> batch, out uint expected, out uint actual)
    {
        if (batch.Length < MinBatchHeaderSize)
        {
            expected = 0;
            actual = 0;
            return false;
        }

        // Read expected CRC from header (bytes 17-20, big-endian)
        expected = BinaryPrimitives.ReadUInt32BigEndian(batch.Slice(CrcOffset, 4));

        // Compute actual CRC from data (bytes 21+)
        actual = Crc32C.Compute(batch[CrcDataOffset..]);

        return expected == actual;
    }

    /// <summary>
    /// Validates the CRC checksum of a record batch.
    /// </summary>
    /// <param name="batch">The record batch bytes to validate</param>
    /// <returns>True if the CRC is valid, false otherwise</returns>
    public static bool ValidateCrc(ReadOnlySpan<byte> batch)
    {
        return ValidateCrc(batch, out _, out _);
    }

    /// <summary>
    /// The single CRC pass an append needs (#85). Shared by <see cref="PartitionLog"/> and
    /// <see cref="EphemeralPartitionLog"/> so the three copies of this logic cannot drift apart.
    /// </summary>
    /// <returns>
    /// <see cref="BatchCrcMode.Recompute"/>: the computed CRC — the caller overwrites the field
    /// with it. <see cref="BatchCrcMode.Validate"/>: the computed CRC, which equals the stored one
    /// (a mismatch throws <see cref="DataCorruptionException"/>). <see cref="BatchCrcMode.Trusted"/>:
    /// 0 without computing anything — the caller must leave the field alone.
    /// </returns>
    public static uint PrepareAppendCrc(ReadOnlySpan<byte> batch, BatchCrcMode crcMode, string topic, int partition)
    {
        if (crcMode == BatchCrcMode.Trusted)
        {
            return 0;
        }

        if (batch.Length < MinBatchHeaderSize)
        {
            // Don't report this as a CRC mismatch — "expected 0x00000000, actual 0x00000000" tells
            // nobody anything. The batch is simply too short to be a RecordBatch.
            throw new DataCorruptionException(
                $"Data corruption detected in {topic}-{partition}: batch is {batch.Length} bytes, " +
                $"below the {MinBatchHeaderSize}-byte RecordBatch header");
        }

        var actual = Crc32C.Compute(batch[CrcDataOffset..]);
        if (crcMode == BatchCrcMode.Validate)
        {
            var expected = BinaryPrimitives.ReadUInt32BigEndian(batch.Slice(CrcOffset, 4));
            if (expected != actual)
            {
                throw new DataCorruptionException(topic, partition, GetBaseOffset(batch), expected, actual);
            }
        }

        return actual;
    }

    /// <summary>
    /// Extracts the base offset from a record batch header.
    /// </summary>
    /// <param name="batch">The record batch bytes</param>
    /// <returns>The base offset, or -1 if the batch is too small</returns>
    public static long GetBaseOffset(ReadOnlySpan<byte> batch)
    {
        if (batch.Length < 8)
            return -1;

        return BinaryPrimitives.ReadInt64BigEndian(batch[..8]);
    }

    /// <summary>
    /// Extracts the batch length from a record batch header.
    /// </summary>
    /// <param name="batch">The record batch bytes</param>
    /// <returns>The batch length (excluding the first 12 bytes), or -1 if the batch is too small</returns>
    public static int GetBatchLength(ReadOnlySpan<byte> batch)
    {
        if (batch.Length < 12)
            return -1;

        return BinaryPrimitives.ReadInt32BigEndian(batch.Slice(8, 4));
    }
}
