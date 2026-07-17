using System.Buffers;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Streams stored Kafka RecordBatch-v2 bytes into a <see cref="BigEndianWriter"/> for the
/// native consume path (fetch). Split out of <c>RecordBatchSerializer</c> (#59 b4-tier2) so
/// the codec relocates to Core while these hot per-record writes keep the concrete writer —
/// no interface dispatch in the loop.
/// </summary>
internal static class RecordBatchStreamer
{
    /// <summary>
    /// Stream records from a record batch directly to a BigEndianWriter.
    /// Zero-allocation path that avoids creating intermediate Message objects.
    /// Returns the number of records written.
    /// </summary>
    public static int StreamRecordsToWriter(ReadOnlySpan<byte> recordBatch, BigEndianWriter writer)
    {
        var span = recordBatch;
        var pos = 0;

        // Read record batch header
        var baseOffset = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        pos += 4; // Skip partitionLeaderEpoch
        var magic = span[pos++];

        if (magic != KafkaConstants.Magic.V2)
        {
            return 0;
        }

        pos += 4; // Skip crc
        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos, 2));
        pos += 2;
        pos += 4; // Skip lastOffsetDelta
        var baseTimestamp = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        pos += 8; // Skip maxTimestamp
        pos += 8; // Skip producerId
        pos += 2; // Skip producerEpoch
        pos += 4; // Skip baseSequence
        var recordCount = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;

        // Extract compression type
        var compressionType = attributes & KafkaConstants.Compression.Mask;

        // Calculate records size
        var headerSizeAfterLength = KafkaConstants.RecordBatch.HeaderSize -
                                     KafkaConstants.RecordBatch.BaseOffsetSize -
                                     KafkaConstants.RecordBatch.LengthSize;
        var recordsSize = batchLength - headerSizeAfterLength;

        var compressedRecordsSpan = span.Slice(pos, recordsSize);

        // Decompress if needed - use pooled decompression to avoid intermediate allocation.
        // DecompressPooled's contract: if IsPooled, the CALLER must return the buffer to the pool —
        // the streamed records are fully copied into the writer below, so the buffer's lifetime ends
        // when this method returns (#74: this rent used to leak on every compressed batch).
        ReadOnlySpan<byte> recordsSpan;
        byte[]? pooledBuffer = null;
        if (compressionType != KafkaConstants.Compression.None)
        {
            var (buffer, length, isPooled) = CompressionCodec.DecompressPooled(compressedRecordsSpan, compressionType);
            pooledBuffer = isPooled ? buffer : null;
            recordsSpan = buffer.AsSpan(0, length);
        }
        else
        {
            recordsSpan = compressedRecordsSpan;
        }

        try
        {
            return StreamParsedRecords(recordsSpan, writer, recordCount, baseOffset, baseTimestamp);
        }
        finally
        {
            if (pooledBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(pooledBuffer);
            }
        }
    }

    /// <summary>
    /// Streams the (already decompressed) records section to the writer, skipping headers. Split out
    /// of <see cref="StreamRecordsToWriter"/> so the pooled decompression buffer can be returned in a
    /// finally around the parse loop (#74).
    /// </summary>
    private static int StreamParsedRecords(
        ReadOnlySpan<byte> recordsSpan, BigEndianWriter writer, int recordCount, long baseOffset, long baseTimestamp)
    {
        var recordsPos = 0;

        // Stream records directly to writer without creating Message objects
        for (int i = 0; i < recordCount; i++)
        {
            // Read record length
            VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);

            recordsPos++; // Skip record attributes

            // timestampDelta
            var timestampDeltaRaw = VarintCodec.ReadVarLong(recordsSpan, ref recordsPos);
            var timestampDelta = ZigZag.Decode((ulong)timestampDeltaRaw);

            // offsetDelta
            var offsetDeltaRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var offsetDelta = ZigZag.Decode((uint)offsetDeltaRaw);

            // keyLength
            var keyLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var keyLength = ZigZag.Decode((uint)keyLengthRaw);

            // Write to output: offset, timestamp
            writer.Write(baseOffset + offsetDelta);
            writer.Write(baseTimestamp + timestampDelta);

            // Write key
            if (keyLength <= 0)
            {
                writer.Write(-1); // null key
            }
            else
            {
                writer.Write(keyLength);
                writer.Write(recordsSpan.Slice(recordsPos, keyLength));
                recordsPos += keyLength;
            }

            // valueLength
            var valueLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var valueLength = ZigZag.Decode((uint)valueLengthRaw);

            // Write value
            writer.Write(valueLength);
            if (valueLength > 0)
            {
                writer.Write(recordsSpan.Slice(recordsPos, valueLength));
                recordsPos += valueLength;
            }

            // Skip headers — Kafka-v2 header lengths sind zigzag-signed
            // varints (siehe WriteHeadersFromNativeBlock fuer den Hintergrund).
            var headerCount = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
            for (int h = 0; h < headerCount; h++)
            {
                var headerKeyLen = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
                if (headerKeyLen > 0) recordsPos += headerKeyLen;
                var headerValueLen = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
                if (headerValueLen > 0) recordsPos += headerValueLen;
            }
        }

        return recordCount;
    }

    /// <summary>
    /// Stream raw record batch bytes directly to a BigEndianWriter without per-record parsing.
    /// This is the zero-copy fast path - just extracts metadata from batch header.
    /// Returns the number of records in the batch.
    /// </summary>
    /// <summary>
    /// Reads the record count out of a RecordBatch-v2 header without parsing any records, so the
    /// fetch/push paths can size their writer up front (#83). Returns 0 for truncated batches,
    /// mirroring <see cref="StreamBatchRawToWriter"/>'s 61-byte guard.
    /// </summary>
    /// <remarks>
    /// The count is producer-controlled: Kafka-compat produce stores client batches verbatim, so a
    /// corrupt or hostile batch can claim any int32. It is clamped to what the remaining bytes can
    /// physically hold (7 bytes is the smallest possible record) — an unclamped value would
    /// overflow the capacity estimate and throw out of the caller's per-batch try/catch, killing
    /// the connection instead of skipping the one bad batch.
    /// </remarks>
    public static int PeekRecordCount(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < 61)
        {
            return 0;
        }

        const int MinRecordBytes = 7;
        var count = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57, 4));
        return Math.Clamp(count, 0, (recordBatch.Length - 61) / MinRecordBytes);
    }

    public static int StreamBatchRawToWriter(ReadOnlySpan<byte> recordBatch, BigEndianWriter writer)
    {
        if (recordBatch.Length < 61)
        {
            return 0;
        }

        // Extract minimal header info
        var baseOffset = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(recordBatch);
        var recordCount = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57, 4));
        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(recordBatch.Slice(21, 2));
        var compressionType = attributes & KafkaConstants.Compression.Mask;

        // If compressed, we must decompress and parse (can't avoid this)
        if (compressionType != KafkaConstants.Compression.None)
        {
            // Fall back to full parsing for compressed batches (span-based, no ToArray)
            return StreamRecordsToWriter(recordBatch, writer);
        }

        // Uncompressed: stream records directly without creating Message objects
        var baseTimestamp = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(27, 8));

        // Calculate records start position
        var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(8, 4));
        var headerSizeAfterLength = KafkaConstants.RecordBatch.HeaderSize -
                                     KafkaConstants.RecordBatch.BaseOffsetSize -
                                     KafkaConstants.RecordBatch.LengthSize;
        var recordsStart = KafkaConstants.RecordBatch.HeaderSize;
        var recordsSize = batchLength - headerSizeAfterLength;

        var recordsSpan = recordBatch.Slice(recordsStart, recordsSize);
        var recordsPos = 0;

        // Stream records directly to writer
        for (int i = 0; i < recordCount; i++)
        {
            // Read record length (zigzag varint)
            VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);

            recordsPos++; // Skip record attributes

            // timestampDelta
            var timestampDeltaRaw = VarintCodec.ReadVarLong(recordsSpan, ref recordsPos);
            var timestampDelta = ZigZag.Decode((ulong)timestampDeltaRaw);

            // offsetDelta
            var offsetDeltaRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var offsetDelta = ZigZag.Decode((uint)offsetDeltaRaw);

            // keyLength
            var keyLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var keyLength = ZigZag.Decode((uint)keyLengthRaw);

            // Write offset + timestamp
            writer.Write(baseOffset + offsetDelta);
            writer.Write(baseTimestamp + timestampDelta);

            // Write key
            if (keyLength <= 0)
            {
                writer.Write(-1);
            }
            else
            {
                writer.Write(keyLength);
                writer.Write(recordsSpan.Slice(recordsPos, keyLength));
                recordsPos += keyLength;
            }

            // valueLength
            var valueLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var valueLength = ZigZag.Decode((uint)valueLengthRaw);

            // Write value
            writer.Write(valueLength);
            if (valueLength > 0)
            {
                writer.Write(recordsSpan.Slice(recordsPos, valueLength));
                recordsPos += valueLength;
            }

            // Translate the Kafka-style headers back into the native wire
            // block (int32 count + int32-prefixed entries) so the client can
            // decode them with NativeMessageHeaderCodec. Kafka-v2 header
            // lengths sind zigzag-signed varints, der native Block ist
            // int32 big-endian (Decoder-friendly).
            var headerCount = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
            writer.Write(headerCount);
            for (int h = 0; h < headerCount; h++)
            {
                var headerKeyLen = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
                writer.Write(headerKeyLen);
                if (headerKeyLen > 0)
                {
                    writer.Write(recordsSpan.Slice(recordsPos, headerKeyLen));
                    recordsPos += headerKeyLen;
                }
                var headerValueLen = ZigZag.Decode((uint)VarintCodec.ReadVarInt(recordsSpan, ref recordsPos));
                writer.Write(headerValueLen);
                if (headerValueLen > 0)
                {
                    writer.Write(recordsSpan.Slice(recordsPos, headerValueLen));
                    recordsPos += headerValueLen;
                }
            }
        }

        return recordCount;
    }
}