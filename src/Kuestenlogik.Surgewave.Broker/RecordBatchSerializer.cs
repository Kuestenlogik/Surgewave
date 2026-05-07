using System.Buffers;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Serialization;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;
using CompressionCodec = Kuestenlogik.Surgewave.Core.Util.CompressionCodec;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Handles serialization and deserialization of Kafka record batches (magic v2 format)
/// </summary>
public sealed class RecordBatchSerializer(ILogger<RecordBatchSerializer> logger)
{

    /// <summary>
    /// Serialize messages into Kafka record batch format (magic v2).
    /// Returns an exact-sized byte array (allocates).
    /// For zero-allocation path, use SerializeMessagesPooled.
    /// </summary>
    public byte[] SerializeMessages(List<Message> messages)
    {
        if (messages.Count == 0)
            return [];

        var (buffer, length) = SerializeMessagesPooled(messages);
        try
        {
            var result = new byte[length];
            buffer.AsSpan(0, length).CopyTo(result);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Serialize messages into Kafka record batch format (magic v2).
    /// Returns a pooled buffer and actual length. Caller MUST return buffer to ArrayPool.
    /// Zero-allocation hot path for high-throughput scenarios.
    /// </summary>
    public (byte[] Buffer, int Length) SerializeMessagesPooled(List<Message> messages)
    {
        if (messages.Count == 0)
            return ([], 0);

        var baseOffset = messages[0].Offset;
        var baseTimestamp = messages[0].Timestamp;

        // Calculate exact buffer size to avoid expansion/reallocation
        // Per-record overhead: length varint(5) + attrs(1) + timestamp varlong(10) + offset varint(5)
        //                     + key len varint(5) + value len varint(5) + headers count(1) = 32 bytes max
        // Add 10% margin for safety
        var dataSize = 0;
        foreach (var msg in messages)
        {
            dataSize += msg.Key.Length + msg.Value.Length;
        }
        var estimatedSize = KafkaConstants.RecordBatch.HeaderSize + (messages.Count * 35) + dataSize;

        // Rent main buffer and small varint scratch buffer
        var buffer = ArrayPool<byte>.Shared.Rent(estimatedSize);
        Span<byte> varintScratch = stackalloc byte[10]; // Max varint/varlong size

        var span = buffer.AsSpan();
        var pos = 0;

        // Base Offset (8 bytes)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(span.Slice(pos, 8), baseOffset);
        pos += 8;

        // Batch Length placeholder - will write later (4 bytes)
        var batchLengthPosition = pos;
        pos += 4;

        // Partition Leader Epoch (4 bytes) = 0
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span.Slice(pos, 4), 0);
        pos += 4;

        // Magic (1 byte) = 2
        span[pos++] = KafkaConstants.Magic.V2;

        // CRC placeholder - will write later (4 bytes)
        var crcPosition = pos;
        pos += 4;

        // CRC data starts here
        var crcDataStart = pos;

        // Attributes (2 bytes) = 0
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(span.Slice(pos, 2), 0);
        pos += 2;

        // Last Offset Delta (4 bytes)
        var lastOffsetDelta = (int)(messages[^1].Offset - baseOffset);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span.Slice(pos, 4), lastOffsetDelta);
        pos += 4;

        // Base Timestamp (8 bytes)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(span.Slice(pos, 8), baseTimestamp);
        pos += 8;

        // Max Timestamp (8 bytes)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(span.Slice(pos, 8), messages[^1].Timestamp);
        pos += 8;

        // Producer ID (8 bytes) = -1
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(span.Slice(pos, 8), -1);
        pos += 8;

        // Producer Epoch (2 bytes) = -1
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(span.Slice(pos, 2), -1);
        pos += 2;

        // Base Sequence (4 bytes) = -1
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span.Slice(pos, 4), -1);
        pos += 4;

        // Records Count (4 bytes)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span.Slice(pos, 4), messages.Count);
        pos += 4;

        // Write records inline (no separate allocation)
        foreach (var message in messages)
        {
            // Reserve space for record length prefix (will write later)
            var recordLengthPos = pos;
            var recordStartPos = pos + 5; // Max 5 bytes for varint
            var recordPos = recordStartPos;

            // Ensure buffer has space for this record
            var neededSpace = 50 + message.Key.Length + message.Value.Length;
            if (recordPos + neededSpace > buffer.Length)
            {
                var newBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                span.Slice(0, pos).CopyTo(newBuffer);
                ArrayPool<byte>.Shared.Return(buffer);
                buffer = newBuffer;
                span = buffer.AsSpan();
            }

            // attributes (int8) = 0
            span[recordPos++] = 0;

            // timestampDelta (varlong, zigzag encoded)
            var timestampDelta = message.Timestamp - baseTimestamp;
            var timestampDeltaEncoded = KafkaProtocolPrimitives.ZigzagEncode(timestampDelta);
            recordPos += VarintCodec.WriteVarLong(span.Slice(recordPos), (long)timestampDeltaEncoded);

            // offsetDelta (varint, signed zigzag encoded)
            var offsetDelta = (int)(message.Offset - baseOffset);
            var offsetDeltaEncoded = KafkaProtocolPrimitives.ZigzagEncode(offsetDelta);
            recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), (int)offsetDeltaEncoded);

            // key (varint length + bytes, signed zigzag encoded)
            if (message.Key.Length > 0)
            {
                var keyLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(message.Key.Length);
                recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), (int)keyLengthEncoded);
                message.Key.Span.CopyTo(span.Slice(recordPos));
                recordPos += message.Key.Length;
            }
            else
            {
                // -1 for null key, encoded with zigzag (-1 becomes 1)
                recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), 1);
            }

            // value (varint length + bytes, signed zigzag encoded)
            if (message.Value.Length > 0)
            {
                var valueLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(message.Value.Length);
                recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), (int)valueLengthEncoded);
                message.Value.Span.CopyTo(span.Slice(recordPos));
                recordPos += message.Value.Length;
            }
            else
            {
                // -1 for null value, encoded with zigzag (-1 becomes 1)
                recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), 1);
            }

            // headers (varint count = 0)
            recordPos += VarintCodec.WriteVarInt(span.Slice(recordPos), 0);

            // Calculate actual record content length
            var recordContentLength = recordPos - recordStartPos;
            var recordLengthEncoded = KafkaProtocolPrimitives.ZigzagEncode(recordContentLength);

            // Write record length varint and shift content if needed
            var lengthVarIntBytes = VarintCodec.WriteVarInt(varintScratch, (int)recordLengthEncoded);
            if (lengthVarIntBytes < 5)
            {
                // Shift record content to close the gap
                var actualRecordStart = recordLengthPos + lengthVarIntBytes;
                span.Slice(recordStartPos, recordContentLength).CopyTo(span.Slice(actualRecordStart));
                pos = actualRecordStart + recordContentLength;
            }
            else
            {
                pos = recordPos;
            }
            // Write the length prefix
            varintScratch.Slice(0, lengthVarIntBytes).CopyTo(span.Slice(recordLengthPos));
        }

        // Calculate and write batch length
        var batchLength = pos - batchLengthPosition - 4;
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(span.Slice(batchLengthPosition, 4), batchLength);

        // Calculate CRC over data from crcDataStart to end
        var crcData = span.Slice(crcDataStart, pos - crcDataStart);
        var crc = Crc32C.Compute(crcData);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(span.Slice(crcPosition, 4), crc);

        return (buffer, pos);
    }

    /// <summary>
    /// Parse a Kafka record batch (magic v2) into individual messages.
    /// Optimized with direct Span-based parsing - no MemoryStream allocations.
    /// </summary>
    public List<Message> ParseRecordBatch(byte[] recordBatch)
    {
        var span = recordBatch.AsSpan();
        var pos = 0;

        Log.ParseRecordBatchLength(logger, recordBatch.Length);

        // Read record batch header directly with BinaryPrimitives
        var baseOffset = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        var partitionLeaderEpoch = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        var magic = span[pos++];

        Log.ParseRecordBatchHeader(logger, baseOffset, batchLength, magic);

        if (magic != KafkaConstants.Magic.V2)
        {
            throw new NotSupportedException($"Only Kafka record batch magic v2 is supported, got v{magic}");
        }

        var crc = System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos, 2));
        pos += 2;
        var lastOffsetDelta = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        var baseTimestamp = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        var maxTimestamp = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        var producerId = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span.Slice(pos, 8));
        pos += 8;
        var producerEpoch = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(span.Slice(pos, 2));
        pos += 2;
        var baseSequence = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;
        var recordCount = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(pos, 4));
        pos += 4;

        Log.ParseRecordBatchRecordCount(logger, recordCount, pos);

        // Extract compression type from attributes (bits 0-2)
        var compressionType = attributes & KafkaConstants.Compression.Mask;

        // Calculate records size
        var headerSizeAfterLength = KafkaConstants.RecordBatch.HeaderSize -
                                     KafkaConstants.RecordBatch.BaseOffsetSize -
                                     KafkaConstants.RecordBatch.LengthSize;
        var recordsSize = batchLength - headerSizeAfterLength;
        Log.ParseRecordBatchRecordsSize(logger, recordsSize, span.Length - pos);

        // Get records data as span slice (zero-copy)
        var compressedRecordsSpan = span.Slice(pos, recordsSize);
        Log.ParseRecordBatchBytesRead(logger, compressedRecordsSpan.Length);

        // Decompress if needed - track backing array for zero-copy Memory slices
        // Use pooled decompression to avoid intermediate ToArray() allocation
        ReadOnlySpan<byte> recordsSpan;
        byte[] recordsBackingArray;
        int recordsBackingOffset;
        int recordsBackingLength;
        if (compressionType != KafkaConstants.Compression.None)
        {
            // DecompressPooled avoids the intermediate ToArray() allocation
            // Note: We don't return the buffer to pool because Message objects hold Memory references
            var (buffer, length, _) = CompressionCodec.DecompressPooled(compressedRecordsSpan, compressionType);
            recordsBackingArray = buffer;
            recordsBackingLength = length;
            recordsBackingOffset = 0;
            recordsSpan = buffer.AsSpan(0, length);
            Log.ParseRecordBatchDecompressed(logger, CompressionCodec.GetCompressionName(compressionType),
                compressedRecordsSpan.Length, length);
        }
        else
        {
            recordsBackingArray = recordBatch;
            recordsBackingOffset = pos; // Offset into original array
            recordsBackingLength = recordsSize;
            recordsSpan = compressedRecordsSpan;
        }

        var messages = new List<Message>(recordCount);
        var recordsPos = 0;

        // Parse individual records inline (no KafkaProtocolReader allocation per record)
        for (int i = 0; i < recordCount; i++)
        {
            Log.ParseRecordBatchParsingRecord(logger, i, recordsPos, recordsSpan.Length - recordsPos);

            // Read the record length - zigzag-encoded varint
            var recordLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var recordLength = KafkaProtocolPrimitives.ZigzagDecode((uint)recordLengthRaw);
            Log.ParseRecordBatchRecordLength(logger, i, recordLength, recordLengthRaw);

            // Parse record content inline
            var recordStart = recordsPos;

            var recordAttributes = (sbyte)recordsSpan[recordsPos++];
            Log.ParseRecordBatchAttributes(logger, i, recordAttributes);

            // timestampDelta - zigzag-encoded varlong
            var timestampDeltaRaw = VarintCodec.ReadVarLong(recordsSpan, ref recordsPos);
            var timestampDelta = KafkaProtocolPrimitives.ZigzagDecode((ulong)timestampDeltaRaw);
            Log.ParseRecordBatchTimestampDelta(logger, i, timestampDelta, timestampDeltaRaw);

            // offsetDelta - zigzag-encoded varint
            var offsetDeltaRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var offsetDelta = KafkaProtocolPrimitives.ZigzagDecode((uint)offsetDeltaRaw);
            Log.ParseRecordBatchOffsetDelta(logger, i, offsetDelta, offsetDeltaRaw);

            // keyLength - zigzag-encoded varint
            var keyLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var keyLength = KafkaProtocolPrimitives.ZigzagDecode((uint)keyLengthRaw);
            Log.ParseRecordBatchKeyLength(logger, i, keyLength, keyLengthRaw);

            // Zero-copy: use Memory slice instead of ToArray
            ReadOnlyMemory<byte> key;
            if (keyLength <= 0)
            {
                key = ReadOnlyMemory<byte>.Empty;
            }
            else
            {
                key = recordsBackingArray.AsMemory(recordsBackingOffset + recordsPos, keyLength);
                recordsPos += keyLength;
            }

            // valueLength - zigzag-encoded varint
            var valueLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var valueLength = KafkaProtocolPrimitives.ZigzagDecode((uint)valueLengthRaw);
            Log.ParseRecordBatchValueLength(logger, i, valueLength, valueLengthRaw);

            // Zero-copy: use Memory slice instead of ToArray
            ReadOnlyMemory<byte> value;
            if (valueLength <= 0)
            {
                value = ReadOnlyMemory<byte>.Empty;
            }
            else
            {
                value = recordsBackingArray.AsMemory(recordsBackingOffset + recordsPos, valueLength);
                recordsPos += valueLength;
            }

            // Read headers count and skip them
            var headerCount = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            for (int h = 0; h < headerCount; h++)
            {
                var headerKeyLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerKeyLen > 0) recordsPos += headerKeyLen;
                var headerValueLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerValueLen > 0) recordsPos += headerValueLen;
            }

            var message = new Message
            {
                Offset = baseOffset + offsetDelta,
                Timestamp = baseTimestamp + timestampDelta,
                Key = key,
                Value = value,
                Headers = Array.Empty<byte>()
            };

            Log.ParseRecordBatchMessageCreated(logger, message.Offset, message.Timestamp, message.Key.Length, message.Value.Length);
            messages.Add(message);
        }

        Log.ParseRecordBatchComplete(logger, messages.Count);
        return messages;
    }

    /// <summary>
    /// Stream records from a record batch directly to a BigEndianWriter.
    /// Zero-allocation path that avoids creating intermediate Message objects.
    /// Returns the number of records written.
    /// </summary>
    internal int StreamRecordsToWriter(ReadOnlySpan<byte> recordBatch, BigEndianWriter writer)
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

        // Decompress if needed - use pooled decompression to avoid intermediate allocation
        ReadOnlySpan<byte> recordsSpan;
        byte[]? decompressedBuffer = null;
        int decompressedLength = 0;
        if (compressionType != KafkaConstants.Compression.None)
        {
            var (buffer, length, _) = CompressionCodec.DecompressPooled(compressedRecordsSpan, compressionType);
            decompressedBuffer = buffer;
            decompressedLength = length;
            recordsSpan = buffer.AsSpan(0, length);
        }
        else
        {
            recordsSpan = compressedRecordsSpan;
        }

        var recordsPos = 0;

        // Stream records directly to writer without creating Message objects
        for (int i = 0; i < recordCount; i++)
        {
            // Read record length
            VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);

            recordsPos++; // Skip record attributes

            // timestampDelta
            var timestampDeltaRaw = VarintCodec.ReadVarLong(recordsSpan, ref recordsPos);
            var timestampDelta = KafkaProtocolPrimitives.ZigzagDecode((ulong)timestampDeltaRaw);

            // offsetDelta
            var offsetDeltaRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var offsetDelta = KafkaProtocolPrimitives.ZigzagDecode((uint)offsetDeltaRaw);

            // keyLength
            var keyLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var keyLength = KafkaProtocolPrimitives.ZigzagDecode((uint)keyLengthRaw);

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
            var valueLength = KafkaProtocolPrimitives.ZigzagDecode((uint)valueLengthRaw);

            // Write value
            writer.Write(valueLength);
            if (valueLength > 0)
            {
                writer.Write(recordsSpan.Slice(recordsPos, valueLength));
                recordsPos += valueLength;
            }

            // Skip headers
            var headerCount = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            for (int h = 0; h < headerCount; h++)
            {
                var headerKeyLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerKeyLen > 0) recordsPos += headerKeyLen;
                var headerValueLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerValueLen > 0) recordsPos += headerValueLen;
            }
        }

        return recordCount;
    }

    /// <summary>
    /// Combine multiple record batches into a single byte array.
    /// </summary>
    public byte[] CombineBatches(List<byte[]> batches)
    {
        if (batches.Count == 0)
            return Array.Empty<byte>();

        if (batches.Count == 1)
        {
            return batches[0];
        }

        var totalSize = batches.Sum(b => b.Length);
        var result = new byte[totalSize];
        var offset = 0;

        foreach (var batch in batches)
        {
            Array.Copy(batch, 0, result, offset, batch.Length);
            offset += batch.Length;
        }

        return result;
    }

    /// <summary>
    /// Combine multiple record batches into a pooled byte array — zero GC allocation
    /// on the consume hot path. The caller MUST return the buffer to
    /// <see cref="System.Buffers.ArrayPool{T}.Shared"/> after use.
    /// </summary>
    /// <returns>A tuple of (pooled buffer, valid byte count). The buffer may be larger
    /// than <c>validLength</c> — only the first <c>validLength</c> bytes contain batch data.</returns>
    public (byte[] buffer, int validLength) CombineBatchesPooled(List<byte[]> batches)
    {
        if (batches.Count == 0)
            return (Array.Empty<byte>(), 0);

        if (batches.Count == 1)
            return (batches[0], batches[0].Length);

        var totalSize = batches.Sum(b => b.Length);
        var result = System.Buffers.ArrayPool<byte>.Shared.Rent(totalSize);
        var offset = 0;

        foreach (var batch in batches)
        {
            Array.Copy(batch, 0, result, offset, batch.Length);
            offset += batch.Length;
        }

        return (result, totalSize);
    }

    /// <summary>
    /// Get the record count from a batch header without parsing records.
    /// Very fast - just reads 4 bytes at offset 57.
    /// </summary>
    internal static int GetRecordCount(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < 61) return 0;
        return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57, 4));
    }

    /// <summary>
    /// Stream raw record batch bytes directly to a BigEndianWriter without per-record parsing.
    /// This is the zero-copy fast path - just extracts metadata from batch header.
    /// Returns the number of records in the batch.
    /// </summary>
    internal int StreamBatchRawToWriter(ReadOnlySpan<byte> recordBatch, BigEndianWriter writer)
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
            var timestampDelta = KafkaProtocolPrimitives.ZigzagDecode((ulong)timestampDeltaRaw);

            // offsetDelta
            var offsetDeltaRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var offsetDelta = KafkaProtocolPrimitives.ZigzagDecode((uint)offsetDeltaRaw);

            // keyLength
            var keyLengthRaw = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            var keyLength = KafkaProtocolPrimitives.ZigzagDecode((uint)keyLengthRaw);

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
            var valueLength = KafkaProtocolPrimitives.ZigzagDecode((uint)valueLengthRaw);

            // Write value
            writer.Write(valueLength);
            if (valueLength > 0)
            {
                writer.Write(recordsSpan.Slice(recordsPos, valueLength));
                recordsPos += valueLength;
            }

            // Skip headers
            var headerCount = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
            for (int h = 0; h < headerCount; h++)
            {
                var headerKeyLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerKeyLen > 0) recordsPos += headerKeyLen;
                var headerValueLen = VarintCodec.ReadVarInt(recordsSpan, ref recordsPos);
                if (headerValueLen > 0) recordsPos += headerValueLen;
            }
        }

        return recordCount;
    }

    #region BigEndian Read/Write Helpers

    private static uint ReadUInt32BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[4];
        reader.Read(buffer);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(buffer);
    }

    private static short ReadInt16BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[2];
        reader.Read(buffer);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(buffer);
    }

    private static int ReadInt32BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[4];
        reader.Read(buffer);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(buffer);
    }

    private static long ReadInt64BigEndian(BinaryReader reader)
    {
        Span<byte> buffer = stackalloc byte[8];
        reader.Read(buffer);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(buffer);
    }

    private static void WriteBigEndianInt16(BinaryWriter writer, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt32(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt64(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianUInt32(BinaryWriter writer, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    #endregion
}
