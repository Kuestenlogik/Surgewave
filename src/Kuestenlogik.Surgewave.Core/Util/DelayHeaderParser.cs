using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Parses record-level headers from raw Kafka RecordBatch bytes.
/// Used to extract the surgewave-deliver-at-ms header for delayed delivery.
/// </summary>
public static class DelayHeaderParser
{
    /// <summary>
    /// Well-known header key for delayed message delivery.
    /// Value is a UTF-8 encoded long timestamp in milliseconds since epoch.
    /// </summary>
    public const string DeliverAtHeaderKey = "surgewave-deliver-at-ms";

    /// <summary>
    /// Well-known header key for relative delay in milliseconds.
    /// Value is a UTF-8 encoded long milliseconds from produce time.
    /// </summary>
    public const string DeliverAfterHeaderKey = "surgewave-deliver-after-ms";

    private static readonly byte[] DeliverAtKeyBytes = Encoding.UTF8.GetBytes(DeliverAtHeaderKey);
    private static readonly byte[] DeliverAfterKeyBytes = Encoding.UTF8.GetBytes(DeliverAfterHeaderKey);

    /// <summary>
    /// Extract the maximum deliver-at timestamp from a record batch.
    /// Scans all records in the batch for surgewave-deliver-at-ms or surgewave-deliver-after-ms headers.
    /// Returns null if no delay headers are found.
    /// </summary>
    public static long? ExtractDeliverAtTimestamp(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length <= KafkaConstants.RecordBatch.HeaderSize)
            return null;

        // Check compression — if compressed, we can't parse individual records without decompression
        var compressionType = CompressionCodec.GetCompressionTypeFromBatch(recordBatch);
        if (compressionType != KafkaConstants.Compression.None)
        {
            return ExtractFromCompressedBatch(recordBatch, compressionType);
        }

        return ExtractFromUncompressedBatch(recordBatch);
    }

    private static long? ExtractFromUncompressedBatch(ReadOnlySpan<byte> recordBatch)
    {
        var recordCount = CompressionCodec.GetRecordCount(recordBatch);
        if (recordCount <= 0)
            return null;

        // Get base timestamp for relative delay calculation
        var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.BaseTimestampOffset, 8));

        var recordsData = recordBatch[KafkaConstants.RecordBatch.HeaderSize..];
        long? maxDeliverAt = null;

        var offset = 0;
        for (var i = 0; i < recordCount && offset < recordsData.Length; i++)
        {
            var deliverAt = ParseRecordForDeliverAt(recordsData, ref offset, baseTimestamp);
            if (deliverAt.HasValue)
            {
                maxDeliverAt = maxDeliverAt.HasValue
                    ? Math.Max(maxDeliverAt.Value, deliverAt.Value)
                    : deliverAt.Value;
            }
        }

        return maxDeliverAt;
    }

    private static long? ExtractFromCompressedBatch(ReadOnlySpan<byte> recordBatch, int compressionType)
    {
        // Decompress records section and parse
        var recordsCompressed = recordBatch[KafkaConstants.RecordBatch.HeaderSize..];
        byte[] decompressed;
        try
        {
            decompressed = CompressionCodec.Decompress(recordsCompressed.ToArray(), compressionType);
        }
        catch
        {
            return null; // Can't decompress — skip delay parsing
        }

        var recordCount = CompressionCodec.GetRecordCount(recordBatch);
        var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.BaseTimestampOffset, 8));

        long? maxDeliverAt = null;
        var offset = 0;

        for (var i = 0; i < recordCount && offset < decompressed.Length; i++)
        {
            var deliverAt = ParseRecordForDeliverAt(decompressed, ref offset, baseTimestamp);
            if (deliverAt.HasValue)
            {
                maxDeliverAt = maxDeliverAt.HasValue
                    ? Math.Max(maxDeliverAt.Value, deliverAt.Value)
                    : deliverAt.Value;
            }
        }

        return maxDeliverAt;
    }

    /// <summary>
    /// Parse a single record within the records section, looking for deliver-at headers.
    /// Advances offset past this record.
    /// </summary>
    private static long? ParseRecordForDeliverAt(ReadOnlySpan<byte> data, ref int offset, long baseTimestamp)
    {
        if (offset >= data.Length)
            return null;

        // Record format: length (varint) | attributes (1) | timestampDelta (varint) | offsetDelta (varint) |
        //                keyLength (varint) | key (bytes) | valueLength (varint) | value (bytes) |
        //                headersCount (varint) | [header]*

        var recordLength = ReadVarInt(data, ref offset);
        if (recordLength <= 0 || offset + recordLength > data.Length)
        {
            offset = data.Length; // Bail out
            return null;
        }

        var recordEnd = offset + recordLength;

        // Skip attributes (1 byte)
        if (offset >= recordEnd) { offset = recordEnd; return null; }
        offset++;

        // Skip timestampDelta (varint)
        ReadVarInt(data, ref offset);

        // Skip offsetDelta (varint)
        ReadVarInt(data, ref offset);

        // Skip key
        var keyLength = ReadVarInt(data, ref offset);
        if (keyLength > 0)
            offset += keyLength;

        // Skip value
        var valueLength = ReadVarInt(data, ref offset);
        if (valueLength > 0)
            offset += valueLength;

        // Parse headers
        var headersCount = ReadVarInt(data, ref offset);
        long? deliverAt = null;

        for (var h = 0; h < headersCount && offset < recordEnd; h++)
        {
            var headerKeyLength = ReadVarInt(data, ref offset);
            if (headerKeyLength <= 0 || offset + headerKeyLength > recordEnd)
                break;

            var headerKey = data.Slice(offset, headerKeyLength);
            offset += headerKeyLength;

            var headerValueLength = ReadVarInt(data, ref offset);
            ReadOnlySpan<byte> headerValue = default;
            if (headerValueLength > 0 && offset + headerValueLength <= recordEnd)
            {
                headerValue = data.Slice(offset, headerValueLength);
                offset += headerValueLength;
            }

            // Check for deliver-at header
            if (headerKey.SequenceEqual(DeliverAtKeyBytes) && headerValue.Length > 0)
            {
                if (TryParseTimestamp(headerValue, out var ts))
                    deliverAt = ts;
            }
            // Check for deliver-after header (relative delay)
            else if (headerKey.SequenceEqual(DeliverAfterKeyBytes) && headerValue.Length > 0)
            {
                if (TryParseTimestamp(headerValue, out var delayMs))
                {
                    var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    deliverAt = nowMs + delayMs;
                }
            }
        }

        offset = recordEnd;
        return deliverAt;
    }

    private static bool TryParseTimestamp(ReadOnlySpan<byte> utf8Value, out long timestamp)
    {
        Span<char> chars = stackalloc char[utf8Value.Length];
        var charCount = Encoding.UTF8.GetChars(utf8Value, chars);
        return long.TryParse(chars[..charCount], NumberStyles.Integer, CultureInfo.InvariantCulture, out timestamp);
    }

    /// <summary>
    /// Read a ZigZag-encoded variable-length integer.
    /// </summary>
    private static int ReadVarInt(ReadOnlySpan<byte> data, ref int offset)
    {
        var value = 0;
        var shift = 0;

        while (offset < data.Length)
        {
            var b = data[offset++];
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                // ZigZag decode
                return (int)((uint)value >> 1) ^ -(value & 1);
            }
            shift += 7;
            if (shift >= 32)
                break;
        }

        return value;
    }
}
