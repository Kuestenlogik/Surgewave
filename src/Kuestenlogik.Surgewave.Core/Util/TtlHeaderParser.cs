using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Parses record-level headers from raw Kafka RecordBatch bytes.
/// Used to extract the surgewave-ttl-ms header for per-message TTL.
/// </summary>
public static class TtlHeaderParser
{
    /// <summary>
    /// Well-known header key for message time-to-live in milliseconds.
    /// Value is a UTF-8 encoded long representing TTL duration in milliseconds.
    /// </summary>
    public const string TtlHeaderKey = "surgewave-ttl-ms";

    private static readonly byte[] TtlKeyBytes = Encoding.UTF8.GetBytes(TtlHeaderKey);

    /// <summary>
    /// Extract the TTL from a record batch and calculate the expiry timestamp.
    /// Scans all records in the batch for surgewave-ttl-ms headers.
    /// Returns the minimum expiry timestamp (baseTimestamp + ttlMs) if found, null otherwise.
    /// </summary>
    public static long? ExtractExpiryTimestamp(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length <= KafkaConstants.RecordBatch.HeaderSize)
            return null;

        // Check compression -- if compressed, we can't parse individual records without decompression
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

        // Get base timestamp for expiry calculation
        var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.BaseTimestampOffset, 8));

        var recordsData = recordBatch[KafkaConstants.RecordBatch.HeaderSize..];
        long? minExpiry = null;

        var offset = 0;
        for (var i = 0; i < recordCount && offset < recordsData.Length; i++)
        {
            var expiry = ParseRecordForExpiry(recordsData, ref offset, baseTimestamp);
            if (expiry.HasValue)
            {
                minExpiry = minExpiry.HasValue
                    ? Math.Min(minExpiry.Value, expiry.Value)
                    : expiry.Value;
            }
        }

        return minExpiry;
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
            return null; // Can't decompress -- skip TTL parsing
        }

        var recordCount = CompressionCodec.GetRecordCount(recordBatch);
        var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.BaseTimestampOffset, 8));

        long? minExpiry = null;
        var offset = 0;

        for (var i = 0; i < recordCount && offset < decompressed.Length; i++)
        {
            var expiry = ParseRecordForExpiry(decompressed, ref offset, baseTimestamp);
            if (expiry.HasValue)
            {
                minExpiry = minExpiry.HasValue
                    ? Math.Min(minExpiry.Value, expiry.Value)
                    : expiry.Value;
            }
        }

        return minExpiry;
    }

    /// <summary>
    /// Parse a single record within the records section, looking for TTL headers.
    /// Advances offset past this record.
    /// Returns the calculated expiry timestamp (baseTimestamp + ttlMs) if a TTL header is found.
    /// </summary>
    private static long? ParseRecordForExpiry(ReadOnlySpan<byte> data, ref int offset, long baseTimestamp)
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
        long? expiry = null;

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

            // Check for TTL header
            if (headerKey.SequenceEqual(TtlKeyBytes) && headerValue.Length > 0)
            {
                if (TryParseTimestamp(headerValue, out var ttlMs))
                {
                    expiry = baseTimestamp + ttlMs;
                }
            }
        }

        offset = recordEnd;
        return expiry;
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
