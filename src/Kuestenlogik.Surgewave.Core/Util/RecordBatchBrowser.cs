using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Read-side decoder for Kafka v2 record batches used by browse/inspection surfaces
/// (message browser REST API, GraphQL, SQL scans, topic sampling). Transparently
/// decompresses GZIP/Snappy/LZ4/ZSTD batches via <see cref="CompressionCodec"/>.
/// </summary>
/// <remarks>
/// Not for the hot fetch path — that stays on the Broker's span-based
/// <c>RecordBatchSerializer</c>. This decoder favours robustness over speed:
/// malformed records terminate the batch instead of throwing.
/// </remarks>
public static class RecordBatchBrowser
{
    /// <summary>Size of the fixed v2 batch header (base offset .. record count).</summary>
    private const int HeaderSize = 61;

    /// <summary>
    /// Decodes all records of a single v2 record batch, decompressing when needed.
    /// </summary>
    /// <param name="batch">One record batch, starting at the base offset field.</param>
    public static RecordBatchBrowseResult Parse(ReadOnlySpan<byte> batch)
    {
        if (batch.Length < HeaderSize)
        {
            return RecordBatchBrowseResult.Empty;
        }

        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(batch);
        var batchLength = BinaryPrimitives.ReadInt32BigEndian(batch.Slice(8));
        var attributes = BinaryPrimitives.ReadInt16BigEndian(batch.Slice(21));
        var firstTimestamp = BinaryPrimitives.ReadInt64BigEndian(batch.Slice(27));
        var recordCount = BinaryPrimitives.ReadInt32BigEndian(batch.Slice(57));
        var compressionType = attributes & 0x07;

        // batchLength counts from the partition leader epoch (offset 12); clamp to the
        // buffer so a truncated read or trailing bytes cannot push us out of bounds.
        var declaredEnd = 12L + batchLength;
        var end = batchLength > 0 && declaredEnd < batch.Length ? (int)declaredEnd : batch.Length;
        var recordsSection = batch[HeaderSize..end];

        if (compressionType == KafkaConstants.Compression.None)
        {
            return new RecordBatchBrowseResult
            {
                Records = ParseRecords(recordsSection, baseOffset, firstTimestamp, recordCount),
                IsCompressed = false,
                CompressionType = compressionType,
                HeaderRecordCount = recordCount,
                DecompressionFailed = false,
                BaseOffset = baseOffset,
                FirstTimestampMs = firstTimestamp,
            };
        }

        byte[]? pooled = null;
        try
        {
            byte[] buffer;
            int length;
            if (CompressionCodec.IsSupported(compressionType))
            {
                bool isPooled;
                (buffer, length, isPooled) = CompressionCodec.DecompressPooled(recordsSection, compressionType);
                pooled = isPooled ? buffer : null;
            }
            else
            {
                buffer = [];
                length = -1;
            }

            if (length < 0)
            {
                return Undecodable(compressionType, recordCount, baseOffset, firstTimestamp);
            }

            return new RecordBatchBrowseResult
            {
                Records = ParseRecords(buffer.AsSpan(0, length), baseOffset, firstTimestamp, recordCount),
                IsCompressed = true,
                CompressionType = compressionType,
                HeaderRecordCount = recordCount,
                DecompressionFailed = false,
                BaseOffset = baseOffset,
                FirstTimestampMs = firstTimestamp,
            };
        }
        catch (Exception)
        {
            // Corrupt payload or codec failure — report instead of throwing so
            // browse surfaces can render an honest placeholder.
            return Undecodable(compressionType, recordCount, baseOffset, firstTimestamp);
        }
        finally
        {
            if (pooled is not null)
            {
                ArrayPool<byte>.Shared.Return(pooled);
            }
        }
    }

    private static RecordBatchBrowseResult Undecodable(
        int compressionType, int recordCount, long baseOffset, long firstTimestamp) => new()
    {
        Records = [],
        IsCompressed = true,
        CompressionType = compressionType,
        HeaderRecordCount = recordCount,
        DecompressionFailed = true,
        BaseOffset = baseOffset,
        FirstTimestampMs = firstTimestamp,
    };

    private static List<BrowsedRecord> ParseRecords(
        ReadOnlySpan<byte> records, long baseOffset, long firstTimestamp, int recordCount)
    {
        var result = new List<BrowsedRecord>(Math.Clamp(recordCount, 0, 1024));
        var pos = 0;

        for (var i = 0; i < recordCount && pos < records.Length; i++)
        {
            var recordLength = ReadVarInt(records, ref pos);
            if (recordLength <= 0 || pos + recordLength > records.Length)
            {
                break;
            }

            var bodyStart = pos;

            // Record attributes are a plain int8 per the wire format (always 0 today).
            pos += 1;

            var timestampDelta = ReadVarLong(records, ref pos);
            var offsetDelta = ReadVarInt(records, ref pos);

            var recordEnd = bodyStart + recordLength;
            if (!TryReadSizedBytes(records, ref pos, recordEnd, out var key) ||
                !TryReadSizedBytes(records, ref pos, recordEnd, out var value))
            {
                break;
            }

            var headers = new Dictionary<string, string>();
            var headerCount = ReadVarInt(records, ref pos);
            for (var h = 0; h < headerCount && pos < records.Length; h++)
            {
                if (!TryReadSizedBytes(records, ref pos, recordEnd, out var headerKey) ||
                    headerKey is null)
                {
                    break; // Header keys cannot be null — malformed record.
                }

                TryReadSizedBytes(records, ref pos, recordEnd, out var headerValue);
                headers[Encoding.UTF8.GetString(headerKey)] =
                    headerValue is null ? "" : Encoding.UTF8.GetString(headerValue);
            }

            result.Add(new BrowsedRecord(
                Offset: baseOffset + offsetDelta,
                TimestampMs: firstTimestamp + timestampDelta,
                Key: key,
                Value: value,
                Headers: headers));

            // Jump to the next record via the declared length — keeps us aligned even
            // if an individual record carries fields this decoder does not model.
            pos = bodyStart + recordLength;
        }

        return result;
    }

    /// <summary>
    /// Reads a varint-length-prefixed byte block. A length of -1 yields <c>null</c> (legitimate
    /// null key/value); a length overrunning the record or buffer returns <c>false</c> (malformed).
    /// </summary>
    private static bool TryReadSizedBytes(ReadOnlySpan<byte> span, ref int pos, int limit, out byte[]? bytes)
    {
        var length = ReadVarInt(span, ref pos);
        if (length < 0)
        {
            bytes = null;
            return true;
        }

        if (length == 0)
        {
            bytes = [];
            return true;
        }

        if (pos + length > limit || pos + length > span.Length)
        {
            bytes = null;
            return false;
        }

        bytes = span.Slice(pos, length).ToArray();
        pos += length;
        return true;
    }

    private static int ReadVarInt(ReadOnlySpan<byte> span, ref int pos)
    {
        var result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return (result >>> 1) ^ -(result & 1); // ZigZag decode
            }

            shift += 7;
            if (shift > 28)
            {
                break;
            }
        }

        return 0;
    }

    private static long ReadVarLong(ReadOnlySpan<byte> span, ref int pos)
    {
        long result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return (result >>> 1) ^ -(result & 1); // ZigZag decode
            }

            shift += 7;
            if (shift > 63)
            {
                break;
            }
        }

        return 0;
    }
}
