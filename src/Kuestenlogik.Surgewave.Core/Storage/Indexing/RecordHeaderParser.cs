using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Core.Storage.Indexing;

/// <summary>
/// Parser for extracting headers from Kafka RecordBatch format.
/// Provides efficient iteration over records and their headers without full deserialization.
/// </summary>
public static class RecordHeaderParser
{
    /// <summary>
    /// Minimum size of a RecordBatch header (61 bytes).
    /// </summary>
    public const int MinBatchHeaderSize = 61;

    /// <summary>
    /// Parse batch header information from a RecordBatch.
    /// </summary>
    public static BatchHeader ParseBatchHeader(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < MinBatchHeaderSize)
            throw new ArgumentException($"RecordBatch too small: {recordBatch.Length} bytes, need at least {MinBatchHeaderSize}");

        return new BatchHeader
        {
            BaseOffset = BinaryPrimitives.ReadInt64BigEndian(recordBatch),
            BatchLength = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(8)),
            PartitionLeaderEpoch = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(12)),
            Magic = recordBatch[16],
            Crc = BinaryPrimitives.ReadUInt32BigEndian(recordBatch.Slice(17)),
            Attributes = BinaryPrimitives.ReadInt16BigEndian(recordBatch.Slice(21)),
            LastOffsetDelta = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(23)),
            BaseTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(27)),
            MaxTimestamp = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(35)),
            ProducerId = BinaryPrimitives.ReadInt64BigEndian(recordBatch.Slice(43)),
            ProducerEpoch = BinaryPrimitives.ReadInt16BigEndian(recordBatch.Slice(51)),
            BaseSequence = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(53)),
            RecordCount = BinaryPrimitives.ReadInt32BigEndian(recordBatch.Slice(57))
        };
    }

    /// <summary>
    /// Enumerate all records in a RecordBatch, parsing headers on demand.
    /// </summary>
    public static RecordEnumerator EnumerateRecords(ReadOnlySpan<byte> recordBatch)
    {
        return new RecordEnumerator(recordBatch);
    }

    /// <summary>
    /// Get all headers from a specific record within a batch.
    /// </summary>
    /// <param name="recordBatch">The full RecordBatch bytes</param>
    /// <param name="recordIndex">Zero-based index of the record within the batch</param>
    /// <returns>List of headers, or empty if record not found</returns>
    public static List<RecordHeader> GetHeaders(ReadOnlySpan<byte> recordBatch, int recordIndex)
    {
        var headers = new List<RecordHeader>();
        var enumerator = EnumerateRecords(recordBatch);
        var currentIndex = 0;

        while (enumerator.MoveNext())
        {
            if (currentIndex == recordIndex)
            {
                foreach (var header in enumerator.Current.Headers)
                {
                    headers.Add(header);
                }
                break;
            }
            currentIndex++;
        }

        return headers;
    }

    /// <summary>
    /// Find records that contain a specific header key.
    /// </summary>
    /// <param name="recordBatch">The full RecordBatch bytes</param>
    /// <param name="headerKey">The header key to search for (UTF-8 encoded)</param>
    /// <returns>Records containing the specified header</returns>
    public static List<RecordWithHeader> FindRecordsWithHeader(ReadOnlySpan<byte> recordBatch, string headerKey)
    {
        var keyBytes = Encoding.UTF8.GetBytes(headerKey);
        return FindRecordsWithHeader(recordBatch, keyBytes);
    }

    /// <summary>
    /// Find records that contain a specific header key.
    /// </summary>
    /// <param name="recordBatch">The full RecordBatch bytes</param>
    /// <param name="headerKey">The header key bytes to search for</param>
    /// <returns>Records containing the specified header</returns>
    public static List<RecordWithHeader> FindRecordsWithHeader(ReadOnlySpan<byte> recordBatch, ReadOnlySpan<byte> headerKey)
    {
        var results = new List<RecordWithHeader>();
        var enumerator = EnumerateRecords(recordBatch);

        while (enumerator.MoveNext())
        {
            var record = enumerator.Current;
            foreach (var header in record.Headers)
            {
                if (SimdSpanComparer.SequenceEqual(header.Key.Span, headerKey))
                {
                    results.Add(new RecordWithHeader(record.Offset, record.Timestamp, header));
                    break; // Only add once per record even if header appears multiple times
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Extract a specific header value from all records in a batch.
    /// Useful for building custom indexes.
    /// </summary>
    /// <param name="recordBatch">The full RecordBatch bytes</param>
    /// <param name="headerKey">The header key to extract</param>
    /// <returns>Tuples of (offset, headerValue) for records containing the header</returns>
    public static List<(long Offset, long Timestamp, ReadOnlyMemory<byte> Value)> ExtractHeaderValues(
        ReadOnlySpan<byte> recordBatch, string headerKey)
    {
        var keyBytes = Encoding.UTF8.GetBytes(headerKey);
        return ExtractHeaderValues(recordBatch, keyBytes);
    }

    /// <summary>
    /// Extract a specific header value from all records in a batch.
    /// </summary>
    public static List<(long Offset, long Timestamp, ReadOnlyMemory<byte> Value)> ExtractHeaderValues(
        ReadOnlySpan<byte> recordBatch, ReadOnlySpan<byte> headerKey)
    {
        var results = new List<(long, long, ReadOnlyMemory<byte>)>();
        var enumerator = EnumerateRecords(recordBatch);

        while (enumerator.MoveNext())
        {
            var record = enumerator.Current;
            foreach (var header in record.Headers)
            {
                if (SimdSpanComparer.SequenceEqual(header.Key.Span, headerKey))
                {
                    results.Add((record.Offset, record.Timestamp, header.Value));
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Read a zigzag-encoded varint from a span.
    /// </summary>
    internal static (long Value, int BytesRead) ReadVarint(ReadOnlySpan<byte> data)
    {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;

        while (bytesRead < data.Length)
        {
            byte b = data[bytesRead];
            bytesRead++;

            result |= (long)(b & 0x7F) << shift;

            if ((b & 0x80) == 0)
            {
                // Zigzag decode
                return ((result >> 1) ^ -(result & 1), bytesRead);
            }

            shift += 7;

            if (shift > 63)
                throw new InvalidDataException("Varint too long");
        }

        throw new InvalidDataException("Incomplete varint");
    }
}

/// <summary>
/// Parsed RecordBatch header.
/// </summary>
public readonly record struct BatchHeader
{
    public long BaseOffset { get; init; }
    public int BatchLength { get; init; }
    public int PartitionLeaderEpoch { get; init; }
    public byte Magic { get; init; }
    public uint Crc { get; init; }
    public short Attributes { get; init; }
    public int LastOffsetDelta { get; init; }
    public long BaseTimestamp { get; init; }
    public long MaxTimestamp { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public int BaseSequence { get; init; }
    public int RecordCount { get; init; }
}

/// <summary>
/// A single header from a Kafka record.
/// </summary>
public readonly record struct RecordHeader(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value)
{
    /// <summary>
    /// Get the header key as a UTF-8 string.
    /// </summary>
    public string KeyString => Encoding.UTF8.GetString(Key.Span);

    /// <summary>
    /// Get the header value as a UTF-8 string.
    /// </summary>
    public string? ValueString => Value.IsEmpty ? null : Encoding.UTF8.GetString(Value.Span);

    /// <summary>
    /// Get the header value as a 64-bit integer (big-endian).
    /// </summary>
    public long? ValueAsInt64 => Value.Length >= 8
        ? BinaryPrimitives.ReadInt64BigEndian(Value.Span)
        : null;

    /// <summary>
    /// Get the header value as a 32-bit integer (big-endian).
    /// </summary>
    public int? ValueAsInt32 => Value.Length >= 4
        ? BinaryPrimitives.ReadInt32BigEndian(Value.Span)
        : null;
}

/// <summary>
/// A parsed record with its headers available.
/// </summary>
public readonly ref struct ParsedRecord
{
    private readonly ReadOnlySpan<byte> _batchData;
    private readonly int _headersStart;
    private readonly int _headersCount;

    public long Offset { get; }
    public long Timestamp { get; }
    public ReadOnlySpan<byte> Key { get; }
    public ReadOnlySpan<byte> Value { get; }

    internal ParsedRecord(
        long offset,
        long timestamp,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        ReadOnlySpan<byte> batchData,
        int headersStart,
        int headersCount)
    {
        Offset = offset;
        Timestamp = timestamp;
        Key = key;
        Value = value;
        _batchData = batchData;
        _headersStart = headersStart;
        _headersCount = headersCount;
    }

    /// <summary>
    /// Enumerate headers for this record.
    /// </summary>
    public HeaderEnumerator Headers => new(_batchData.Slice(_headersStart), _headersCount);
}

/// <summary>
/// Enumerator for iterating over records in a RecordBatch.
/// </summary>
public ref struct RecordEnumerator
{
    private readonly ReadOnlySpan<byte> _batchData;
    private readonly long _baseOffset;
    private readonly long _baseTimestamp;
    private readonly int _recordCount;
    private int _currentRecord;
    private int _position;
    private ParsedRecord _current;

    internal RecordEnumerator(ReadOnlySpan<byte> batchData)
    {
        if (batchData.Length < RecordHeaderParser.MinBatchHeaderSize)
        {
            _batchData = default;
            _baseOffset = 0;
            _baseTimestamp = 0;
            _recordCount = 0;
            _currentRecord = 0;
            _position = 0;
            _current = default;
            return;
        }

        _batchData = batchData;
        _baseOffset = BinaryPrimitives.ReadInt64BigEndian(batchData);
        _baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(batchData.Slice(27));
        _recordCount = BinaryPrimitives.ReadInt32BigEndian(batchData.Slice(57));
        _currentRecord = -1;
        _position = 61; // Records start after the 61-byte header
        _current = default;
    }

    public ParsedRecord Current => _current;

    public bool MoveNext()
    {
        _currentRecord++;
        if (_currentRecord >= _recordCount || _position >= _batchData.Length)
            return false;

        try
        {
            // Read record length
            var (recordLength, lenBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += lenBytes;

            if (_position + recordLength > _batchData.Length)
                return false;

            var recordStart = _position;

            // Skip attributes (varint)
            var (_, attrBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += attrBytes;

            // Read timestamp delta
            var (timestampDelta, tsBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += tsBytes;

            // Read offset delta
            var (offsetDelta, odBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += odBytes;

            // Read key
            var (keyLength, klBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += klBytes;

            ReadOnlySpan<byte> key = default;
            if (keyLength >= 0)
            {
                key = _batchData.Slice(_position, (int)keyLength);
                _position += (int)keyLength;
            }

            // Read value
            var (valueLength, vlBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += vlBytes;

            ReadOnlySpan<byte> value = default;
            if (valueLength >= 0)
            {
                value = _batchData.Slice(_position, (int)valueLength);
                _position += (int)valueLength;
            }

            // Read headers count
            var headersStart = _position;
            var (headersCount, hcBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
            _position += hcBytes;

            // Skip headers to advance position for next record
            for (int h = 0; h < headersCount; h++)
            {
                var (headerKeyLen, hklBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
                _position += hklBytes + (int)headerKeyLen;

                var (headerValueLen, hvlBytes) = RecordHeaderParser.ReadVarint(_batchData.Slice(_position));
                _position += hvlBytes;
                if (headerValueLen >= 0)
                {
                    _position += (int)headerValueLen;
                }
            }

            _current = new ParsedRecord(
                _baseOffset + offsetDelta,
                _baseTimestamp + timestampDelta,
                key,
                value,
                _batchData,
                headersStart,
                (int)headersCount);

            return true;
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>
/// Enumerator for iterating over headers in a record.
/// </summary>
public ref struct HeaderEnumerator
{
    private readonly ReadOnlySpan<byte> _data;
    private readonly int _count;
    private int _currentHeader;
    private int _position;
    private RecordHeader _current;

    internal HeaderEnumerator(ReadOnlySpan<byte> data, int count)
    {
        _data = data;
        _count = count;
        _currentHeader = -1;
        _position = 0;
        _current = default;

        // Skip the header count varint (we already have the count)
        if (data.Length > 0)
        {
            var (_, hcBytes) = RecordHeaderParser.ReadVarint(data);
            _position = hcBytes;
        }
    }

    public RecordHeader Current => _current;

    public bool MoveNext()
    {
        _currentHeader++;
        if (_currentHeader >= _count || _position >= _data.Length)
            return false;

        try
        {
            // Read header key
            var (keyLen, klBytes) = RecordHeaderParser.ReadVarint(_data.Slice(_position));
            _position += klBytes;

            var key = _data.Slice(_position, (int)keyLen).ToArray();
            _position += (int)keyLen;

            // Read header value
            var (valueLen, vlBytes) = RecordHeaderParser.ReadVarint(_data.Slice(_position));
            _position += vlBytes;

            byte[] value = [];
            if (valueLen >= 0)
            {
                value = _data.Slice(_position, (int)valueLen).ToArray();
                _position += (int)valueLen;
            }

            _current = new RecordHeader(key, value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public HeaderEnumerator GetEnumerator() => this;
}

/// <summary>
/// A record with a matched header value.
/// </summary>
public readonly record struct RecordWithHeader(long Offset, long Timestamp, RecordHeader Header);
