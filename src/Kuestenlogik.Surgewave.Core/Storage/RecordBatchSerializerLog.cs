using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Source-generated trace/debug logging for <see cref="RecordBatchSerializer"/>. Split out of
/// the broker's <c>Log</c> class (#59 b4-tier2) so the record-batch codec can live in Core.
/// </summary>
internal static partial class RecordBatchSerializerLog
{
    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordBatch.Length={Length}")]
    public static partial void ParseRecordBatchLength(ILogger logger, int length);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: baseOffset={BaseOffset}, batchLength={BatchLength}, magic={Magic}")]
    public static partial void ParseRecordBatchHeader(ILogger logger, long baseOffset, int batchLength, byte magic);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordCount={RecordCount}, stream.Position={StreamPosition}")]
    public static partial void ParseRecordBatchRecordCount(ILogger logger, int recordCount, long streamPosition);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: recordsSize={RecordsSize}, remaining bytes={RemainingBytes}")]
    public static partial void ParseRecordBatchRecordsSize(ILogger logger, int recordsSize, int remainingBytes);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: actually read {BytesRead} bytes")]
    public static partial void ParseRecordBatchBytesRead(ILogger logger, int bytesRead);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ParseRecordBatch: Decompressed {CompressionType}: {CompressedSize} -> {DecompressedSize} bytes")]
    public static partial void ParseRecordBatchDecompressed(ILogger logger, string compressionType, int compressedSize, int decompressedSize);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Parsing record {RecordIndex}, position={Position}, remaining={Remaining}")]
    public static partial void ParseRecordBatchParsingRecord(ILogger logger, int recordIndex, int position, int remaining);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} length={RecordLength} (raw={RawLength})")]
    public static partial void ParseRecordBatchRecordLength(ILogger logger, int recordIndex, int recordLength, int rawLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} attributes={Attributes}")]
    public static partial void ParseRecordBatchAttributes(ILogger logger, int recordIndex, sbyte attributes);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} timestampDelta={TimestampDelta} (raw={RawTimestampDelta})")]
    public static partial void ParseRecordBatchTimestampDelta(ILogger logger, int recordIndex, long timestampDelta, long rawTimestampDelta);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} offsetDelta={OffsetDelta} (raw={RawOffsetDelta})")]
    public static partial void ParseRecordBatchOffsetDelta(ILogger logger, int recordIndex, int offsetDelta, int rawOffsetDelta);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} keyLength={KeyLength} (raw={RawKeyLength})")]
    public static partial void ParseRecordBatchKeyLength(ILogger logger, int recordIndex, int keyLength, int rawKeyLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Record {RecordIndex} valueLength={ValueLength} (raw={RawValueLength})")]
    public static partial void ParseRecordBatchValueLength(ILogger logger, int recordIndex, int valueLength, int rawValueLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Message created: Offset={Offset}, Timestamp={Timestamp}, KeyLength={KeyLength}, ValueLength={ValueLength}")]
    public static partial void ParseRecordBatchMessageCreated(ILogger logger, long offset, long timestamp, int keyLength, int valueLength);

    [LoggerMessage(Level = LogLevel.Trace, Message = "ParseRecordBatch: Successfully parsed {MessageCount} messages")]
    public static partial void ParseRecordBatchComplete(ILogger logger, int messageCount);
}