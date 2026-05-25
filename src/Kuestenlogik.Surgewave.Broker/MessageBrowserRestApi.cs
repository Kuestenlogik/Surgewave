using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// REST API endpoints for browsing messages in topics/partitions.
/// </summary>
public static class MessageBrowserRestApi
{
    public static IEndpointRouteBuilder MapMessageBrowser(this IEndpointRouteBuilder app, LogManager logManager)
    {
        var group = app.MapGroup("/admin/messages")
            .WithTags("Message Browser");

        group.MapGet("/{topic}/{partition}", (string topic, int partition, long? offset, int? limit) =>
            GetMessages(logManager, topic, partition, offset ?? 0, limit ?? 20))
            .WithName("GetMessages")
            .WithSummary("Get messages from a topic partition")
            .Produces<MessagesResponse>();

        group.MapGet("/{topic}/{partition}/{offset}", (string topic, int partition, long offset) =>
            GetSingleMessage(logManager, topic, partition, offset))
            .WithName("GetSingleMessage")
            .WithSummary("Get a single message by offset")
            .Produces<MessageResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/{topic}/{partition}/download/{offset}", (string topic, int partition, long offset) =>
            DownloadMessage(logManager, topic, partition, offset))
            .WithName("DownloadMessage")
            .WithSummary("Download a message value as binary")
            .Produces(StatusCodes.Status200OK, contentType: "application/octet-stream")
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static async Task<IResult> GetMessages(
        LogManager logManager,
        string topic,
        int partition,
        long offset,
        int limit)
    {
        var tp = new TopicPartition { Topic = topic, Partition = partition };

        try
        {
            var batches = await logManager.ReadBatchesAsync(tp, offset, maxBytes: 1024 * 1024);
            var messages = new List<MessageResponse>();

            foreach (var batchBytes in batches)
            {
                var records = ParseRecordBatch(batchBytes, topic, partition);
                messages.AddRange(records);

                if (messages.Count >= limit)
                    break;
            }

            // Get partition info from the log
            var log = logManager.GetLog(tp);
            var highWatermark = log?.HighWatermark ?? 0;
            var logStartOffset = log?.LogStartOffset ?? 0;

            return Results.Ok(new MessagesResponse(
                Topic: topic,
                Partition: partition,
                Offset: offset,
                HighWatermark: highWatermark,
                LogStartOffset: logStartOffset,
                Messages: messages.Take(limit).ToList()));
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to read messages: {ex.Message}");
        }
    }

    private static async Task<IResult> GetSingleMessage(
        LogManager logManager,
        string topic,
        int partition,
        long offset)
    {
        var tp = new TopicPartition { Topic = topic, Partition = partition };

        try
        {
            var batches = await logManager.ReadBatchesAsync(tp, offset, maxBytes: 64 * 1024);

            foreach (var batchBytes in batches)
            {
                var records = ParseRecordBatch(batchBytes, topic, partition);
                var message = records.FirstOrDefault(r => r.Offset == offset);
                if (message != null)
                {
                    return Results.Ok(message);
                }
            }

            return Results.NotFound(new { message = $"Message at offset {offset} not found" });
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to read message: {ex.Message}");
        }
    }

    private static async Task<IResult> DownloadMessage(
        LogManager logManager,
        string topic,
        int partition,
        long offset)
    {
        var tp = new TopicPartition { Topic = topic, Partition = partition };

        try
        {
            var batches = await logManager.ReadBatchesAsync(tp, offset, maxBytes: 64 * 1024);

            foreach (var batchBytes in batches)
            {
                var records = ParseRecordBatch(batchBytes, topic, partition);
                var message = records.FirstOrDefault(r => r.Offset == offset);
                if (message != null)
                {
                    var valueBytes = message.ValueBase64 != null
                        ? Convert.FromBase64String(message.ValueBase64)
                        : Encoding.UTF8.GetBytes(message.Value ?? "");

                    return Results.File(
                        valueBytes,
                        contentType: "application/octet-stream",
                        fileDownloadName: $"{topic}-{partition}-{offset}.bin");
                }
            }

            return Results.NotFound(new { message = $"Message at offset {offset} not found" });
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to read message: {ex.Message}");
        }
    }

    private static List<MessageResponse> ParseRecordBatch(byte[] batchBytes, string topic, int partition)
    {
        var messages = new List<MessageResponse>();

        try
        {
            // Parse Kafka RecordBatch format
            var span = batchBytes.AsSpan();
            if (span.Length < 61) // Minimum RecordBatch size
                return messages;

            // RecordBatch header
            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(span);
            // var batchLength = BinaryPrimitives.ReadInt32BigEndian(span.Slice(8));
            // var partitionLeaderEpoch = BinaryPrimitives.ReadInt32BigEndian(span.Slice(12));
            // var magic = span[16];
            // var crc = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(17));
            var attributes = BinaryPrimitives.ReadInt16BigEndian(span.Slice(21));
            // var lastOffsetDelta = BinaryPrimitives.ReadInt32BigEndian(span.Slice(23));
            var firstTimestamp = BinaryPrimitives.ReadInt64BigEndian(span.Slice(27));
            // var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(span.Slice(35));
            // var producerId = BinaryPrimitives.ReadInt64BigEndian(span.Slice(43));
            // var producerEpoch = BinaryPrimitives.ReadInt16BigEndian(span.Slice(51));
            // var baseSequence = BinaryPrimitives.ReadInt32BigEndian(span.Slice(53));
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(span.Slice(57));

            var compression = (attributes & 0x07);
            if (compression != 0)
            {
                // Compressed batch - return placeholder
                messages.Add(new MessageResponse(
                    Offset: baseOffset,
                    Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp),
                    Key: null,
                    Value: $"[Compressed batch with {recordCount} records]",
                    ValueBase64: null,
                    Headers: new Dictionary<string, string>(),
                    IsCompressed: true,
                    ValueSizeBytes: batchBytes.Length));
                return messages;
            }

            // Parse uncompressed records
            var pos = 61;
            for (var i = 0; i < recordCount && pos < span.Length; i++)
            {
                try
                {
                    var recordStart = pos;

                    // Record length (varint)
                    var recordLength = ReadVarInt(span, ref pos);
                    if (recordLength <= 0 || pos + recordLength > span.Length)
                        break;

                    // Attributes (varint, typically 0)
                    ReadVarInt(span, ref pos);

                    // Timestamp delta (varint)
                    var timestampDelta = ReadVarLong(span, ref pos);

                    // Offset delta (varint)
                    var offsetDelta = ReadVarInt(span, ref pos);

                    // Key length (varint, -1 = null)
                    var keyLength = ReadVarInt(span, ref pos);
                    string? key = null;
                    if (keyLength > 0 && pos + keyLength <= span.Length)
                    {
                        key = Encoding.UTF8.GetString(span.Slice(pos, keyLength));
                        pos += keyLength;
                    }
                    else if (keyLength > 0)
                    {
                        break;
                    }

                    // Value length (varint, -1 = null)
                    var valueLength = ReadVarInt(span, ref pos);
                    string? value = null;
                    string? valueBase64 = null;
                    if (valueLength > 0 && pos + valueLength <= span.Length)
                    {
                        var valueBytes = span.Slice(pos, valueLength).ToArray();
                        // Try to decode as UTF-8, fallback to Base64
                        if (IsValidUtf8(valueBytes))
                        {
                            value = Encoding.UTF8.GetString(valueBytes);
                        }
                        else
                        {
                            valueBase64 = Convert.ToBase64String(valueBytes);
                            value = $"[Binary data: {valueLength} bytes]";
                        }
                        pos += valueLength;
                    }
                    else if (valueLength > 0)
                    {
                        break;
                    }

                    // Headers count (varint)
                    var headerCount = ReadVarInt(span, ref pos);
                    var headers = new Dictionary<string, string>();
                    for (var h = 0; h < headerCount && pos < span.Length; h++)
                    {
                        var headerKeyLen = ReadVarInt(span, ref pos);
                        if (headerKeyLen > 0 && pos + headerKeyLen <= span.Length)
                        {
                            var headerKey = Encoding.UTF8.GetString(span.Slice(pos, headerKeyLen));
                            pos += headerKeyLen;

                            var headerValueLen = ReadVarInt(span, ref pos);
                            if (headerValueLen > 0 && pos + headerValueLen <= span.Length)
                            {
                                var headerValue = Encoding.UTF8.GetString(span.Slice(pos, headerValueLen));
                                pos += headerValueLen;
                                headers[headerKey] = headerValue;
                            }
                        }
                    }

                    messages.Add(new MessageResponse(
                        Offset: baseOffset + offsetDelta,
                        Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp + timestampDelta),
                        Key: key,
                        Value: value,
                        ValueBase64: valueBase64,
                        Headers: headers,
                        IsCompressed: false,
                        ValueSizeBytes: valueLength > 0 ? valueLength : 0));
                }
                catch
                {
                    break;
                }
            }
        }
        catch
        {
            // Failed to parse batch
        }

        return messages;
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
                // ZigZag decode
                return (result >> 1) ^ -(result & 1);
            }
            shift += 7;
            if (shift > 28) break;
        }
        return result;
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
                // ZigZag decode
                return (result >> 1) ^ -(result & 1);
            }
            shift += 7;
            if (shift > 63) break;
        }
        return result;
    }

    private static bool IsValidUtf8(byte[] bytes)
    {
        try
        {
            // Check for common binary patterns
            foreach (var b in bytes)
            {
                if (b == 0) return false; // Null byte
            }
            Encoding.UTF8.GetString(bytes);
            return true;
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>
/// Response containing messages from a partition.
/// </summary>
public sealed record MessagesResponse(
    string Topic,
    int Partition,
    long Offset,
    long HighWatermark,
    long LogStartOffset,
    IReadOnlyList<MessageResponse> Messages);

/// <summary>
/// Response representing a single message.
/// </summary>
public sealed record MessageResponse(
    long Offset,
    DateTimeOffset Timestamp,
    string? Key,
    string? Value,
    string? ValueBase64,
    IReadOnlyDictionary<string, string> Headers,
    bool IsCompressed,
    int ValueSizeBytes);
