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
    public static IEndpointRouteBuilder MapMessageBrowser(this IEndpointRouteBuilder app, LogManager logManager, RecordBatchSerializer serializer)
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

        group.MapPost("/{topic}", (string topic, ProduceMessageRequest request, CancellationToken ct) =>
            ProduceMessage(logManager, serializer, topic, request, ct))
            .WithName("ProduceMessage")
            .WithSummary("Produce a single message to a topic")
            .Produces<ProduceResultResponse>()
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        group.MapPost("/{topic}/batch", (string topic, List<ProduceMessageRequest> requests, CancellationToken ct) =>
            ProduceBatch(logManager, serializer, topic, requests, ct))
            .WithName("ProduceMessageBatch")
            .WithSummary("Produce a batch of messages to a topic")
            .Produces<List<ProduceResultResponse>>()
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        group.MapGet("/{topic}/{partition}/offset-for-timestamp", (string topic, int partition, long timestamp) =>
            GetOffsetForTimestamp(logManager, topic, partition, timestamp))
            .WithName("GetOffsetForTimestamp")
            .WithSummary("Find the first offset at or after a unix-ms timestamp")
            .Produces<OffsetForTimestampResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }

    private static async Task<IResult> ProduceMessage(
        LogManager logManager,
        RecordBatchSerializer serializer,
        string topic,
        ProduceMessageRequest request,
        CancellationToken ct)
    {
        try
        {
            var result = await ProduceSingleAsync(logManager, serializer, topic, request, ct);
            return Results.Ok(result);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to produce message: {ex.Message}");
        }
    }

    private static async Task<IResult> ProduceBatch(
        LogManager logManager,
        RecordBatchSerializer serializer,
        string topic,
        List<ProduceMessageRequest> requests,
        CancellationToken ct)
    {
        try
        {
            var results = new List<ProduceResultResponse>(requests.Count);
            foreach (var request in requests)
            {
                results.Add(await ProduceSingleAsync(logManager, serializer, topic, request, ct));
            }
            return Results.Ok(results);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to produce batch: {ex.Message}");
        }
    }

    private static async Task<ProduceResultResponse> ProduceSingleAsync(
        LogManager logManager,
        RecordBatchSerializer serializer,
        string topic,
        ProduceMessageRequest request,
        CancellationToken ct)
    {
        var metadata = logManager.GetTopicMetadata(topic);
        var partitionCount = metadata?.PartitionCount ?? 1;
        var partition = request.Partition is int explicitPartition && explicitPartition >= 0
            ? explicitPartition
            : PickPartition(request.Key, partitionCount);

        var log = logManager.GetOrCreateLog(new TopicPartition { Topic = topic, Partition = partition });
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var message = new Message
        {
            Offset = 0,
            Timestamp = timestamp,
            Key = request.Key is null ? ReadOnlyMemory<byte>.Empty : Encoding.UTF8.GetBytes(request.Key),
            Value = request.Value is null ? ReadOnlyMemory<byte>.Empty : Encoding.UTF8.GetBytes(request.Value),
            Headers = SerializeHeaders(request.Headers)
        };

        var recordBatch = serializer.SerializeMessages([message]);
        var baseOffset = await log.AppendBatchAsync(recordBatch, ct);

        return new ProduceResultResponse(topic, partition, baseOffset, DateTimeOffset.FromUnixTimeMilliseconds(timestamp));
    }

    private static IResult GetOffsetForTimestamp(LogManager logManager, string topic, int partition, long timestamp)
    {
        var log = logManager.GetLog(new TopicPartition { Topic = topic, Partition = partition });
        if (log is null)
        {
            return Results.NotFound(new { message = $"Partition {topic}-{partition} not found" });
        }

        // Timestamp nach der letzten Nachricht → NextOffset, damit die UI ans Log-Ende springt.
        var offset = log.FindOffsetByTimestamp(timestamp) ?? log.NextOffset;
        return Results.Ok(new OffsetForTimestampResponse(topic, partition, timestamp, offset));
    }

    /// <summary>
    /// Deterministic keyed partitioning (FNV-1a over the UTF-8 key) so the same
    /// key always lands on the same partition across broker restarts —
    /// string.GetHashCode is randomized per process and unsuitable here.
    /// Null keys spread randomly like Kafka's round-robin default.
    /// </summary>
    private static int PickPartition(string? key, int partitionCount)
    {
        if (partitionCount <= 1)
            return 0;

        if (string.IsNullOrEmpty(key))
            return Random.Shared.Next(partitionCount);

        var hash = 2166136261u;
        foreach (var b in Encoding.UTF8.GetBytes(key))
        {
            hash ^= b;
            hash *= 16777619;
        }
        return (int)(hash % (uint)partitionCount);
    }

    /// <summary>
    /// Serializes string headers into the native-wire header block format
    /// ([count:int32][keyLen:int32][key][valueLen:int32][value]..., BIG-endian)
    /// that <see cref="RecordBatchSerializer"/> expects on <see cref="Message.Headers"/>
    /// (see WriteHeadersFromNativeBlock).
    /// </summary>
    private static byte[] SerializeHeaders(Dictionary<string, string>? headers)
    {
        if (headers == null || headers.Count == 0)
            return [];

        var size = 4;
        foreach (var (key, value) in headers)
        {
            size += 8 + Encoding.UTF8.GetByteCount(key) + Encoding.UTF8.GetByteCount(value);
        }

        var block = new byte[size];
        var pos = 0;
        BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), headers.Count);
        pos += 4;

        foreach (var (key, value) in headers)
        {
            var keyLen = Encoding.UTF8.GetBytes(key, block.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), keyLen);
            pos += 4 + keyLen;

            var valueLen = Encoding.UTF8.GetBytes(value, block.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), valueLen);
            pos += 4 + valueLen;
        }

        return block;
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

/// <summary>
/// Request to produce a message. Partition null or -1 selects a partition
/// automatically (keyed hash, otherwise random).
/// </summary>
public sealed record ProduceMessageRequest(
    string? Key,
    string? Value,
    Dictionary<string, string>? Headers,
    int? Partition);

/// <summary>
/// Result of producing a single message.
/// </summary>
public sealed record ProduceResultResponse(
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset Timestamp);

/// <summary>
/// Result of an offset-for-timestamp lookup.
/// </summary>
public sealed record OffsetForTimestampResponse(
    string Topic,
    int Partition,
    long Timestamp,
    long Offset);
