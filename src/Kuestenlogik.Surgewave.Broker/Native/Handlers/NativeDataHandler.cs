using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol data operations: Produce, ProduceBatch, Fetch, ListOffsets, CommitOffset, FetchOffset, Nack.
/// </summary>
public sealed class NativeDataHandler : INativeRequestHandler
{
    private readonly BrokerConfig _config;
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _recordBatchSerializer;
    private readonly NativeGroupCoordinator _groupCoordinator;
    private readonly DlqManager? _dlqManager;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<NativeDataHandler> _logger;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.Produce,
        SurgewaveOpCode.ProduceBatch,
        SurgewaveOpCode.Fetch,
        SurgewaveOpCode.ListOffsets,
        SurgewaveOpCode.CommitOffset,
        SurgewaveOpCode.FetchOffset,
        SurgewaveOpCode.Nack
    ];

    public NativeDataHandler(
        BrokerConfig config,
        LogManager logManager,
        RecordBatchSerializer recordBatchSerializer,
        NativeGroupCoordinator groupCoordinator,
        ILogger<NativeDataHandler> logger,
        TimeProvider? timeProvider = null,
        DlqManager? dlqManager = null)
    {
        _config = config;
        _logManager = logManager;
        _recordBatchSerializer = recordBatchSerializer;
        _groupCoordinator = groupCoordinator;
        _dlqManager = dlqManager;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.Produce => HandleProduceAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ProduceBatch => HandleProduceBatchAsync(context, payload, cancellationToken),
            SurgewaveOpCode.Fetch => HandleFetchAsync(context, payload, cancellationToken),
            SurgewaveOpCode.ListOffsets => HandleListOffsetsAsync(context, payload, cancellationToken),
            SurgewaveOpCode.CommitOffset => HandleCommitOffsetAsync(context, payload, cancellationToken),
            SurgewaveOpCode.FetchOffset => HandleFetchOffsetAsync(context, payload, cancellationToken),
            SurgewaveOpCode.Nack => HandleNackAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandleProduceAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var span = payload.Span;
        var position = 0;

        // Read topic string
        var topicLength = BinaryPrimitives.ReadInt16BigEndian(span[position..]);
        position += 2;

        // Use interned topic name to avoid repeated string allocations (same cache as ParseProduceBatch).
        // The cache verifies bytes on hit — a 32-bit hash collision must not route to the wrong topic (#73).
        var topic = topicLength > 0
            ? _topicNameCache.GetOrAdd(span.Slice(position, topicLength))
            : string.Empty;
        position += topicLength > 0 ? topicLength : 0;

        var partition = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
        position += 4;

        var messageCount = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
        position += 4;

        if (string.IsNullOrEmpty(topic))
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Topic name required", cancellationToken);
            return;
        }

        var topicMetadata = _logManager.GetTopicMetadata(topic);
        if (topicMetadata == null)
        {
            // Auto-create topic if enabled
            if (_config.AutoCreateTopics)
            {
                _logger.LogInformation("Auto-creating topic {Topic} with {Partitions} partitions", topic, _config.DefaultNumPartitions);
                topicMetadata = await _logManager.CreateTopicAsync(
                    topic,
                    _config.DefaultNumPartitions,
                    _config.DefaultReplicationFactor,
                    config: null,
                    cancellationToken);
            }
            else
            {
                await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                    SurgewaveErrorCode.TopicNotFound, $"Topic not found: {topic}", cancellationToken);
                return;
            }
        }

        if (partition < 0 || partition >= topicMetadata.PartitionCount)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.PartitionNotFound, $"Partition not found: {partition}", cancellationToken);
            return;
        }

        var topicPartition = new TopicPartition { Topic = topic, Partition = partition };
        var log = _logManager.GetOrCreateLog(topicPartition);

        // Re-obtain span after potential await (spans can't be held across await boundaries)
        span = payload.Span;

        // Read messages using zero-copy Memory slicing
        // Use pooled list to reduce GC pressure, TimeProvider extension for fast timestamp
        var messages = ListPool<Message>.Rent(messageCount);
        try
        {
            var baseTimestamp = _timeProvider.GetUnixTimeMilliseconds();
            var baseOffset = log.NextOffset;

            for (int i = 0; i < messageCount; i++)
            {
                var keyLength = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
                position += 4;

                ReadOnlyMemory<byte> key = keyLength > 0 ? payload.Slice(position, keyLength) : ReadOnlyMemory<byte>.Empty;
                position += keyLength > 0 ? keyLength : 0;

                var valueLength = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
                position += 4;
                var value = valueLength > 0 ? payload.Slice(position, valueLength) : ReadOnlyMemory<byte>.Empty;
                position += valueLength > 0 ? valueLength : 0;

                // Per-message native header block — slice it verbatim so the
                // RecordBatchSerializer can write Kafka-style record headers
                // without re-decoding. The block layout is documented on
                // NativeMessageHeaderCodec.
                var headersStart = position;
                // Only the block length is needed — Decode would materialize a dictionary with a
                // string key and byte[] value per header, all of it discarded (#83).
                var headerBytes = NativeMessageHeaderCodec.GetBlockLength(span[headersStart..]);
                var headers = headerBytes > 0
                    ? payload.Slice(headersStart, headerBytes)
                    : ReadOnlyMemory<byte>.Empty;
                position += headerBytes;

                messages.Add(new Message
                {
                    Offset = baseOffset + i,
                    Timestamp = baseTimestamp,
                    Key = key,
                    Value = value,
                    Headers = headers
                });
            }

            // Serialize to RecordBatch using pooled buffer (zero-allocation hot path)
            var (recordBatch, recordBatchLength) = _recordBatchSerializer.SerializeMessagesPooled(messages);
            try
            {
                var actualBaseOffset = await _logManager.AppendBatchAsync(topicPartition, recordBatch.AsMemory(0, recordBatchLength), cancellationToken);

                // Response: baseOffset(8) + messageCount(4) - use pooled buffer to avoid allocation
                var responsePayload = ArrayPool<byte>.Shared.Rent(12);
                try
                {
                    BinaryPrimitives.WriteInt64BigEndian(responsePayload.AsSpan(0, 8), actualBaseOffset);
                    BinaryPrimitives.WriteInt32BigEndian(responsePayload.AsSpan(8, 4), messageCount);

                    await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ProduceAck,
                        SurgewaveErrorCode.None, responsePayload.AsMemory(0, 12), cancellationToken);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(responsePayload);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(recordBatch);
            }
        }
        finally
        {
            ListPool<Message>.Return(messages);
        }
    }

    private async Task HandleProduceBatchAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var parsedBatch = ParseProduceBatch(payload);

        try
        {
            using var writer = new BigEndianWriter();
            writer.Write((short)parsedBatch.Count);

            foreach (var topicBatch in parsedBatch)
            {
                writer.WriteString(topicBatch.Topic);
                writer.Write(topicBatch.Partitions.Count);

                var topicMetadata = _logManager.GetTopicMetadata(topicBatch.Topic);

                // Auto-create topic if enabled and not found
                if (topicMetadata == null && _config.AutoCreateTopics)
                {
                    _logger.LogInformation("Auto-creating topic {Topic} with {Partitions} partitions", topicBatch.Topic, _config.DefaultNumPartitions);
                    topicMetadata = await _logManager.CreateTopicAsync(
                        topicBatch.Topic,
                        _config.DefaultNumPartitions,
                        _config.DefaultReplicationFactor,
                        config: null,
                        cancellationToken);
                }

                foreach (var partitionBatch in topicBatch.Partitions)
                {
                    writer.Write(partitionBatch.Partition);

                    if (topicMetadata == null)
                    {
                        writer.Write((short)SurgewaveErrorCode.TopicNotFound);
                        writer.Write(-1L);
                        continue;
                    }

                    if (partitionBatch.Partition < 0 || partitionBatch.Partition >= topicMetadata.PartitionCount)
                    {
                        writer.Write((short)SurgewaveErrorCode.PartitionNotFound);
                        writer.Write(-1L);
                        continue;
                    }

                    var topicPartition = new TopicPartition { Topic = topicBatch.Topic, Partition = partitionBatch.Partition };
                    var log = _logManager.GetOrCreateLog(topicPartition);

                    // Use pooled list to reduce GC pressure
                    var messages = ListPool<Message>.Rent(partitionBatch.Messages.Count);
                    try
                    {
                        var baseTimestamp = _timeProvider.GetUnixTimeMilliseconds();
                        var baseOffset = log.NextOffset;

                        for (int m = 0; m < partitionBatch.Messages.Count; m++)
                        {
                            var msg = partitionBatch.Messages[m];
                            messages.Add(new Message
                            {
                                Offset = baseOffset + m,
                                Timestamp = baseTimestamp,
                                Key = msg.Key,
                                Value = msg.Value,
                                Headers = msg.Headers
                            });
                        }

                        // Use pooled serialization for zero-allocation hot path
                        var (recordBatch, recordBatchLength) = _recordBatchSerializer.SerializeMessagesPooled(messages);
                        try
                        {
                            var actualBaseOffset = await _logManager.AppendBatchAsync(topicPartition, recordBatch.AsMemory(0, recordBatchLength), cancellationToken);
                            writer.Write((short)SurgewaveErrorCode.None);
                            writer.Write(actualBaseOffset);
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(recordBatch);
                        }
                    }
                    finally
                    {
                        ListPool<Message>.Return(messages);
                    }
                }
            }

            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ProduceAck,
                SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
        }
        finally
        {
            // Return pooled lists
            ReturnParsedBatchToPool(parsedBatch);
        }
    }

    /// <summary>
    /// Returns all pooled lists from parsed batch back to pool
    /// </summary>
    private static void ReturnParsedBatchToPool(List<ParsedTopicBatch> parsedBatch)
    {
        foreach (var topicBatch in parsedBatch)
        {
            foreach (var partitionBatch in topicBatch.Partitions)
            {
                ListPool<ParsedMessage>.Return(partitionBatch.Messages);
            }
            ListPool<ParsedPartitionBatch>.Return(topicBatch.Partitions);
        }
        ListPool<ParsedTopicBatch>.Return(parsedBatch);
    }

    private record struct ParsedMessage(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value, ReadOnlyMemory<byte> Headers);
    private record struct ParsedPartitionBatch(int Partition, List<ParsedMessage> Messages);
    private record struct ParsedTopicBatch(string Topic, List<ParsedPartitionBatch> Partitions);

    // String interning cache for topic names to avoid repeated allocations. Byte-verifying: a
    // 32-bit hash collision returns the correct (freshly decoded) name instead of the wrong topic (#73).
    private static readonly Utf8StringInternCache _topicNameCache = new(maxEntries: 10_000, maxByteLength: 256);

    private static List<ParsedTopicBatch> ParseProduceBatch(ReadOnlyMemory<byte> payload)
    {
        var span = payload.Span;
        var position = 0;

        var topicLength = BinaryPrimitives.ReadInt16BigEndian(span[position..]);
        position += 2;

        // Use interned topic name to avoid repeated string allocations (byte-verifying cache, #73)
        var topic = topicLength > 0
            ? _topicNameCache.GetOrAdd(span.Slice(position, topicLength))
            : string.Empty;
        position += topicLength > 0 ? topicLength : 0;

        var partition = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
        position += 4;

        var messageCount = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
        position += 4;

        // Use pooled list to reduce GC pressure
        var messages = ListPool<ParsedMessage>.Rent(messageCount);
        for (int m = 0; m < messageCount; m++)
        {
            var keyLength = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
            position += 4;

            ReadOnlyMemory<byte> key = keyLength > 0 ? payload.Slice(position, keyLength) : ReadOnlyMemory<byte>.Empty;
            position += keyLength > 0 ? keyLength : 0;

            var valueLength = BinaryPrimitives.ReadInt32BigEndian(span[position..]);
            position += 4;
            var value = valueLength > 0 ? payload.Slice(position, valueLength) : ReadOnlyMemory<byte>.Empty;
            position += valueLength > 0 ? valueLength : 0;

            var headersStart = position;
            // Length only — see HandleProduceAsync: Decode's dictionary would be thrown away (#83).
            var headerBytes = NativeMessageHeaderCodec.GetBlockLength(span[headersStart..]);
            var headers = headerBytes > 0
                ? payload.Slice(headersStart, headerBytes)
                : ReadOnlyMemory<byte>.Empty;
            position += headerBytes;

            messages.Add(new ParsedMessage(key, value, headers));
        }

        // Use pooled lists for partitions and topics
        var partitions = ListPool<ParsedPartitionBatch>.Rent(1);
        partitions.Add(new ParsedPartitionBatch(partition, messages));

        var topics = ListPool<ParsedTopicBatch>.Rent(1);
        topics.Add(new ParsedTopicBatch(topic, partitions));
        return topics;
    }

    private async Task HandleFetchAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);

        var topic = reader.ReadString();
        var partition = reader.ReadInt32();
        var offset = reader.ReadInt64();
        var maxBytes = reader.ReadInt32();

        // Read optional maxWaitMs parameter for long-polling (default 5000ms if not provided)
        var maxWaitMs = reader.Remaining >= 4 ? reader.ReadInt32() : 5000;

        if (string.IsNullOrEmpty(topic))
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Topic name required", cancellationToken);
            return;
        }

        // Auto-create topic if enabled (same as Produce behavior)
        var topicMetadata = _logManager.GetTopicMetadata(topic);
        if (topicMetadata == null)
        {
            if (_config.AutoCreateTopics)
            {
                _logger.LogInformation("Auto-creating topic {Topic} on fetch with {Partitions} partitions", topic, _config.DefaultNumPartitions);
                topicMetadata = await _logManager.CreateTopicAsync(
                    topic,
                    _config.DefaultNumPartitions,
                    _config.DefaultReplicationFactor,
                    config: null,
                    cancellationToken);
            }
            else
            {
                await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                    SurgewaveErrorCode.TopicNotFound, $"Topic not found: {topic}", cancellationToken);
                return;
            }
        }

        if (partition < 0 || partition >= topicMetadata.PartitionCount)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.PartitionNotFound, $"Partition not found: {partition}", cancellationToken);
            return;
        }

        var topicPartition = new TopicPartition { Topic = topic, Partition = partition };
        var log = _logManager.GetOrCreateLog(topicPartition);

        // Long-polling: keep trying until data is available or timeout
        var deadline = DateTime.UtcNow.AddMilliseconds(maxWaitMs);
        var (data, batchOffsets) = await _logManager.ReadBatchesContiguousAsync(topicPartition, offset, maxBytes, cancellationToken);

        while (data.Length == 0 && maxWaitMs > 0 && DateTime.UtcNow < deadline)
        {
            // Wait for new data with remaining timeout
            var remainingMs = (int)(deadline - DateTime.UtcNow).TotalMilliseconds;
            if (remainingMs <= 0) break;

            await log.WaitForDataAsync(offset, TimeSpan.FromMilliseconds(Math.Min(remainingMs, 1000)), cancellationToken);

            // Re-read regardless of whether WaitForDataAsync returned true
            // (handles race conditions where data arrived but notification was missed)
            (data, batchOffsets) = await _logManager.ReadBatchesContiguousAsync(topicPartition, offset, maxBytes, cancellationToken);
        }

        // Pre-size for the native re-framing: it writes fixed int32/int64 fields where Kafka used
        // varints, so the output is LARGER than the input for small records — "data.Length + 128"
        // guaranteed a grow+copy of the whole response (#83). 24 bytes/record upper-bounds the
        // fixed-field delta; the 61-byte batch header is not copied and credits header expansion.
        var dataSpan = data.Span;
        var estimatedRecords = 0;
        for (int i = 0; i < batchOffsets.Count; i++)
        {
            var start = batchOffsets[i];
            var end = i + 1 < batchOffsets.Count ? batchOffsets[i + 1] : data.Length;
            estimatedRecords += RecordBatchStreamer.PeekRecordCount(dataSpan.Slice(start, end - start));
        }

        // Use pooled writer for zero-allocation fetch path (Dispose returns to pool)
        using var writer = BigEndianWriter.Rent(12 + data.Length + estimatedRecords * 24);

        writer.Write(log.HighWatermark);
        var countPosition = writer.Length;
        writer.Write(0); // Placeholder for message count

        var totalMessageCount = 0;
        for (int i = 0; i < batchOffsets.Count; i++)
        {
            var batchStart = batchOffsets[i];
            var batchEnd = i + 1 < batchOffsets.Count ? batchOffsets[i + 1] : data.Length;
            var batchSpan = dataSpan.Slice(batchStart, batchEnd - batchStart);

            try
            {
                totalMessageCount += RecordBatchStreamer.StreamBatchRawToWriter(batchSpan, writer);
            }
            catch (Exception ex)
            {
                // Only log if warning level is enabled (avoid allocation when disabled)
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning(ex, "Failed to parse record batch at offset {BatchIndex}", i);
                }
            }
        }

        writer.PatchInt32(countPosition, totalMessageCount);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.FetchResponse,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleListOffsetsAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);

        var topic = reader.ReadString();
        var partition = reader.ReadInt32();
        var timestamp = reader.ReadInt64();

        // Auto-create topic if enabled (same as Produce behavior)
        var topicMetadata = _logManager.GetTopicMetadata(topic!);
        if (topicMetadata == null)
        {
            if (_config.AutoCreateTopics)
            {
                _logger.LogInformation("Auto-creating topic {Topic} on ListOffsets with {Partitions} partitions", topic, _config.DefaultNumPartitions);
                topicMetadata = await _logManager.CreateTopicAsync(
                    topic!,
                    _config.DefaultNumPartitions,
                    _config.DefaultReplicationFactor,
                    config: null,
                    cancellationToken);
            }
            else
            {
                await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                    SurgewaveErrorCode.TopicNotFound, $"Topic not found: {topic}", cancellationToken);
                return;
            }
        }

        if (partition < 0 || partition >= topicMetadata.PartitionCount)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.PartitionNotFound, $"Partition not found: {partition}", cancellationToken);
            return;
        }

        var topicPartition = new TopicPartition { Topic = topic!, Partition = partition };
        var log = _logManager.GetOrCreateLog(topicPartition);

        long offset = timestamp switch
        {
            -1 => log.HighWatermark,
            -2 => log.LogStartOffset,
            _ => log.FindOffsetByTimestamp(timestamp) ?? log.HighWatermark
        };

        // Use pooled buffer to avoid allocation
        var responsePayload = ArrayPool<byte>.Shared.Rent(8);
        try
        {
            BinaryPrimitives.WriteInt64BigEndian(responsePayload, offset);
            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ListOffsets,
                SurgewaveErrorCode.None, responsePayload.AsMemory(0, 8), cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responsePayload);
        }
    }

    private async Task HandleCommitOffsetAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var generationId = reader.ReadInt32();
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var offset = reader.ReadInt64();

        var result = _groupCoordinator.CommitOffset(groupId, memberId, generationId, topic, partition, offset, null);

        using var writer = new BigEndianWriter();
        writer.Write((ushort)result.ErrorCode);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.CommitOffset,
            (SurgewaveErrorCode)result.ErrorCode, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleFetchOffsetAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var groupId = reader.ReadString() ?? "";
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();

        var result = _groupCoordinator.FetchOffset(groupId, topic, partition);

        using var writer = new BigEndianWriter();
        writer.Write((ushort)result.ErrorCode);
        writer.Write(result.Offset);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.FetchOffset,
            (SurgewaveErrorCode)result.ErrorCode, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleNackAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);

        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var offset = reader.ReadInt64();

        if (_dlqManager == null)
        {
            await context.SendErrorAsync(context.Header.RequestId, context.Header.OpCode,
                SurgewaveErrorCode.InvalidRequest, "Broker DLQ is not enabled", cancellationToken);
            return;
        }

        var routedToDlq = await _dlqManager.HandleNackAsync(topic, partition, offset, cancellationToken);
        var retryCount = _dlqManager.GetRetryCount(topic, partition, offset);

        using var writer = new BigEndianWriter();
        writer.Write((byte)(routedToDlq ? 1 : 0));
        writer.Write(retryCount);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.NackAck,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }
}
