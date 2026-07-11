using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// High-level consumer client compatible with Kafka consumers
/// </summary>
public sealed class KafkaConsumer : IAsyncDisposable
{
    private readonly ConsumerConfig _config;
    private readonly KafkaTransport.OpenedTransport _transport;
    private readonly Stream _stream;
    private readonly string _clientId;
    private readonly Dictionary<TopicPartition, long> _subscriptions = new();
    // After SASL handshake we've already used correlation ids 1 and 2;
    // start the regular request stream past those.
    private int _correlationId = 100;
    private bool _disposed;

    public KafkaConsumer(ConsumerConfig config)
    {
        _config = config;
        _clientId = config.ClientId ?? $"kafka-consumer-{Guid.NewGuid():N}";

        // Transport handles TCP connect → optional TLS → optional SASL.
        _transport = KafkaTransport.Open(config.BootstrapServers, _clientId, config.Ssl, config.Sasl);
        _stream = _transport.Stream;
    }

    /// <summary>
    /// The topic-partitions this consumer is currently subscribed or assigned to.
    /// </summary>
    public IReadOnlyCollection<TopicPartition> Assignment => _subscriptions.Keys;

    /// <summary>
    /// Subscribe to topics. Fetches topic metadata from the broker and subscribes
    /// to every partition of each topic (single-consumer semantics — no group
    /// assignment is performed; this consumer reads all partitions itself).
    /// </summary>
    public void Subscribe(params string[] topics)
        => SubscribeAsync(topics).GetAwaiter().GetResult();

    /// <summary>
    /// Subscribe to topics asynchronously. Fetches topic metadata from the broker
    /// and subscribes to every partition of each topic. Failures during metadata
    /// discovery are propagated as <see cref="BrokerResponseException"/>.
    /// </summary>
    public async Task SubscribeAsync(string[] topics, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (topics.Length == 0)
        {
            return;
        }

        var metadata = await FetchMetadataAsync(topics, cancellationToken);
        var initialOffset = _config.AutoOffsetReset == "earliest" ? 0L : -1L;

        foreach (var topic in topics)
        {
            var topicMetadata = metadata.Topics.FirstOrDefault(
                    t => string.Equals(t.Name, topic, StringComparison.Ordinal))
                ?? throw new BrokerResponseException(
                    $"Metadata response did not contain topic '{topic}'", nameof(ApiKey.Metadata));

            if (topicMetadata.ErrorCode != ErrorCode.None)
            {
                throw new BrokerResponseException(
                    $"Metadata request for topic '{topic}' failed: {topicMetadata.ErrorCode}",
                    nameof(ApiKey.Metadata));
            }

            if (topicMetadata.Partitions.Count == 0)
            {
                throw new BrokerResponseException(
                    $"Metadata for topic '{topic}' contains no partitions",
                    nameof(ApiKey.Metadata));
            }

            foreach (var partition in topicMetadata.Partitions)
            {
                if (partition.ErrorCode != ErrorCode.None)
                {
                    throw new BrokerResponseException(
                        $"Metadata for '{topic}' partition {partition.PartitionIndex} failed: {partition.ErrorCode}",
                        nameof(ApiKey.Metadata));
                }

                var tp = new TopicPartition { Topic = topic, Partition = partition.PartitionIndex };
                _subscriptions[tp] = initialOffset;
            }
        }
    }

    /// <summary>
    /// Fetch topic metadata from the broker over the existing connection.
    /// Uses Metadata v4 (non-flexible wire format, supports AllowAutoTopicCreation).
    /// </summary>
    private async Task<MetadataResponse> FetchMetadataAsync(string[] topics, CancellationToken cancellationToken)
    {
        const short metadataVersion = 4;

        var request = new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = metadataVersion,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            Topics = topics
                .Select(t => new MetadataRequest.MetadataRequestTopic { Name = t })
                .ToList()
        };

        // Send request - combine size prefix + payload into single write
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);

        var requestSpan = writer.WrittenSpan;
        var totalWriteLength = 4 + requestSpan.Length;
        var combinedBuffer = ArrayPool<byte>.Shared.Rent(totalWriteLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(combinedBuffer, requestSpan.Length);
            requestSpan.CopyTo(combinedBuffer.AsSpan(4));
            await _stream.WriteAsync(combinedBuffer.AsMemory(0, totalWriteLength), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combinedBuffer);
        }

        // Read response - use pooled buffer
        var responseSizeBuffer = ArrayPool<byte>.Shared.Rent(4);
        int responseSize;
        try
        {
            await _stream.ReadExactlyAsync(responseSizeBuffer.AsMemory(0, 4), cancellationToken);
            responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer.AsSpan(0, 4));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseSizeBuffer);
        }

        if (responseSize < 4)
        {
            throw new BrokerResponseException(
                $"Metadata response too short ({responseSize} bytes)", nameof(ApiKey.Metadata));
        }

        var responseBytes = ArrayPool<byte>.Shared.Rent(responseSize);
        try
        {
            await _stream.ReadExactlyAsync(responseBytes.AsMemory(0, responseSize), cancellationToken);

            // Response header: 4-byte correlation id, then the body.
            var correlationId = BinaryPrimitives.ReadInt32BigEndian(responseBytes.AsSpan(0, 4));
            return MetadataResponse.ReadFrom(
                responseBytes.AsSpan(4, responseSize - 4), metadataVersion, correlationId);
        }
        catch (InvalidDataException ex)
        {
            throw new BrokerResponseException(
                "Malformed Metadata response from broker", ex);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseBytes);
        }
    }

    /// <summary>
    /// Assign specific topic-partition
    /// </summary>
    public void Assign(TopicPartition topicPartition)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _subscriptions[topicPartition] = _config.AutoOffsetReset == "earliest" ? 0 : -1;
    }

    /// <summary>
    /// Consume a single message asynchronously
    /// </summary>
    public async Task<ConsumerRecord?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var records = await PollAsync(timeout, cancellationToken);
        return records.Count > 0 ? records[0] : null;
    }

    /// <summary>
    /// Poll for new records
    /// </summary>
    public async Task<List<ConsumerRecord>> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_subscriptions.Count == 0)
        {
            return [];
        }

        var records = new List<ConsumerRecord>();

        foreach (var (topicPartition, offset) in _subscriptions.ToList())
        {
            var fetchedRecords = await FetchAsync(topicPartition, offset, cancellationToken);
            records.AddRange(fetchedRecords);

            // Update offset
            if (fetchedRecords.Count > 0)
            {
                _subscriptions[topicPartition] = fetchedRecords[^1].Offset + 1;
            }
        }

        return records;
    }

    /// <summary>
    /// Seek to a specific offset
    /// </summary>
    public void Seek(TopicPartition topicPartition, long offset)
    {
        _subscriptions[topicPartition] = offset;
    }

    /// <summary>
    /// Commit offsets for the specified topic-partitions
    /// </summary>
    public async Task CommitAsync(IEnumerable<(TopicPartition TopicPartition, long Offset)> offsets, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (string.IsNullOrEmpty(_config.GroupId))
        {
            throw new InvalidOperationException("Cannot commit offsets without a GroupId configured");
        }

        var offsetList = offsets.ToList();
        if (offsetList.Count == 0)
        {
            return;
        }

        // Group by topic
        var topicCommits = offsetList
            .GroupBy(o => o.TopicPartition.Topic)
            .Select(g => new TopicPartitionCommit
            {
                Topic = g.Key,
                Partitions = g.Select(o => new PartitionCommit
                {
                    PartitionIndex = o.TopicPartition.Partition,
                    CommittedOffset = o.Offset,
                    CommittedLeaderEpoch = -1,
                    Metadata = null
                }).ToList()
            })
            .ToList();

        var request = new OffsetCommitRequest
        {
            ApiKey = ApiKey.OffsetCommit,
            ApiVersion = 2, // v2 is widely supported
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            GroupId = _config.GroupId,
            GenerationIdOrMemberEpoch = -1,
            MemberId = null,
            RetentionTimeMs = -1,
            Topics = topicCommits
        };

        // Send request - combine size prefix + payload into single write
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);

        var requestSpan = writer.WrittenSpan;
        var totalWriteLength = 4 + requestSpan.Length;
        var combinedBuffer = ArrayPool<byte>.Shared.Rent(totalWriteLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(combinedBuffer, requestSpan.Length);
            requestSpan.CopyTo(combinedBuffer.AsSpan(4));
            await _stream.WriteAsync(combinedBuffer.AsMemory(0, totalWriteLength), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combinedBuffer);
        }

        // Read response - use pooled buffer
        var responseSizeBuffer = ArrayPool<byte>.Shared.Rent(4);
        int responseSize;
        try
        {
            await _stream.ReadExactlyAsync(responseSizeBuffer.AsMemory(0, 4), cancellationToken);
            responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer.AsSpan(0, 4));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseSizeBuffer);
        }
        var responseBytes = ArrayPool<byte>.Shared.Rent(responseSize);
        try
        {
            await _stream.ReadExactlyAsync(responseBytes.AsMemory(0, responseSize), cancellationToken);

            // Parse response to check for errors
            using var responseStream = new MemoryStream(responseBytes, 0, responseSize, writable: false);
            using var reader = new BinaryReader(responseStream);

            var correlationId = reader.ReadInt32();
            var topicCount = reader.ReadInt32();

            for (int i = 0; i < topicCount; i++)
            {
                var topic = ReadString(reader);
                var partitionCount = reader.ReadInt32();

                for (int j = 0; j < partitionCount; j++)
                {
                    var partition = reader.ReadInt32();
                    var errorCode = (ErrorCode)reader.ReadInt16();

                    if (errorCode != ErrorCode.None)
                    {
                        throw new InvalidOperationException($"Failed to commit offset for {topic}[{partition}]: {errorCode}");
                    }
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseBytes);
        }
    }

    /// <summary>
    /// Commit all current offsets
    /// </summary>
    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        var offsets = _subscriptions.Select(kv => (kv.Key, kv.Value));
        return CommitAsync(offsets, cancellationToken);
    }

    private async Task<List<ConsumerRecord>> FetchAsync(TopicPartition topicPartition, long offset, CancellationToken cancellationToken)
    {
        // Fetch v4 adds last_stable_offset + aborted_transactions between high_watermark
        // and record_set. The parser below handles both v3 and v4 layouts.
        var request = new FetchRequest
        {
            ApiKey = ApiKey.Fetch,
            ApiVersion = 4,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            ReplicaId = -1,
            MaxWaitMs = _config.FetchMaxWaitMs,
            MinBytes = _config.FetchMinBytes,
            MaxBytes = 1024 * 1024, // 1MB
            Topics =
            [
                new FetchRequest.FetchTopic
                {
                    Topic = topicPartition.Topic,
                    Partitions =
                    [
                        new FetchRequest.FetchPartition
                        {
                            Partition = topicPartition.Partition,
                            FetchOffset = offset,
                            MaxBytes = 1024 * 1024
                        }
                    ]
                }
            ]
        };

        // Send request - combine size prefix + payload into single write
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);

        var requestSpan = writer.WrittenSpan;
        var totalWriteLength = 4 + requestSpan.Length;
        var combinedBuffer = ArrayPool<byte>.Shared.Rent(totalWriteLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(combinedBuffer, requestSpan.Length);
            requestSpan.CopyTo(combinedBuffer.AsSpan(4));
            await _stream.WriteAsync(combinedBuffer.AsMemory(0, totalWriteLength), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combinedBuffer);
        }

        // Read response - use pooled buffer
        var responseSizeBuffer = ArrayPool<byte>.Shared.Rent(4);
        int responseSize;
        try
        {
            await _stream.ReadExactlyAsync(responseSizeBuffer.AsMemory(0, 4), cancellationToken);
            responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer.AsSpan(0, 4));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseSizeBuffer);
        }
        var responseBytes = ArrayPool<byte>.Shared.Rent(responseSize);
        try
        {
            await _stream.ReadExactlyAsync(responseBytes.AsMemory(0, responseSize), cancellationToken);

            // Parse response
            using var responseStream = new MemoryStream(responseBytes, 0, responseSize, writable: false);
            using var reader = new BinaryReader(responseStream);

            var records = new List<ConsumerRecord>();
            try
            {
                var correlationId = reader.ReadInt32();
                var throttleTimeMs = reader.ReadInt32();

                var topicCount = reader.ReadInt32();

                for (int i = 0; i < topicCount; i++)
                {
                    var topic = ReadString(reader);
                    var partitionCount = reader.ReadInt32();

                    for (int j = 0; j < partitionCount; j++)
                    {
                        var partition = reader.ReadInt32();
                        var errorCode = (ErrorCode)reader.ReadInt16();
                        var highWatermark = reader.ReadInt64();

                        // Fetch v4+: last_stable_offset + aborted_transactions
                        var _lastStableOffset = reader.ReadInt64(); // skip
                        var abortedTxnCount = reader.ReadInt32();
                        for (int k = 0; k < abortedTxnCount; k++)
                        {
                            reader.ReadInt64(); // producerId
                            reader.ReadInt64(); // firstOffset
                        }

                        var recordSetSize = reader.ReadInt32();

                        if (errorCode == ErrorCode.None && recordSetSize > 0)
                        {
                            var recordSetBytes = reader.ReadBytes(recordSetSize);
                            var parsedRecords = ParseRecordSet(topic, partition, recordSetBytes);
                            records.AddRange(parsedRecords);
                        }
                    }
                }
            }
            catch (EndOfStreamException)
            {
                // Truncated or unexpected response layout — treat as empty fetch. Happens
                // when the broker returns a Fetch response in a version newer than v3 (the
                // parser above), or when the topic has no data yet. We'd rather return an
                // empty batch than bubble a cryptic EOF up into consumer callers.
                return records;
            }

            return records;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseBytes);
        }
    }

    private List<ConsumerRecord> ParseRecordSet(string topic, int partition, byte[] recordSetBytes)
    {
        var records = new List<ConsumerRecord>();

        using var stream = new MemoryStream(recordSetBytes);
        using var reader = new BinaryReader(stream);

        while (stream.Position < stream.Length)
        {
            try
            {
                var recordOffset = reader.ReadInt64();
                var timestamp = reader.ReadInt64();

                var keyLen = reader.ReadInt32();
                var key = keyLen > 0 ? reader.ReadBytes(keyLen) : null;

                var valueLen = reader.ReadInt32();
                var value = reader.ReadBytes(valueLen);

                var headersLen = reader.ReadInt32();
                if (headersLen > 0)
                {
                    reader.ReadBytes(headersLen); // Skip headers for now
                }

                records.Add(new ConsumerRecord
                {
                    Topic = topic,
                    Partition = partition,
                    Offset = recordOffset,
                    Timestamp = timestamp,
                    Key = key,
                    Value = value
                });
            }
            catch (EndOfStreamException)
            {
                break;
            }
        }

        return records;
    }

    private string ReadString(BinaryReader reader)
    {
        var length = reader.ReadInt16();
        if (length < 0) return string.Empty;

        var bytes = reader.ReadBytes(length);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _transport.Dispose();
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
