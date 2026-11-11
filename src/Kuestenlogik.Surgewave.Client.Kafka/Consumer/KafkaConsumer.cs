using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Client.Kafka;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// High-level consumer client compatible with Kafka consumers
/// </summary>
public sealed class KafkaConsumer : IAsyncDisposable
{
    // Fetch v4 (record batches with last_stable_offset) and OffsetCommit v2: both still
    // non-flexible, and what the response readers in Protocol.Kafka take (#236).
    private const short FetchVersion = 4;
    private const short OffsetCommitVersion = 2;
    private const short ListOffsetsVersion = 2;

    private static readonly RecordBatchSerializer s_batches = new(NullLogger<RecordBatchSerializer>.Instance);

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
        var initialOffset = StartOffset;

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

        // "Latest" means the end of the log when the subscription is made, not when the
        // first poll happens to run, so the start offsets are resolved here.
        await ResolveStartOffsetsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Where a partition with no position starts: the ListOffsets timestamp for the configured
    /// reset, earliest (-2) or latest (-1). It is resolved to a real offset before the first
    /// fetch; a fetch at a negative offset returns nothing, so a consumer left there would
    /// never see a record (#236).
    /// </summary>
    private long StartOffset => _config.AutoOffsetReset == "earliest"
        ? ListOffsetsRequest.TimestampType.Earliest
        : ListOffsetsRequest.TimestampType.Latest;

    private async Task ResolveStartOffsetsAsync(CancellationToken cancellationToken)
    {
        var unresolved = _subscriptions.Where(kv => kv.Value < 0).ToList();
        if (unresolved.Count == 0)
        {
            return;
        }

        var request = new ListOffsetsRequest
        {
            ApiKey = ApiKey.ListOffsets,
            ApiVersion = ListOffsetsVersion,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            ReplicaId = -1,
            IsolationLevel = 0,
            Topics = unresolved
                .GroupBy(kv => kv.Key.Topic)
                .Select(g => new TopicPartitionTimestamp
                {
                    Topic = g.Key,
                    Partitions = g.Select(kv => new PartitionTimestamp
                    {
                        PartitionIndex = kv.Key.Partition,
                        Timestamp = kv.Value,
                    }).ToList(),
                })
                .ToList(),
        };

        var body = await KafkaWire.RoundTripAsync(_stream, request, nameof(ApiKey.ListOffsets), cancellationToken)
            .ConfigureAwait(false);
        ListOffsetsResponse response;
        try
        {
            response = ListOffsetsResponse.ReadFrom(body, ListOffsetsVersion, request.CorrelationId);
        }
        catch (InvalidDataException ex)
        {
            throw new BrokerResponseException("Malformed ListOffsets response from broker", ex);
        }

        foreach (var topic in response.Topics)
        {
            foreach (var partition in topic.Partitions)
            {
                if (partition.ErrorCode != ErrorCode.None)
                {
                    throw new BrokerResponseException(
                        $"ListOffsets for {topic.Topic}[{partition.PartitionIndex}] failed: {partition.ErrorCode}",
                        nameof(ApiKey.ListOffsets));
                }
                _subscriptions[new TopicPartition { Topic = topic.Topic, Partition = partition.PartitionIndex }] = partition.Offset;
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
        // Resolved to a real offset before the next fetch.
        _subscriptions[topicPartition] = StartOffset;
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

        await ResolveStartOffsetsAsync(cancellationToken).ConfigureAwait(false);

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
            ApiVersion = OffsetCommitVersion,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            GroupId = _config.GroupId,
            GenerationIdOrMemberEpoch = -1,
            MemberId = null,
            RetentionTimeMs = -1,
            Topics = topicCommits
        };

        var body = await KafkaWire.RoundTripAsync(_stream, request, nameof(ApiKey.OffsetCommit), cancellationToken)
            .ConfigureAwait(false);
        OffsetCommitResponse response;
        try
        {
            response = OffsetCommitResponse.ReadFrom(body, OffsetCommitVersion, request.CorrelationId);
        }
        catch (InvalidDataException ex)
        {
            throw new BrokerResponseException("Malformed OffsetCommit response from broker", ex);
        }

        foreach (var topic in response.Topics)
        {
            foreach (var partition in topic.Partitions)
            {
                if (partition.ErrorCode != ErrorCode.None)
                {
                    throw new InvalidOperationException(
                        $"Failed to commit offset for {topic.Topic}[{partition.PartitionIndex}]: {partition.ErrorCode}");
                }
            }
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
        var request = new FetchRequest
        {
            ApiKey = ApiKey.Fetch,
            ApiVersion = FetchVersion,
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

        var body = await KafkaWire.RoundTripAsync(_stream, request, nameof(ApiKey.Fetch), cancellationToken)
            .ConfigureAwait(false);
        FetchResponse response;
        try
        {
            response = FetchResponse.ReadFrom(body, FetchVersion, request.CorrelationId);
        }
        catch (InvalidDataException ex)
        {
            throw new BrokerResponseException("Malformed Fetch response from broker", ex);
        }

        var records = new List<ConsumerRecord>();
        foreach (var topic in response.Responses)
        {
            foreach (var partition in topic.Partitions)
            {
                if (partition.ErrorCode != ErrorCode.None)
                {
                    throw new BrokerResponseException(
                        $"Fetch of {topic.Topic}[{partition.Partition}] failed: {partition.ErrorCode}", nameof(ApiKey.Fetch));
                }
                records.AddRange(ParseRecordSet(topic.Topic ?? topicPartition.Topic, partition.Partition, partition.RecordSet.Span, offset));
            }
        }
        return records;
    }

    /// <summary>
    /// The records of a fetched record set: one or more record batches (magic v2) back to
    /// back. A broker may cut the last batch short at the fetch's byte limit; that partial
    /// batch is left for the next fetch, as Kafka clients do. Records before
    /// <paramref name="fetchOffset"/> are skipped: a batch is returned whole even when the
    /// fetch starts inside it.
    /// </summary>
    private static List<ConsumerRecord> ParseRecordSet(string topic, int partition, ReadOnlySpan<byte> recordSet, long fetchOffset)
    {
        const int batchPrefix = 12; // base offset (8) + batch length (4)
        var records = new List<ConsumerRecord>();
        var pos = 0;
        while (recordSet.Length - pos >= batchPrefix)
        {
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(recordSet.Slice(pos + 8, 4));
            var batchSize = batchPrefix + batchLength;
            if (batchLength <= 0 || batchSize > recordSet.Length - pos)
            {
                break;
            }

            foreach (var message in s_batches.ParseRecordBatch(recordSet.Slice(pos, batchSize).ToArray()))
            {
                if (message.Offset < fetchOffset)
                {
                    continue;
                }
                records.Add(new ConsumerRecord
                {
                    Topic = topic,
                    Partition = partition,
                    Offset = message.Offset,
                    Timestamp = message.Timestamp,
                    Key = message.Key.IsEmpty ? null : message.Key.ToArray(),
                    Value = message.Value.ToArray(),
                    Headers = KafkaWire.DecodeHeaders(message.Headers.Span),
                });
            }
            pos += batchSize;
        }
        return records;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _transport.Dispose();
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
