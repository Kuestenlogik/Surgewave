using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Storage.Indexing;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Read;
using Kuestenlogik.Surgewave.Storage.Disaggregated.Routing;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for core data APIs: Produce, Fetch, ListOffsets
/// </summary>
public sealed partial class DataApiHandler : IKafkaRequestHandler
{
    private readonly IBrokerConfigView _config;
    private readonly LogManager _logManager;
    private readonly IProduceTransactionCoordinator _transactionCoordinator;
    private readonly IQuotaManager _quotaManager;
    private readonly IBandwidthQuota? _bandwidthQuotaManager;
    private readonly RecordBatchSerializer _recordBatchSerializer;
    private readonly IAuthorizer? _aclAuthorizer;
    private readonly IDeduplicationManager? _deduplicationManager;
    private readonly IDelayIndex? _delayIndex;
    private readonly ITtlIndex? _ttlIndex;
    private readonly IBrokerMetrics? _metrics;
    private readonly SurgewaveBrokerObservability? _observability;
    private readonly IRecordTransformPipeline? _recordTransform;
    private readonly IColdStartProfiler? _coldStartProfiler;
    private readonly IPartitionAppender _partitionAppender;
    private readonly IDisaggregatedSegmentReader? _disaggregatedReader;
    private readonly ILogger<DataApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.Produce,
        ApiKey.Fetch,
        ApiKey.ListOffsets
    ];

    public DataApiHandler(
        IBrokerConfigView config,
        LogManager logManager,
        IProduceTransactionCoordinator transactionCoordinator,
        IQuotaManager quotaManager,
        RecordBatchSerializer recordBatchSerializer,
        IAuthorizer? aclAuthorizer,
        IDeduplicationManager? deduplicationManager,
        IDelayIndex? delayIndex,
        ITtlIndex? ttlIndex,
        IBrokerMetrics? metrics,
        ILogger<DataApiHandler> logger,
        IBandwidthQuota? bandwidthQuotaManager = null,
        SurgewaveBrokerObservability? observability = null,
        IRecordTransformPipeline? recordTransform = null,
        IColdStartProfiler? coldStartProfiler = null,
        IPartitionAppender? partitionAppender = null,
        IDisaggregatedSegmentReader? disaggregatedReader = null)
    {
        _config = config;
        _logManager = logManager;
        _transactionCoordinator = transactionCoordinator;
        _quotaManager = quotaManager;
        _bandwidthQuotaManager = bandwidthQuotaManager;
        _recordBatchSerializer = recordBatchSerializer;
        _aclAuthorizer = aclAuthorizer;
        _deduplicationManager = deduplicationManager;
        _delayIndex = delayIndex;
        _ttlIndex = ttlIndex;
        _metrics = metrics;
        _observability = observability;
        _recordTransform = recordTransform;
        _coldStartProfiler = coldStartProfiler;
        _logger = logger;
        // Default = direct LogManager call (pre-G21 behaviour). Operators that
        // enable disaggregated storage pass a RoutingPartitionAppender via
        // SurgewaveRuntimeBuilder.WithPartitionAppender(...).
        // Validate: these bytes came from a producer with their own CRC. Checking it costs the same
        // single pass the append already made to overwrite it, and stops us from silently healing
        // corruption into the log (#85).
        _partitionAppender = partitionAppender
            ?? new DelegatingPartitionAppender((tp, batch, _, ct) =>
                _logManager.AppendBatchAsync(tp, batch, BatchCrcMode.Validate, ct).AsTask());
        _disaggregatedReader = disaggregatedReader;
    }

    /// <summary>
    /// Rewrites a transformed batch's CRC so the validating append accepts it: a record-transform
    /// plugin changes the records but carries no CRC contract (#85).
    /// </summary>
    /// <returns>
    /// The same memory when it is array-backed (stamped in place), otherwise a stamped copy —
    /// never the unstamped input, which the append would reject as corrupt.
    /// </returns>
    private static ReadOnlyMemory<byte> RestampCrc(ReadOnlyMemory<byte> batch)
    {
        if (batch.Length < RecordBatchValidator.MinBatchHeaderSize)
        {
            // Too short to be a RecordBatch — let the append reject it with a precise message.
            return batch;
        }

        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(batch, out ArraySegment<byte> segment))
        {
            StampCrc(segment.Array!.AsSpan(segment.Offset, segment.Count));
            return batch;
        }

        var copy = batch.ToArray();
        StampCrc(copy);
        return copy;
    }

    private static void StampCrc(Span<byte> batch)
    {
        var crc = Crc32C.Compute(batch[RecordBatchValidator.CrcDataOffset..]);
        BinaryPrimitives.WriteUInt32BigEndian(batch.Slice(RecordBatchValidator.CrcOffset, 4), crc);
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            ProduceRequest produceRequest => await HandleProduceAsync(produceRequest, context.ConnectionState, cancellationToken),
            FetchRequest fetchRequest => await HandleFetchAsync(fetchRequest, context.ConnectionState, cancellationToken),
            ListOffsetsRequest listOffsetsRequest => HandleListOffsets(listOffsetsRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by DataApiHandler")
        };
    }

    private async Task<ProduceResponse> HandleProduceAsync(ProduceRequest request, ConnectionState connectionState, CancellationToken cancellationToken)
    {
        var responses = new List<ProduceResponse.TopicProduceResponse>(request.TopicData.Count);

        // Calculate total bytes to produce for quota check (inline loop avoids LINQ closure allocations)
        long totalBytes = 0;
        foreach (var t in request.TopicData)
            foreach (var p in t.PartitionData)
                totalBytes += p.Records.Length;

        // Check produce quota (token bucket)
        var clientId = request.ClientId;
        var throttleTimeMs = _quotaManager.CheckProduceQuota(clientId, totalBytes);

        // Check bandwidth quota (sliding window per-client/user)
        if (_bandwidthQuotaManager is { Enabled: true })
        {
            var bwResult = _bandwidthQuotaManager.CheckAndRecordProduce(clientId, connectionState.AuthenticatedUser, totalBytes);
            if (bwResult.Throttled && bwResult.Delay.HasValue)
            {
                var bwThrottleMs = (int)Math.Ceiling(bwResult.Delay.Value.TotalMilliseconds);
                throttleTimeMs = Math.Max(throttleTimeMs, bwThrottleMs);
            }
        }

        foreach (var topicData in request.TopicData)
        {
            var topic = topicData.Name ?? string.Empty;
            var partitionResponses = new List<ProduceResponse.PartitionProduceResponse>(topicData.PartitionData.Count);

            // Reject writes to read-only mirror topics (geo-replication)
            var topicMetadata = _logManager.GetTopicMetadata(topic);
            if (topicMetadata is { IsReadOnly: true })
            {
                foreach (var partitionData in topicData.PartitionData)
                {
                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.TopicAuthorizationFailed,
                        BaseOffset = -1,
                        LogAppendTimeMs = -1
                    });
                }
                responses.Add(new ProduceResponse.TopicProduceResponse
                {
                    Name = topic,
                    TopicId = topicData.TopicId,
                    PartitionResponses = partitionResponses
                });
                continue;
            }

            // Check authorization for producing to this topic
            if (!AuthorizeTopic(connectionState, topic, AclOperation.Write))
            {
                foreach (var partitionData in topicData.PartitionData)
                {
                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.TopicAuthorizationFailed,
                        BaseOffset = -1,
                        LogAppendTimeMs = -1
                    });
                }
                responses.Add(new ProduceResponse.TopicProduceResponse
                {
                    Name = topic,
                    TopicId = topicData.TopicId,
                    PartitionResponses = partitionResponses
                });
                continue;
            }

            foreach (var partitionData in topicData.PartitionData)
            {
                try
                {
                    // Check for unsupported compression before storing
                    var compressionType = CompressionCodec.GetCompressionTypeFromBatch(partitionData.Records.Span);
                    if (!CompressionCodec.IsSupported(compressionType))
                    {
                        partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                        {
                            Index = partitionData.Index,
                            ErrorCode = ErrorCode.UnsupportedCompressionType,
                            BaseOffset = -1,
                            LogAppendTimeMs = -1
                        });
                        continue;
                    }

                    var topicPartition = new TopicPartition
                    {
                        Topic = topic,
                        Partition = partitionData.Index
                    };

                    // Extract idempotence info and validate if present
                    var (producerId, producerEpoch, baseSequence, lastOffsetDelta) =
                        CompressionCodec.GetIdempotenceInfo(partitionData.Records.Span);

                    if (producerId != KafkaConstants.Producer.NoProducerId)
                    {
                        var validationStatus = _transactionCoordinator.ValidateProduceBatch(
                            producerId, producerEpoch, baseSequence, topicPartition);

                        if (validationStatus != ProduceSequenceStatus.Ok)
                        {
                            // Map the neutral sequence-validation status to the Kafka wire
                            // error code at the protocol boundary (part-c TxnErrorStatus pattern).
                            partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                            {
                                Index = partitionData.Index,
                                ErrorCode = validationStatus switch
                                {
                                    ProduceSequenceStatus.InvalidProducerEpoch => ErrorCode.InvalidProducerEpoch,
                                    ProduceSequenceStatus.UnknownProducerId => ErrorCode.UnknownProducerId,
                                    ProduceSequenceStatus.DuplicateSequence => ErrorCode.DuplicateSequenceNumber,
                                    ProduceSequenceStatus.OutOfOrderSequence => ErrorCode.OutOfOrderSequenceNumber,
                                    _ => ErrorCode.Unknown,
                                },
                                BaseOffset = -1,
                                LogAppendTimeMs = -1
                            });
                            continue;
                        }
                    }

                    // Content-based deduplication check (if enabled for this topic)
                    if (_deduplicationManager != null && IsDeduplicationEnabled(topic))
                    {
                        var dedupResult = _deduplicationManager.CheckDuplicate(topicPartition, partitionData.Records.Span);
                        if (dedupResult.IsDuplicate)
                        {
                            _metrics?.RecordDeduplication(topic, partitionData.Index);
                            partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                            {
                                Index = partitionData.Index,
                                ErrorCode = ErrorCode.None,
                                BaseOffset = dedupResult.OriginalOffset,
                                LogAppendTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            });
                            continue;
                        }
                    }

                    // On-broker record transform (G7 / Redpanda Data Transforms parity).
                    // Runs after dedup so we don't pay the WASM cost on duplicates,
                    // and before append so the persisted bytes are the post-transform
                    // payload. Returning null from the pipeline drops the batch
                    // silently — the producer sees success with the next-in-line
                    // base offset, but no records actually land.
                    var recordsToAppend = partitionData.Records;
                    if (_recordTransform is { } transform && transform.HasBinding(topic))
                    {
                        var transformed = await transform.TransformAsync(topic, recordsToAppend, cancellationToken)
                            .ConfigureAwait(false);
                        if (transformed is null)
                        {
                            // Drop: report a synthetic base offset matching the log's
                            // current end so the producer's idempotent state stays
                            // self-consistent.
                            var droppedLog = _logManager.GetLog(topicPartition);
                            partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                            {
                                Index = partitionData.Index,
                                ErrorCode = ErrorCode.None,
                                BaseOffset = droppedLog?.NextOffset ?? 0,
                                LogAppendTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            });
                            continue;
                        }
                        // The plugin rewrote the records, so the producer's CRC no longer describes
                        // them. Restamp before the validating append (#85).
                        recordsToAppend = RestampCrc(transformed.Value);
                    }

                    // Store raw RecordBatch bytes through the appender — defaults to
                    // direct LogManager append; in disaggregated mode a routing
                    // appender intercepts and dispatches stateless topics to the
                    // StatelessAgent. The record count is parsed from the batch
                    // header (Kafka RecordBatch v2, offset 57); stateless mode
                    // needs it for offset assignment.
                    var produceRecordCount = RecordHeaderParser.ParseBatchHeader(recordsToAppend.Span).RecordCount;
                    var baseOffset = await _partitionAppender.AppendBatchAsync(
                        topicPartition, recordsToAppend, produceRecordCount, cancellationToken);

                    // Register hash after successful write (deduplication)
                    _deduplicationManager?.Register(topicPartition, recordsToAppend.Span, baseOffset);

                    // Extract and index delayed delivery headers (if enabled for this topic)
                    if (_delayIndex != null && IsDelayDeliveryEnabled(topic))
                    {
                        var deliverAtMs = DelayHeaderParser.ExtractDeliverAtTimestamp(partitionData.Records.Span);
                        if (deliverAtMs.HasValue)
                        {
                            _delayIndex.RecordDelayedBatch(topicPartition, baseOffset, deliverAtMs.Value);
                        }
                    }

                    // Extract and index TTL headers (if enabled for this topic)
                    if (_ttlIndex != null && IsTtlEnabled(topic))
                    {
                        var expiryMs = TtlHeaderParser.ExtractExpiryTimestamp(partitionData.Records.Span);
                        if (expiryMs.HasValue)
                        {
                            _ttlIndex.RecordTtlBatch(topicPartition, baseOffset, expiryMs.Value);
                        }
                        else if (_config.DefaultTtlMs > 0)
                        {
                            // Apply default TTL when no header is present
                            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            _ttlIndex.RecordTtlBatch(topicPartition, baseOffset, nowMs + _config.DefaultTtlMs);
                        }
                    }

                    // Track transactional batches for LSO calculation
                    if (CompressionCodec.IsTransactional(partitionData.Records.Span) &&
                        !CompressionCodec.IsControlBatch(partitionData.Records.Span))
                    {
                        _transactionCoordinator.RecordTransactionalBatch(topicPartition, producerId, baseOffset);
                    }

                    RecordBatchStored(topic, partitionData.Index, baseOffset, partitionData.Records.Length);

                    // Record produce metrics
                    var recordCount = CompressionCodec.GetRecordCount(partitionData.Records.Span);
                    _metrics?.RecordProduce(topic, partitionData.Index, recordCount, partitionData.Records.Length, 0);
                    _coldStartProfiler?.RecordProduce(topic, recordCount, partitionData.Records.Length);

                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.None,
                        BaseOffset = baseOffset,
                        LogAppendTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    });

                    // Surface the produce event on the observability
                    // bus. The HasSubscribers gate is critical — without
                    // it we would allocate a SurgewaveBrokerEvent for every
                    // single produce even when no observer is wired up.
                    // Payload bytes are deliberately omitted (they would
                    // be a second copy of the batch); observers that
                    // need bytes subscribe to a regular consume stream.
                    // Rejected / Consumed / Rebalanced are also wired —
                    // see the catch block below, the fetch path further
                    // down, and ConsumerGroupCoordinator.HandleSyncGroup.
                    if (_observability?.HasSubscribers == true)
                    {
                        _observability.Publish(new SurgewaveBrokerEvent(
                            SurgewaveBrokerEventKind.Produced,
                            topic, partitionData.Index, baseOffset,
                            Principal: connectionState.AuthenticatedUser,
                            RejectReason: null, Consumers: null,
                            Key: null, Value: null,
                            Timestamp: DateTimeOffset.UtcNow));
                    }
                }
                catch (DataCorruptionException dex)
                {
                    // The producer's CRC did not match its own bytes — answer the way Kafka does
                    // instead of healing the corruption into the log (#85).
                    ProduceError(dex, topic, partitionData.Index);
                    _metrics?.RecordProduceError(topic, partitionData.Index, ErrorCode.CorruptMessage.ToString());

                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.CorruptMessage,
                        BaseOffset = -1,
                        LogAppendTimeMs = -1
                    });

                    if (_observability?.HasSubscribers == true)
                    {
                        _observability.Publish(new SurgewaveBrokerEvent(
                            SurgewaveBrokerEventKind.Rejected,
                            topic, partitionData.Index, Offset: null,
                            Principal: connectionState.AuthenticatedUser,
                            RejectReason: dex.Message, Consumers: null,
                            Key: null, Value: null,
                            Timestamp: DateTimeOffset.UtcNow));
                    }
                }
                catch (Exception ex)
                {
                    ProduceError(ex, topic, partitionData.Index);
                    _metrics?.RecordProduceError(topic, partitionData.Index, ErrorCode.Unknown.ToString());

                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.Unknown,
                        BaseOffset = -1,
                        LogAppendTimeMs = -1
                    });

                    if (_observability?.HasSubscribers == true)
                    {
                        _observability.Publish(new SurgewaveBrokerEvent(
                            SurgewaveBrokerEventKind.Rejected,
                            topic, partitionData.Index, Offset: null,
                            Principal: connectionState.AuthenticatedUser,
                            RejectReason: ex.Message, Consumers: null,
                            Key: null, Value: null,
                            Timestamp: DateTimeOffset.UtcNow));
                    }
                }
            }

            responses.Add(new ProduceResponse.TopicProduceResponse
            {
                Name = topic,
                TopicId = topicData.TopicId,
                PartitionResponses = partitionResponses
            });
        }

        // Record produced bytes for quota tracking (after successful produce)
        _quotaManager.RecordProducedBytes(clientId, totalBytes);

        return new ProduceResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Responses = responses,
            ThrottleTimeMs = throttleTimeMs
        };
    }

    private async Task<FetchResponse> HandleFetchAsync(FetchRequest request, ConnectionState connectionState, CancellationToken cancellationToken)
    {
        var responses = new List<FetchResponse.FetchableTopicResponse>(request.Topics.Count);
        var isReadCommitted = request.IsolationLevel == FetchRequest.ReadCommitted;

        // Check fetch quota upfront based on max bytes requested (inline loop avoids LINQ closure allocations)
        var clientId = request.ClientId;
        long maxBytesRequested = 0;
        foreach (var t in request.Topics)
            foreach (var p in t.Partitions)
                maxBytesRequested += p.MaxBytes;
        var throttleTimeMs = _quotaManager.CheckFetchQuota(clientId, maxBytesRequested);

        // Check bandwidth quota (sliding window per-client/user) — pre-flight check only, record actual bytes after fetch
        if (_bandwidthQuotaManager is { Enabled: true })
        {
            var bwResult = _bandwidthQuotaManager.CheckConsume(clientId, connectionState.AuthenticatedUser, maxBytesRequested);
            if (bwResult.Throttled && bwResult.Delay.HasValue)
            {
                var bwThrottleMs = (int)Math.Ceiling(bwResult.Delay.Value.TotalMilliseconds);
                throttleTimeMs = Math.Max(throttleTimeMs, bwThrottleMs);
            }
        }

        long totalBytesFetched = 0;

        foreach (var topicRequest in request.Topics)
        {
            // Fetch v13+ identifies topics by UUID only — the Name field is null on
            // the wire. Resolve the id to a name (KIP-516, used by KIP-848 next-gen
            // consumers) before the rest of the pipeline tries to look up partition
            // logs by topic name.
            var topic = topicRequest.Topic;
            if (string.IsNullOrEmpty(topic) && topicRequest.TopicId != Guid.Empty)
            {
                topic = _logManager.ResolveTopicId(topicRequest.TopicId);
            }
            topic ??= string.Empty;
            var partitionResponses = new List<FetchResponse.PartitionResponse>(topicRequest.Partitions.Count);

            // Check authorization for reading from this topic
            if (!AuthorizeTopic(connectionState, topic, AclOperation.Read))
            {
                foreach (var partitionData in topicRequest.Partitions)
                {
                    partitionResponses.Add(new FetchResponse.PartitionResponse
                    {
                        Partition = partitionData.Partition,
                        ErrorCode = ErrorCode.TopicAuthorizationFailed,
                        HighWatermark = 0,
                        RecordSet = []
                    });
                }
                responses.Add(new FetchResponse.FetchableTopicResponse
                {
                    Topic = topic,
                    TopicId = topicRequest.TopicId,
                    Partitions = partitionResponses
                });
                continue;
            }

            foreach (var partitionData in topicRequest.Partitions)
            {
                try
                {
                    var topicPartition = new TopicPartition
                    {
                        Topic = topic,
                        Partition = partitionData.Partition
                    };

                    // Get log once for all operations
                    var log = _logManager.GetLog(topicPartition);

                    // Debug: Log the state before fetch (Trace level - only when debugging)
                    FetchDebug(topic, partitionData.Partition, partitionData.FetchOffset,
                        log?.LogStartOffset ?? -1, log?.NextOffset ?? -1, log != null);

                    var highWatermark = log?.HighWatermark ?? 0;

                    // Determine if any per-batch filtering is needed. When not needed
                    // (the common case: READ_UNCOMMITTED, no delay, no TTL), use the
                    // contiguous read path — zero per-batch allocation, one Memory slice.
                    var needsFiltering = isReadCommitted
                        || (_delayIndex != null && IsDelayDeliveryEnabled(topic) && _delayIndex.HasDelayedRecords(topicPartition))
                        || (_ttlIndex != null && IsTtlEnabled(topic) && _ttlIndex.HasTtlRecords(topicPartition));

                    byte[] recordSet;
                    int messageCount;

                    // Disaggregated read fallback: when the topic uses
                    // disaggregated storage and the requested offset has
                    // already been flushed to the object store (i.e. the
                    // local WAL no longer holds it), serve from the
                    // manifest. The reader returns HitManifest=false for
                    // offsets past the manifest tail — those still live in
                    // the local WAL and the normal read path below picks
                    // them up. Skip when no reader is wired (default) or
                    // when the topic isn't disaggregated.
                    var fetchTopicMetadata = _logManager.GetTopicMetadata(topic);
                    if (_disaggregatedReader is not null && fetchTopicMetadata?.IsDisaggregated == true)
                    {
                        var disagRead = await _disaggregatedReader.TryReadAsync(
                            topicPartition,
                            partitionData.FetchOffset,
                            partitionData.MaxBytes,
                            cancellationToken).ConfigureAwait(false);
                        if (disagRead.HitManifest)
                        {
                            recordSet = disagRead.LogBytes.ToArray();
                            messageCount = 0;
                            // Same record-count tallying pattern as the
                            // contiguous fast path: walk the concatenated
                            // batches and read the count field at offset 57.
                            var span = disagRead.LogBytes.Span;
                            var cursor = 0;
                            while (cursor + 61 <= span.Length)
                            {
                                var batchLen = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(cursor + 8, 4));
                                var batchTotal = 12 + batchLen; // baseOffset(8) + batchLength(4) + body
                                if (cursor + 57 + 4 <= span.Length)
                                    messageCount += System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span.Slice(cursor + 57, 4));
                                cursor += batchTotal;
                            }

                            partitionResponses.Add(new FetchResponse.PartitionResponse
                            {
                                Partition = partitionData.Partition,
                                ErrorCode = ErrorCode.None,
                                HighWatermark = highWatermark,
                                RecordSet = recordSet,
                            });
                            continue;
                        }
                    }

                    if (!needsFiltering)
                    {
                        // Fast path: contiguous read — single allocation for all batches.
                        var (contiguousData, batchOffsets) = await _logManager.ReadBatchesContiguousAsync(
                            topicPartition, partitionData.FetchOffset,
                            maxBytes: partitionData.MaxBytes, cancellationToken);

                        BatchesRead(batchOffsets.Count, topic, partitionData.Partition, partitionData.FetchOffset);

                        recordSet = contiguousData.Length > 0
                            ? contiguousData.ToArray()   // single copy for the response
                            : Array.Empty<byte>();

                        messageCount = 0;
                        foreach (var offset in batchOffsets)
                        {
                            if (offset + 57 + 4 <= contiguousData.Length)
                                messageCount += System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
                                    contiguousData.Span.Slice(offset + 57, 4));
                        }
                    }
                    else
                    {
                        // Slow path: per-batch read + filtering
                        var recordBatches = await _logManager.ReadBatchesAsync(
                            topicPartition, partitionData.FetchOffset,
                            maxBytes: partitionData.MaxBytes, cancellationToken);

                        BatchesRead(recordBatches.Count, topic, partitionData.Partition, partitionData.FetchOffset);

                        var filteredBatches = recordBatches.Count > 0
                            ? FilterBatchesForIsolationLevel(topicPartition, recordBatches, isReadCommitted, highWatermark)
                            : recordBatches;

                        if (_delayIndex != null && filteredBatches.Count > 0 &&
                            IsDelayDeliveryEnabled(topic) && _delayIndex.HasDelayedRecords(topicPartition))
                        {
                            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            filteredBatches = DelayFilter.FilterDelayedBatches(filteredBatches, _delayIndex, topicPartition, nowMs);
                        }

                        if (_ttlIndex != null && filteredBatches.Count > 0 &&
                            IsTtlEnabled(topic) && _ttlIndex.HasTtlRecords(topicPartition))
                        {
                            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                            filteredBatches = TtlFilter.FilterExpiredBatches(filteredBatches, _ttlIndex, topicPartition, nowMs);
                        }

                        recordSet = _recordBatchSerializer.CombineBatches(filteredBatches);

                        messageCount = 0;
                        foreach (var b in filteredBatches)
                            messageCount += CompressionCodec.GetRecordCount(b);
                    }

                    BatchesCombined(0, recordSet.Length);
                    LogOffsets(log?.LogStartOffset ?? 0, highWatermark);

                    totalBytesFetched += recordSet.Length;
                    _metrics?.RecordFetch(topic, partitionData.Partition, messageCount, recordSet.Length, 0);

                    partitionResponses.Add(new FetchResponse.PartitionResponse
                    {
                        Partition = partitionData.Partition,
                        ErrorCode = ErrorCode.None,
                        HighWatermark = highWatermark,
                        LogStartOffset = log?.LogStartOffset ?? 0,
                        RecordSet = recordSet
                    });

                    // Observability tap — emit one Consumed event per
                    // partition fetch that actually returned records.
                    // Empty fetches (poll-with-no-data) would be noise
                    // in the tap stream. HasSubscribers gates allocation
                    // so fetches on an unobserved broker pay nothing.
                    // Payload bytes aren't forwarded for the same
                    // hot-path reason as Produced.
                    if (messageCount > 0 && _observability?.HasSubscribers == true)
                    {
                        _observability.Publish(new SurgewaveBrokerEvent(
                            SurgewaveBrokerEventKind.Consumed,
                            topic, partitionData.Partition, partitionData.FetchOffset,
                            Principal: connectionState.AuthenticatedUser,
                            RejectReason: null,
                            Consumers: null,
                            Key: null, Value: null,
                            Timestamp: DateTimeOffset.UtcNow));
                    }
                }
                catch (Exception ex)
                {
                    FetchError(ex, topic, partitionData.Partition);
                    _metrics?.RecordError(ErrorCode.Unknown.ToString());

                    partitionResponses.Add(new FetchResponse.PartitionResponse
                    {
                        Partition = partitionData.Partition,
                        ErrorCode = ErrorCode.Unknown,
                        HighWatermark = 0,
                        RecordSet = []
                    });
                }
            }

            responses.Add(new FetchResponse.FetchableTopicResponse
            {
                Topic = topic,
                TopicId = topicRequest.TopicId,
                Partitions = partitionResponses
            });
        }

        // Record fetched bytes for quota tracking (after successful fetch)
        _quotaManager.RecordFetchedBytes(clientId, totalBytesFetched);

        // Record actual bytes fetched for bandwidth quota (not the max requested)
        if (_bandwidthQuotaManager is { Enabled: true } && totalBytesFetched > 0)
        {
            _bandwidthQuotaManager.RecordConsume(clientId, totalBytesFetched);
        }

        return new FetchResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Responses = responses
        };
    }

    /// <summary>
    /// Filter record batches based on isolation level using TransactionIndex.
    /// For READ_COMMITTED: filter out control batches, uncommitted transactional batches, and aborted batches.
    /// For READ_UNCOMMITTED: only filter out control batches (transaction markers).
    /// </summary>
    private List<byte[]> FilterBatchesForIsolationLevel(
        TopicPartition partition,
        List<byte[]> batches,
        bool isReadCommitted,
        long highWatermark)
    {
        if (isReadCommitted)
        {
            return _transactionCoordinator.FilterForReadCommitted(partition, batches, highWatermark);
        }
        else
        {
            return _transactionCoordinator.FilterForReadUncommitted(batches);
        }
    }

    private ListOffsetsResponse HandleListOffsets(ListOffsetsRequest request)
    {
        var topics = new List<TopicPartitionOffsets>();

        foreach (var topicRequest in request.Topics)
        {
            var partitions = new List<PartitionOffsetInfo>();

            foreach (var partitionRequest in topicRequest.Partitions)
            {
                try
                {
                    var topicPartition = new TopicPartition
                    {
                        Topic = topicRequest.Topic,
                        Partition = partitionRequest.PartitionIndex
                    };

                    long offset;
                    long timestamp = partitionRequest.Timestamp;
                    var log = _logManager.GetLog(topicPartition);

                    // Special timestamps per Apache Kafka ListOffsetsRequest constants:
                    //   -1 LATEST_TIMESTAMP                     (next offset to be written)
                    //   -2 EARLIEST_TIMESTAMP                   (LogStartOffset)
                    //   -3 MAX_TIMESTAMP            (KIP-734)    (offset whose record has the max timestamp)
                    //   -4 EARLIEST_LOCAL_TIMESTAMP (KIP-1059)  (start of local log; same as -2 when no tiered tier is in front)
                    //   -5 LATEST_TIERED_TIMESTAMP  (KIP-405)   (last offset that has been uploaded to remote storage)
                    //   -6 EARLIEST_PENDING_UPLOAD  (KIP-1023)  (start of segments still waiting to upload)
                    // Surgewave's broker-internal tiering keeps a single LogStartOffset, so
                    // the broker-public surface treats EARLIEST and EARLIEST_LOCAL the
                    // same and reports -1 for the tiered-only offsets when no tier is
                    // active. The wire contract is satisfied — clients that only need
                    // the local view (KIP-1059's reason for existing) get the right
                    // answer; tiered-aware tooling can probe -5 / -6 and gracefully
                    // fall back to -2 when the response is -1.
                    offset = ResolveListOffsetTimestamp(log, timestamp);

                    partitions.Add(new PartitionOffsetInfo
                    {
                        PartitionIndex = partitionRequest.PartitionIndex,
                        ErrorCode = ErrorCode.None,
                        Timestamp = timestamp,
                        Offset = offset
                    });
                }
                catch
                {
                    partitions.Add(new PartitionOffsetInfo
                    {
                        PartitionIndex = partitionRequest.PartitionIndex,
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        Timestamp = -1,
                        Offset = -1
                    });
                }
            }

            topics.Add(new TopicPartitionOffsets
            {
                Topic = topicRequest.Topic,
                Partitions = partitions
            });
        }

        return new ListOffsetsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Topics = topics
        };
    }

    /// <summary>
    /// Resolves a ListOffsets-style special timestamp to a concrete log offset.
    /// Handles every reserved value documented in
    /// <see cref="ListOffsetsRequest.TimestampType"/> plus a positive timestamp
    /// look-up via <see cref="IPartitionLog.FindOffsetByTimestamp"/>. Pulled out
    /// into a static helper so unit tests can exercise the timestamp matrix
    /// without constructing the full <see cref="DataApiHandler"/> dependency
    /// graph.
    /// </summary>
    internal static long ResolveListOffsetTimestamp(IPartitionLog? log, long timestamp)
    {
        if (timestamp == ListOffsetsRequest.TimestampType.Latest)
        {
            return log?.NextOffset ?? 0;
        }

        if (timestamp == ListOffsetsRequest.TimestampType.Earliest
            || timestamp == ListOffsetsRequest.TimestampType.EarliestLocalTimestamp)
        {
            // EARLIEST or EARLIEST_LOCAL (KIP-1059) — both map to LogStartOffset on
            // a non-tiered broker; on a tiered broker the local tier shares the
            // same start-of-log boundary.
            return log?.LogStartOffset ?? 0;
        }

        if (timestamp == ListOffsetsRequest.TimestampType.MaxTimestamp)
        {
            // KIP-734: find the offset whose record carries the largest timestamp.
            return log?.FindOffsetByTimestamp(long.MaxValue) ?? -1;
        }

        if (timestamp == ListOffsetsRequest.TimestampType.LastTieredOffset
            || timestamp == ListOffsetsRequest.TimestampType.EarliestPendingUploadOffset)
        {
            // KIP-1005 / KIP-1023: tiered-storage probes. Surgewave doesn't expose the
            // broker-internal tier through this RPC, so clients see -1 (offset not
            // available). Tiered-aware admin tools detect tiered-storage capability
            // via the API-versions response before they ask and degrade gracefully.
            return -1;
        }

        // Positive timestamp → OffsetsForTimes look-up.
        if (log == null) return 0;
        return log.FindOffsetByTimestamp(timestamp) ?? log.NextOffset;
    }

    /// <summary>
    /// Check if deduplication is enabled for a topic.
    /// Requires global deduplication enabled AND topic-level opt-in via config.
    /// </summary>
    private bool IsDeduplicationEnabled(string topic)
    {
        if (!_config.DeduplicationEnabled)
            return false;

        var metadata = _logManager.GetTopicMetadata(topic);
        return metadata?.Config.TryGetValue("surgewave.dedup.enabled", out var val) == true
            && string.Equals(val, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Check if TTL is enabled for a topic.
    /// Requires global TTL enabled AND topic-level opt-in via config.
    /// </summary>
    private bool IsTtlEnabled(string topic)
    {
        if (!_config.TtlEnabled)
            return false;

        var metadata = _logManager.GetTopicMetadata(topic);
        return metadata?.Config.TryGetValue("surgewave.ttl.enabled", out var val) == true
            && string.Equals(val, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Check if delayed delivery is enabled for a topic.
    /// Requires global delay delivery enabled AND topic-level opt-in via config.
    /// </summary>
    private bool IsDelayDeliveryEnabled(string topic)
    {
        if (!_config.DelayDeliveryEnabled)
            return false;

        var metadata = _logManager.GetTopicMetadata(topic);
        return metadata?.Config.TryGetValue("surgewave.delay.enabled", out var val) == true
            && string.Equals(val, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Check if the current connection is authorized to perform an operation on a topic
    /// </summary>
    private bool AuthorizeTopic(ConnectionState connectionState, string topic, AclOperation operation)
    {
        // If ACL is not enabled, allow all operations
        if (_aclAuthorizer == null)
        {
            return true;
        }

        // Get principal from connection state (authenticated user)
        // Use "User:anonymous" for unauthenticated connections
        var principal = connectionState.IsAuthenticated
            ? $"User:{connectionState.AuthenticatedUser}"
            : "User:anonymous";

        var result = _aclAuthorizer.Authorize(
            principal,
            connectionState.ClientHost,
            AclResourceType.Topic,
            topic,
            operation);

        return result.IsAllowed;
    }

    // Source-generated high-performance logging (relocated from the broker's shared Log class
    // in #59 b4-tier2; kept as instance [LoggerMessage] methods over the _logger field, matching
    // the sibling Kafka handlers).
    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored RecordBatch for {Topic}-{Partition}, baseOffset={BaseOffset}, size={Size} bytes")]
    private partial void RecordBatchStored(string topic, int partition, long baseOffset, int size);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error producing to {Topic}-{Partition}")]
    private partial void ProduceError(Exception ex, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Read {BatchCount} batches from {Topic}-{Partition} at offset {FetchOffset}")]
    private partial void BatchesRead(int batchCount, string topic, int partition, long fetchOffset);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Combined {BatchCount} batches into {RecordSetSize} bytes")]
    private partial void BatchesCombined(int batchCount, int recordSetSize);

    [LoggerMessage(Level = LogLevel.Trace, Message = "LogStartOffset={LogStartOffset}, HighWatermark={HighWatermark}")]
    private partial void LogOffsets(long logStartOffset, long highWatermark);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error fetching from {Topic}-{Partition}")]
    private partial void FetchError(Exception ex, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Trace, Message = "[FetchDebug] {Topic}-{Partition} fetchOffset={FetchOffset}, logStartOffset={LogStartOffset}, nextOffset={NextOffset}, logExists={LogExists}")]
    private partial void FetchDebug(string topic, int partition, long fetchOffset, long logStartOffset, long nextOffset, bool logExists);
}
