using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Broker.Abstractions.Routing;
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
    private IPartitionLeadership? _partitionLeadership;
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

    /// <summary>
    /// Ceiling for decompressing a producer-supplied batch, taken from what the topic will accept.
    /// </summary>
    /// <remarks>
    /// The bound belongs to the destination, not to a constant: a batch that could not be stored is
    /// not worth expanding in order to read a header out of it. Only reached on topics with TTL or
    /// delayed delivery enabled — an ordinary produce never decompresses at all.
    /// </remarks>
    private long MaxDecompressedBytes(TopicMetadata? topicMetadata)
        => topicMetadata is null
            ? DefaultMaxDecompressedBytes
            : ConfigParser.GetMaxMessageBytes(topicMetadata.Config, DefaultMaxDecompressedBytes);

    private const long DefaultMaxDecompressedBytes = 1024 * 1024;

    private IPartitionCommitGate? _commitGate;

    /// <summary>
    /// Supplies the durability gate consulted for acks=all. Left unset on a broker without
    /// replication, where every write a partition accepts is as durable as that broker gets.
    /// </summary>
    /// <summary>
    /// Supplies the leadership view once clustering exists (#164).
    /// </summary>
    /// <remarks>
    /// A setter for the same reason SetCommitGate is one: the handler is built before
    /// clustering. Left unset — a single-broker or embedded runtime — the produce path
    /// keeps appending exactly as before, which is the behaviour those deployments need.
    /// </remarks>
    public void SetPartitionLeadership(IPartitionLeadership? leadership)
        => _partitionLeadership = leadership;

    public void SetCommitGate(IPartitionCommitGate? commitGate) => _commitGate = commitGate;

    /// <summary>
    /// Kafka defines acks as -1, 0 or 1; anything else is a malformed request and nothing is
    /// written. Kept out of <c>ProduceRequest.ReadFrom</c> deliberately — the parse path is
    /// benchmark-gated and this is a protocol decision, not a decoding one.
    /// </summary>
    private static ProduceResponse InvalidAcksResponse(ProduceRequest request)
    {
        var responses = new List<ProduceResponse.TopicProduceResponse>(request.TopicData.Count);

        foreach (var topicData in request.TopicData)
        {
            var partitionResponses = new List<ProduceResponse.PartitionProduceResponse>(topicData.PartitionData.Count);
            foreach (var partitionData in topicData.PartitionData)
            {
                partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                {
                    Index = partitionData.Index,
                    ErrorCode = ErrorCode.InvalidRequiredAcks,
                    BaseOffset = -1,
                    LogAppendTimeMs = -1
                });
            }

            responses.Add(new ProduceResponse.TopicProduceResponse
            {
                Name = topicData.Name ?? string.Empty,
                TopicId = topicData.TopicId,
                PartitionResponses = partitionResponses
            });
        }

        return new ProduceResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            Responses = responses,
            ThrottleTimeMs = 0
        };
    }

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
        // The acks decision is made ONCE per request, not per partition: acks=1 and acks=0 must pay
        // no more than this comparison on a field the parser wrote microseconds ago, and must then
        // run exactly the code they ran before. `commitGate` stays null for them, so nothing in the
        // partition loop is dereferenced.
        var acks = request.RequiredAcks;
        if ((uint)(acks + 1) > 2)
            return InvalidAcksResponse(request);

        var commitGate = acks == -1 ? _commitGate : null;

        // Collected across ALL topics and awaited once at the end, not per partition inside the
        // loop. Awaiting inline would serialise replication latency over the partitions of a
        // request, turning a 5 ms round trip into 5 ms x partition count. Stays null for
        // acks=0/1, so those pay one null check.
        List<PendingDurableCommit>? durabilityWaits = null;

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
                    // The bytes exactly as the producer sent them.
                    //
                    // Named because after the record transform below there are TWO buffers in this
                    // scope, and every call site silently picks one:
                    //
                    //   producerRecords  — what the CLIENT sent. Read ONLY before the transform:
                    //                      framing, declared codec, idempotence identity, the dedup
                    //                      hash.
                    //   recordsToAppend  — what goes INTO THE LOG. Everything after the transform,
                    //                      without exception.
                    //
                    // That is one rule, not a decision per call site, and it holds because anything
                    // needed from the client's bytes afterwards is extracted BEFORE the transform as
                    // a value — the producer id, the sequence, the dedup hash — never carried along
                    // as a second buffer. The last such carry was dedup, and it was also the one
                    // that got it wrong.
                    //
                    // Kafka has no equivalent hazard: no broker-side transform at all, and where
                    // LogValidator does rewrite bytes it returns them as ValidationResult while the
                    // original stays private to the validator, so nothing downstream can see both.
                    var producerRecords = partitionData.Records;

                    // Kafka permits exactly ONE record batch per partition in a produce request and
                    // enforces it at parse time — ProduceRequest.validateRecords throws
                    // InvalidRecordException("only allowed to contain exactly one record batch per
                    // partition") before any per-partition handling runs. Refusing here, first,
                    // mirrors that placement.
                    //
                    // Until now such a section fell through to the append, where the validating CRC
                    // is computed over the WHOLE section and cannot match the first batch's CRC
                    // field, so the producer was told CorruptMessage. That answer blames transport
                    // for a request the protocol does not permit, and it sent anyone debugging it
                    // looking for a network fault. Nothing was ever written — the refusal was
                    // correct, only its reason was wrong.
                    if (!IsSingleRecordBatch(producerRecords.Span))
                    {
                        partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                        {
                            Index = partitionData.Index,
                            ErrorCode = ErrorCode.InvalidRecord,
                            BaseOffset = -1,
                            LogAppendTimeMs = -1
                        });
                        continue;
                    }

                    // Check for unsupported compression before storing
                    var compressionType = CompressionCodec.GetCompressionTypeFromBatch(producerRecords.Span);
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

                    // A produce for a partition another broker leads is refused rather than
                    // appended (#164). Without this a client whose cached metadata is stale
                    // writes to the old leader and is told nothing, so the records exist
                    // where nobody will ever read them. Kafka answers the same way, from the
                    // equally local leaderLogIfLocal.
                    //
                    // Only when we positively KNOW someone else leads: a single-broker or
                    // embedded runtime has no partition states at all, and must keep working.
                    if (_partitionLeadership?.IsLedByAnotherBroker(topicPartition) == true)
                    {
                        partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                        {
                            Index = partitionData.Index,
                            ErrorCode = ErrorCode.NotLeaderForPartition,
                            BaseOffset = -1,
                            LogAppendTimeMs = -1
                        });
                        continue;
                    }

                    // Durability admission, BEFORE the idempotence validation below — that call
                    // advances the producer's sequence, and advancing it for a batch we then refuse
                    // to write poisons the producer: its retry carries the same sequence and comes
                    // back as a non-retriable DuplicateSequenceNumber. Refusing here means the write
                    // did not happen at all, which is exactly what the client must be told.
                    if (commitGate is not null && !commitGate.CanAdmitDurableWrite(topicPartition))
                    {
                        partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                        {
                            Index = partitionData.Index,
                            ErrorCode = ErrorCode.NotEnoughReplicas,
                            BaseOffset = -1,
                            LogAppendTimeMs = -1
                        });
                        continue;
                    }

                    // Extract idempotence info and validate if present
                    var (producerId, producerEpoch, baseSequence, lastOffsetDelta) =
                        CompressionCodec.GetIdempotenceInfo(producerRecords.Span);

                    if (producerId != KafkaConstants.Producer.NoProducerId)
                    {
                        var check = _transactionCoordinator.ValidateProduceBatch(
                            producerId, producerEpoch, baseSequence, lastOffsetDelta, topicPartition);

                        // A retransmit of a batch we already wrote is answered with SUCCESS and the
                        // offset it landed at, which is the whole point of idempotent delivery: the
                        // producer sent it twice because it never saw the first acknowledgement, and
                        // a duplicate-sequence error would be fatal to it. Only a batch we cannot
                        // place at all is an error.
                        if (check.Status == ProduceSequenceStatus.DuplicateSequence &&
                            check.DuplicateBaseOffset >= 0)
                        {
                            partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                            {
                                Index = partitionData.Index,
                                ErrorCode = ErrorCode.None,
                                BaseOffset = check.DuplicateBaseOffset,
                                LogAppendTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            });
                            continue;
                        }

                        if (check.Status != ProduceSequenceStatus.Ok)
                        {
                            // Map the neutral sequence-validation status to the Kafka wire
                            // error code at the protocol boundary (part-c TxnErrorStatus pattern).
                            partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                            {
                                Index = partitionData.Index,
                                ErrorCode = check.Status switch
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

                    // Content-based deduplication check (if enabled for this topic).
                    //
                    // The hash is carried past the record transform instead of the bytes. That is
                    // what removes the choice further down: the registration after the append needs
                    // an offset, which only exists then, but it must describe the bytes checked
                    // HERE. Passing the buffer across that gap is what made the two ends disagree.
                    ulong dedupHash = 0;
                    if (_deduplicationManager != null && IsDeduplicationEnabled(topic))
                    {
                        var dedupResult = _deduplicationManager.CheckDuplicate(topicPartition, producerRecords.Span);
                        dedupHash = dedupResult.ContentHash;
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
                    var recordsToAppend = producerRecords;
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

                    // The batch is in the log — only NOW does the producer's sequence advance. Doing
                    // it during validation would strand an idempotent producer whenever anything
                    // between the two refused the write (durability admission, quota, a corrupt
                    // payload, a failing append): its retry would collide with a sequence recorded
                    // for a batch that was never written.
                    if (producerId != KafkaConstants.Producer.NoProducerId)
                    {
                        _transactionCoordinator.CommitProduceBatch(
                            producerId, producerEpoch, baseSequence, lastOffsetDelta, topicPartition, baseOffset);
                    }

                    // Register the hash the check produced. Zero means either dedup is off for
                    // this topic or the batch was too small to hash, and Register ignores it — so
                    // a dedup-disabled topic no longer pays a hash pass and no longer fills a
                    // window nobody reads, which it did before.
                    _deduplicationManager?.Register(topicPartition, dedupHash, baseOffset);

                    // Extract and index delayed delivery headers (if enabled for this topic)
                    if (_delayIndex != null && IsDelayDeliveryEnabled(topic))
                    {
                        // This index maps baseOffset to a delivery time, and what lives at
                        // baseOffset is what was appended — a transform that rewrites or drops
                        // headers made the index describe records the log does not hold.
                        var deliverAtMs = DelayHeaderParser.ExtractDeliverAtTimestamp(
                            recordsToAppend.Span, MaxDecompressedBytes(topicMetadata));
                        if (deliverAtMs.HasValue)
                        {
                            _delayIndex.RecordDelayedBatch(topicPartition, baseOffset, deliverAtMs.Value);
                        }
                    }

                    // Extract and index TTL headers (if enabled for this topic)
                    if (_ttlIndex != null && IsTtlEnabled(topic))
                    {
                        // Same reasoning as the delay index above: the expiry belongs to the
                        // records that were actually stored at baseOffset.
                        var expiryMs = TtlHeaderParser.ExtractExpiryTimestamp(
                            recordsToAppend.Span, MaxDecompressedBytes(topicMetadata));
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
                    // recordsToAppend from here on, without exception: everything below describes
                    // the records that were stored at baseOffset. The last-stable-offset anchor and
                    // the produce metrics are statements about the log, not about what arrived.
                    if (CompressionCodec.IsTransactional(recordsToAppend.Span) &&
                        !CompressionCodec.IsControlBatch(recordsToAppend.Span))
                    {
                        _transactionCoordinator.RecordTransactionalBatch(topicPartition, producerId, baseOffset);
                    }

                    RecordBatchStored(topic, partitionData.Index, baseOffset, recordsToAppend.Length);

                    // Record produce metrics
                    var recordCount = CompressionCodec.GetRecordCount(recordsToAppend.Span);
                    _metrics?.RecordProduce(topic, partitionData.Index, recordCount, recordsToAppend.Length, 0);
                    _coldStartProfiler?.RecordProduce(topic, recordCount, recordsToAppend.Length);

                    partitionResponses.Add(new ProduceResponse.PartitionProduceResponse
                    {
                        Index = partitionData.Index,
                        ErrorCode = ErrorCode.None,
                        BaseOffset = baseOffset,
                        LogAppendTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    });

                    // acks=all: the record is in the LEADER's log, which is not what the producer
                    // asked for. Note where it has to get to, and hold the success answer until the
                    // in-sync replicas have it (or the request times out). The offset is exclusive
                    // — base + count — because the high watermark counts the next offset to commit.
                    if (commitGate is not null)
                    {
                        durabilityWaits ??= [];
                        durabilityWaits.Add(new PendingDurableCommit(
                            partitionResponses,
                            partitionResponses.Count - 1,
                            topicPartition,
                            baseOffset + produceRecordCount));
                    }

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

        if (durabilityWaits is not null)
        {
            await AwaitDurableCommitsAsync(durabilityWaits, commitGate!, request.TimeoutMs, cancellationToken)
                .ConfigureAwait(false);
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

    /// <summary>
    /// Whether <paramref name="section"/> holds exactly one Kafka v2 RecordBatch and nothing else.
    /// </summary>
    /// <remarks>
    /// <para>Two integer reads, no walk over the records. This sits on the produce hot path, where
    /// the answer is "yes" for every conforming client, so it must not cost a pass over the batch:
    /// the first batch's own length field already says whether anything follows it.</para>
    ///
    /// <para>Written as a subtraction rather than <c>12 + batchLength == section.Length</c> on
    /// purpose — the addition overflows for a hostile length near <see cref="int.MaxValue"/>, and
    /// the subtraction cannot. A section too short to hold a header, or one whose first batch
    /// overruns it, is not a single batch either and is refused the same way.</para>
    /// </remarks>
    private static bool IsSingleRecordBatch(ReadOnlySpan<byte> section)
    {
        if (section.Length < KafkaConstants.RecordBatch.HeaderSize)
            return false;

        // Layout: BaseOffset int64 @0, BatchLength int32 @8 counting every byte after itself.
        var batchLength = BinaryPrimitives.ReadInt32BigEndian(section.Slice(8, 4));

        return batchLength >= 0 && section.Length - 12 == batchLength;
    }

    /// <summary>
    /// A write that is in the leader's log and still owes the producer proof that the in-sync
    /// replicas have it. Holds the response slot so the verdict can be written back in place —
    /// <c>TopicProduceResponse.PartitionResponses</c> keeps the very list instance built during the
    /// partition loop, so patching the entry patches the response.
    /// </summary>
    private readonly record struct PendingDurableCommit(
        List<ProduceResponse.PartitionProduceResponse> Bucket,
        int Index,
        TopicPartition Partition,
        long CommittedThroughOffset);

    /// <summary>
    /// Waits for every acks=all write of one request concurrently, and downgrades the ones that did
    /// not replicate in time to <see cref="ErrorCode.NotEnoughReplicasAfterAppend"/>.
    /// </summary>
    /// <remarks>
    /// That error code is the honest one: unlike <see cref="ErrorCode.NotEnoughReplicas"/>, which
    /// means the write never happened, it tells the producer the append DID happen but replication
    /// was not confirmed — so a retry may duplicate unless the producer is idempotent. Reporting
    /// success here instead is the bug this method exists to fix.
    /// </remarks>
    private static async Task AwaitDurableCommitsAsync(
        List<PendingDurableCommit> waits,
        IPartitionCommitGate commitGate,
        int requestTimeoutMs,
        CancellationToken cancellationToken)
    {
        // A producer may send timeout 0 ("no bound"); clamp to something finite so a dead follower
        // cannot pin the connection's request pipeline indefinitely.
        var timeout = requestTimeoutMs > 0
            ? TimeSpan.FromMilliseconds(requestTimeoutMs)
            : DefaultDurabilityWaitTimeout;

        var tasks = new Task<bool>[waits.Count];
        for (var i = 0; i < waits.Count; i++)
        {
            var w = waits[i];
            tasks[i] = commitGate
                .WaitForDurableCommitAsync(w.Partition, w.CommittedThroughOffset, timeout, cancellationToken)
                .AsTask();
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);

        for (var i = 0; i < waits.Count; i++)
        {
            if (results[i])
                continue;

            var w = waits[i];
            var original = w.Bucket[w.Index];

            // PartitionProduceResponse is a class, not a record, so this is a rebuild rather than a
            // `with`. BaseOffset goes to -1: the batch is in the leader's log at the offset we were
            // about to report, but telling the producer an offset alongside an error would invite
            // it to treat the write as placed.
            w.Bucket[w.Index] = new ProduceResponse.PartitionProduceResponse
            {
                Index = original.Index,
                ErrorCode = ErrorCode.NotEnoughReplicasAfterAppend,
                BaseOffset = -1,
                LogAppendTimeMs = original.LogAppendTimeMs,
                LogStartOffset = original.LogStartOffset,
                CurrentLeader = original.CurrentLeader
            };
        }
    }

    private static readonly TimeSpan DefaultDurabilityWaitTimeout = TimeSpan.FromSeconds(30);

    private async Task<FetchResponse> HandleFetchAsync(FetchRequest request, ConnectionState connectionState, CancellationToken cancellationToken)
    {
        var responses = new List<FetchResponse.FetchableTopicResponse>(request.Topics.Count);

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

        // The response object exists before the fetch runs so borrowed reads can be attached to it
        // as they happen (#78): a record set is served straight out of the storage lease, and that
        // lease has to stay alive until the response has been serialized. Everything the response
        // needs is already final here — the throttle time is computed above and no longer changes,
        // and the partition results are appended through the `responses` reference.
        var response = new FetchResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = throttleTimeMs,
            Responses = responses
        };

        long totalBytesFetched;
        try
        {
            totalBytesFetched = await FetchTopicsIntoAsync(request, connectionState, response, cancellationToken);
        }
        catch
        {
            // Nobody downstream will ever see this response, so any lease already attached to it
            // would keep its pool buffer or mapped view for good.
            response.ReleaseBorrowedMemory();
            throw;
        }

        // Record fetched bytes for quota tracking (after successful fetch)
        _quotaManager.RecordFetchedBytes(clientId, totalBytesFetched);

        // Record actual bytes fetched for bandwidth quota (not the max requested)
        if (_bandwidthQuotaManager is { Enabled: true } && totalBytesFetched > 0)
        {
            _bandwidthQuotaManager.RecordConsume(clientId, totalBytesFetched);
        }

        return response;
    }

    /// <summary>
    /// Runs the per-partition fetch for every requested topic and appends the results to
    /// <paramref name="response"/>, attaching the storage lease of every borrowed read to it.
    ///
    /// <para>Split out from <see cref="HandleFetchAsync"/> so those leases have exactly one owner:
    /// on the way out — normally or by exception — the response is the single thing that has to be
    /// released to give all of them back (#78).</para>
    /// </summary>
    /// <returns>Total record-set bytes served, for quota accounting.</returns>
    private async Task<long> FetchTopicsIntoAsync(
        FetchRequest request,
        ConnectionState connectionState,
        FetchResponse response,
        CancellationToken cancellationToken)
    {
        var responses = response.Responses;
        var isReadCommitted = request.IsolationLevel == FetchRequest.ReadCommitted;
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
                        RecordSet = ReadOnlyMemory<byte>.Empty
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

                    ReadOnlyMemory<byte> recordSet;
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
                            recordSet = disagRead.LogBytes;   // already ReadOnlyMemory; no copy (#78)
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
                        // Fast path: contiguous read — no per-batch allocation, and no payload copy
                        // either: the engine hands its pooled or memory-mapped buffer over as a
                        // lease. The lease outlives this scope because the response is serialized
                        // later, so its ownership moves to the response — which is why the read
                        // itself is not disposed here (#78).
                        var read = await _logManager.ReadContiguousAsync(
                            topicPartition, partitionData.FetchOffset,
                            maxBytes: partitionData.MaxBytes, cancellationToken);

                        if (read.Lifetime is { } lease)
                            response.AttachLifetime(lease);

                        var contiguousData = read.Data;
                        var batchOffsets = read.BatchOffsets;

                        BatchesRead(batchOffsets.Count, topic, partitionData.Partition, partitionData.FetchOffset);

                        recordSet = contiguousData;

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
                        RecordSet = ReadOnlyMemory<byte>.Empty
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

        return totalBytesFetched;
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
