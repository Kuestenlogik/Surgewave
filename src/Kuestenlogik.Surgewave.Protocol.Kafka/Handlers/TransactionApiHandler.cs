using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handler for the transaction-coordinator APIs: InitProducerId, AddPartitionsToTxn,
/// AddOffsetsToTxn, TxnOffsetCommit, EndTxn, DescribeProducers, DescribeTransactions, ListTransactions.
/// This is the Kafka-DTO &lt;-&gt; neutral ADAPTER for <see cref="ITransactionCoordinator"/> (#59):
/// it decodes each wire request into a protocol-neutral command, calls the coordinator, and
/// re-encodes the neutral result. It owns the wire envelope (CorrelationId/ApiVersion/ThrottleTimeMs)
/// and the neutral-status -> ErrorCode mapping. The coordinator references no Kafka type for its
/// wire-API surface; this is the piece that moves into the Kafka protocol plugin later.
/// </summary>
public sealed class TransactionApiHandler : IKafkaRequestHandler
{
    private readonly ITransactionCoordinator _coordinator;
    private readonly ILogger<TransactionApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.InitProducerId,
        ApiKey.AddPartitionsToTxn,
        ApiKey.AddOffsetsToTxn,
        ApiKey.TxnOffsetCommit,
        ApiKey.EndTxn,
        ApiKey.DescribeProducers,
        ApiKey.DescribeTransactions,
        ApiKey.ListTransactions,
    ];

    public TransactionApiHandler(
        ITransactionCoordinator coordinator,
        ILogger<TransactionApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            InitProducerIdRequest r => ToInitProducerIdResponse(await _coordinator.InitProducerIdAsync(ToInitProducerIdCommand(r), cancellationToken), r),
            AddPartitionsToTxnRequest r => ToAddPartitionsToTxnResponse(_coordinator.AddPartitionsToTxn(ToAddPartitionsToTxnCommand(r)), r),
            AddOffsetsToTxnRequest r => ToAddOffsetsToTxnResponse(_coordinator.AddOffsetsToTxn(ToAddOffsetsToTxnCommand(r)), r),
            TxnOffsetCommitRequest r => ToTxnOffsetCommitResponse(_coordinator.TxnOffsetCommit(ToTxnOffsetCommitCommand(r)), r),
            EndTxnRequest r => ToEndTxnResponse(await _coordinator.EndTxnAsync(ToEndTxnCommand(r), cancellationToken), r),
            DescribeProducersRequest r => ToDescribeProducersResponse(_coordinator.DescribeProducers(ToDescribeProducersCommand(r)), r),
            DescribeTransactionsRequest r => ToDescribeTransactionsResponse(_coordinator.DescribeTransactions(r.TransactionalIds), r),
            ListTransactionsRequest r => ToListTransactionsResponse(
                _coordinator.ListTransactions(r.StateFilters, r.ProducerIdFilters, r.DurationFilter, r.TransactionalIdPattern), r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by TransactionApiHandler")
        };
    }

    /// <summary>
    /// Maps a neutral transaction outcome onto the Kafka wire error code. Public so the gRPC admin
    /// wiring can reuse the exact same mapping instead of duplicating it.
    /// </summary>
    public static ErrorCode ToErrorCode(TxnErrorStatus status) => status switch
    {
        TxnErrorStatus.None => ErrorCode.None,
        TxnErrorStatus.InvalidProducerEpoch => ErrorCode.InvalidProducerEpoch,
        TxnErrorStatus.InvalidTxnState => ErrorCode.InvalidTxnState,
        TxnErrorStatus.UnknownProducerId => ErrorCode.UnknownProducerId,
        TxnErrorStatus.UnknownTopicId => ErrorCode.UnknownTopicId,
        _ => ErrorCode.None,
    };

    // ── InitProducerId ─────────────────────────────────────────────────────

    private static InitProducerIdCommand ToInitProducerIdCommand(InitProducerIdRequest r)
        => new()
        {
            TransactionalId = r.TransactionalId,
            TransactionTimeoutMs = r.TransactionTimeoutMs,
            ProducerId = r.ProducerId,
            ProducerEpoch = r.ProducerEpoch,
        };

    private static InitProducerIdResponse ToInitProducerIdResponse(InitProducerIdResult result, InitProducerIdRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ToErrorCode(result.Status),
            ProducerId = result.ProducerId,
            ProducerEpoch = result.ProducerEpoch,
        };

    // ── AddPartitionsToTxn ─────────────────────────────────────────────────

    private static AddPartitionsToTxnCommand ToAddPartitionsToTxnCommand(AddPartitionsToTxnRequest r)
    {
        var topics = new List<AddPartitionsTopic>(r.Topics.Count);
        foreach (var (topic, partitions) in r.Topics)
        {
            topics.Add(new AddPartitionsTopic(topic, partitions));
        }
        return new AddPartitionsToTxnCommand
        {
            TransactionalId = r.TransactionalId,
            ProducerId = r.ProducerId,
            ProducerEpoch = r.ProducerEpoch,
            Topics = topics,
        };
    }

    private static AddPartitionsToTxnResponse ToAddPartitionsToTxnResponse(AddPartitionsToTxnResult result, AddPartitionsToTxnRequest r)
    {
        var results = new Dictionary<string, List<AddPartitionsToTxnResponse.PartitionResult>>();
        foreach (var topic in result.Topics)
        {
            var partitions = new List<AddPartitionsToTxnResponse.PartitionResult>(topic.Partitions.Count);
            foreach (var p in topic.Partitions)
            {
                partitions.Add(new AddPartitionsToTxnResponse.PartitionResult { Partition = p.Partition, ErrorCode = ToErrorCode(p.Status) });
            }
            results[topic.Topic] = partitions;
        }

        return new AddPartitionsToTxnResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            Results = results,
        };
    }

    // ── AddOffsetsToTxn ────────────────────────────────────────────────────

    private static AddOffsetsToTxnCommand ToAddOffsetsToTxnCommand(AddOffsetsToTxnRequest r)
        => new()
        {
            TransactionalId = r.TransactionalId,
            ProducerId = r.ProducerId,
            ProducerEpoch = r.ProducerEpoch,
            GroupId = r.GroupId,
        };

    private static AddOffsetsToTxnResponse ToAddOffsetsToTxnResponse(AddOffsetsToTxnResult result, AddOffsetsToTxnRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ToErrorCode(result.Status),
        };

    // ── TxnOffsetCommit ────────────────────────────────────────────────────

    private static TxnOffsetCommitCommand ToTxnOffsetCommitCommand(TxnOffsetCommitRequest r)
    {
        var topics = new List<TxnOffsetCommitTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            var partitions = new List<TxnOffsetCommitPartition>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new TxnOffsetCommitPartition(p.Partition, p.CommittedOffset, p.Metadata));
            }
            topics.Add(new TxnOffsetCommitTopic { Name = t.Name, TopicId = t.TopicId, Partitions = partitions });
        }
        return new TxnOffsetCommitCommand
        {
            TransactionalId = r.TransactionalId,
            GroupId = r.GroupId,
            ProducerId = r.ProducerId,
            ProducerEpoch = r.ProducerEpoch,
            Topics = topics,
        };
    }

    private static TxnOffsetCommitResponse ToTxnOffsetCommitResponse(TxnOffsetCommitResult result, TxnOffsetCommitRequest r)
    {
        var topics = new List<TxnOffsetCommitResponse.TxnOffsetCommitTopicResult>(result.Topics.Count);
        foreach (var t in result.Topics)
        {
            var partitions = new List<TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult>(t.Partitions.Count);
            foreach (var p in t.Partitions)
            {
                partitions.Add(new TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult { Partition = p.Partition, ErrorCode = ToErrorCode(p.Status) });
            }
            topics.Add(new TxnOffsetCommitResponse.TxnOffsetCommitTopicResult { Name = t.Name, TopicId = t.TopicId, Partitions = partitions });
        }

        return new TxnOffsetCommitResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            Topics = topics,
        };
    }

    // ── EndTxn ─────────────────────────────────────────────────────────────

    private static EndTxnCommand ToEndTxnCommand(EndTxnRequest r)
        => new()
        {
            TransactionalId = r.TransactionalId,
            ProducerId = r.ProducerId,
            ProducerEpoch = r.ProducerEpoch,
            Committed = r.Committed,
        };

    private static EndTxnResponse ToEndTxnResponse(EndTxnResult result, EndTxnRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ToErrorCode(result.Status),
        };

    // ── DescribeProducers (KIP-664) ────────────────────────────────────────

    private static DescribeProducersCommand ToDescribeProducersCommand(DescribeProducersRequest r)
    {
        var topics = new List<DescribeProducersTopic>(r.Topics.Count);
        foreach (var t in r.Topics)
        {
            topics.Add(new DescribeProducersTopic(t.Name, t.PartitionIndexes));
        }
        return new DescribeProducersCommand(topics);
    }

    private static DescribeProducersResponse ToDescribeProducersResponse(DescribeProducersResult result, DescribeProducersRequest r)
    {
        var topicResponses = new List<DescribeProducersResponse.TopicResponse>(result.Topics.Count);
        foreach (var topic in result.Topics)
        {
            var partitionResponses = new List<DescribeProducersResponse.PartitionResponse>(topic.Partitions.Count);
            foreach (var p in topic.Partitions)
            {
                var producerStates = new List<DescribeProducersResponse.ProducerState>(p.ActiveProducers.Count);
                foreach (var ps in p.ActiveProducers)
                {
                    producerStates.Add(new DescribeProducersResponse.ProducerState
                    {
                        ProducerId = ps.ProducerId,
                        ProducerEpoch = ps.ProducerEpoch,
                        LastSequence = ps.LastSequence,
                        LastTimestamp = ps.LastTimestamp,
                        CoordinatorEpoch = ps.CoordinatorEpoch,
                        CurrentTxnStartOffset = ps.CurrentTxnStartOffset,
                    });
                }
                partitionResponses.Add(new DescribeProducersResponse.PartitionResponse
                {
                    PartitionIndex = p.PartitionIndex,
                    ErrorCode = ToErrorCode(p.Status),
                    ErrorMessage = p.ErrorMessage,
                    ActiveProducers = producerStates,
                });
            }
            topicResponses.Add(new DescribeProducersResponse.TopicResponse { Name = topic.Name, Partitions = partitionResponses });
        }

        return new DescribeProducersResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            Topics = topicResponses,
        };
    }

    // ── DescribeTransactions ───────────────────────────────────────────────

    private static DescribeTransactionsResponse ToDescribeTransactionsResponse(IReadOnlyList<TransactionDescription> descriptions, DescribeTransactionsRequest r)
    {
        var states = new List<DescribeTransactionsResponse.TransactionState>(descriptions.Count);
        foreach (var d in descriptions)
        {
            // The neutral projection already carries the numeric Kafka error code (0 / 59).
            var topics = d.Partitions
                .GroupBy(p => p.Topic, StringComparer.Ordinal)
                .Select(g => new DescribeTransactionsResponse.TopicPartition
                {
                    Topic = g.Key,
                    Partitions = g.Select(p => p.Partition).ToList(),
                })
                .ToList();

            states.Add(new DescribeTransactionsResponse.TransactionState
            {
                ErrorCode = (ErrorCode)d.ErrorCode,
                TransactionalId = d.TransactionalId,
                State = d.State,
                TransactionTimeoutMs = d.TransactionTimeoutMs,
                TransactionStartTimeMs = d.TransactionStartTimeMs,
                ProducerId = d.ProducerId,
                ProducerEpoch = d.ProducerEpoch,
                Topics = topics,
            });
        }

        return new DescribeTransactionsResponse
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            TransactionStates = states,
        };
    }

    // ── ListTransactions ───────────────────────────────────────────────────

    private static ListTransactionsResponse ToListTransactionsResponse(IReadOnlyList<TransactionListing> listings, ListTransactionsRequest r)
        => new()
        {
            CorrelationId = r.CorrelationId,
            ApiVersion = r.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            UnknownStateFilters = [],
            TransactionStates = listings.Select(l => new ListTransactionsResponse.TransactionListing
            {
                TransactionalId = l.TransactionalId,
                ProducerId = l.ProducerId,
                TransactionState = l.State,
            }).ToList(),
        };
}
