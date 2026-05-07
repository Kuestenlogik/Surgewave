using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Coordinates transactions for transactional producers.
/// Manages transaction state, handles transaction APIs, and writes transaction markers.
/// </summary>
public sealed class TransactionCoordinator : IAsyncDisposable
{
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly ILogger<TransactionCoordinator> _logger;
    private readonly ConcurrentDictionary<string, TransactionMetadata> _transactionsByTxnId = new();
    private readonly TransactionMarkerWriter _markerWriter;
    private readonly TransactionStatePersistence _statePersistence;
    private readonly TransactionTimeoutManager _timeoutManager;

    public TransactionCoordinator(
        ProducerStateManager producerStateManager,
        LogManager logManager,
        TransactionIndex transactionIndex,
        OffsetStore offsetStore,
        TransactionStateStore stateStore,
        ILogger<TransactionCoordinator> logger)
    {
        _producerStateManager = producerStateManager;
        _transactionIndex = transactionIndex;
        _offsetStore = offsetStore;
        _logger = logger;

        // Create helper components
        _markerWriter = new TransactionMarkerWriter(logManager, logger);
        _statePersistence = new TransactionStatePersistence(stateStore, producerStateManager, logger);

        // Load persisted transaction state on startup
        _statePersistence.LoadPersistedState(_transactionsByTxnId);

        // Start timeout manager
        _timeoutManager = new TransactionTimeoutManager(
            _transactionsByTxnId,
            _transactionIndex,
            _markerWriter,
            logger);
    }

    public async ValueTask DisposeAsync()
    {
        await _timeoutManager.DisposeAsync();
    }

    /// <summary>
    /// Get the TransactionIndex for fetch filtering.
    /// </summary>
    public TransactionIndex TransactionIndex => _transactionIndex;

    /// <summary>
    /// Records a transactional batch being written. Called from produce path.
    /// Also implicitly adds the partition to the transaction metadata.
    /// </summary>
    public void RecordTransactionalBatch(TopicPartition partition, long producerId, long baseOffset)
    {
        _transactionIndex.RecordTransactionalBatch(partition, producerId, baseOffset);

        // Also track this partition in our transaction metadata
        foreach (var txnMeta in _transactionsByTxnId.Values)
        {
            if (txnMeta.ProducerId == producerId &&
                (txnMeta.State == TransactionState.Ongoing || txnMeta.State == TransactionState.Empty))
            {
                txnMeta.Partitions.Add(partition);
                txnMeta.LastActivityTime = DateTimeOffset.UtcNow;
                if (txnMeta.State == TransactionState.Empty)
                {
                    txnMeta.State = TransactionState.Ongoing;
                }
                break;
            }
        }
    }

    /// <summary>
    /// Validates a produce batch for idempotence (sequence number validation).
    /// </summary>
    public ErrorCode ValidateProduceBatch(long producerId, short epoch, int baseSequence, TopicPartition topicPartition)
    {
        return _producerStateManager.ValidateSequence(producerId, epoch, baseSequence, topicPartition);
    }

    /// <summary>
    /// Handles InitProducerId request for transactional producers.
    /// </summary>
    public async Task<InitProducerIdResponse> HandleInitProducerIdAsync(InitProducerIdRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(request.TransactionalId))
        {
            // Non-transactional idempotent producer
            var (producerId, epoch) = _producerStateManager.AllocateProducerId();
            return new InitProducerIdResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.None,
                ProducerId = producerId,
                ProducerEpoch = epoch
            };
        }

        // Transactional producer
        var txnMetadata = _transactionsByTxnId.GetOrAdd(request.TransactionalId, txnId =>
        {
            var (pid, ep) = _producerStateManager.AllocateProducerId();
            return new TransactionMetadata
            {
                TransactionalId = txnId,
                ProducerId = pid,
                ProducerEpoch = ep,
                State = TransactionState.Empty,
                TransactionTimeoutMs = request.TransactionTimeoutMs
            };
        });

        // KIP-892 Server-Side Defense: the client never raises the epoch. If the request
        // names a producer-id at all, it must match the current incarnation; an older
        // epoch from the same producer-id means a zombie that recovered after we already
        // fenced it.
        //
        // A request with ProducerId == -1 (no prior identity) is the "fresh init" path;
        // we re-allocate / bump on top of the current state instead of trusting client
        // input. A retry with the exact current (pid, epoch) is idempotent and returns
        // the current values.
        const long NoProducerId = -1;
        const short NoProducerEpoch = -1;
        if (request.ProducerId != NoProducerId)
        {
            if (request.ProducerId != txnMetadata.ProducerId
                || request.ProducerEpoch < txnMetadata.ProducerEpoch)
            {
                _logger.LogInformation(
                    "KIP-892 fence: rejecting InitProducerId for {TransactionalId} (zombie pid={ZombiePid}, epoch={ZombieEpoch}; current pid={Pid}, epoch={Epoch})",
                    txnMetadata.TransactionalId,
                    request.ProducerId,
                    request.ProducerEpoch,
                    txnMetadata.ProducerId,
                    txnMetadata.ProducerEpoch);

                return new InitProducerIdResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ThrottleTimeMs = 0,
                    ErrorCode = ErrorCode.InvalidProducerEpoch,
                    ProducerId = txnMetadata.ProducerId,
                    ProducerEpoch = txnMetadata.ProducerEpoch,
                };
            }

            if (request.ProducerEpoch == txnMetadata.ProducerEpoch
                && txnMetadata.State == TransactionState.Empty)
            {
                // Idempotent retry from the current incarnation, nothing in flight — return as-is.
                return new InitProducerIdResponse
                {
                    CorrelationId = request.CorrelationId,
                    ApiVersion = request.ApiVersion,
                    ThrottleTimeMs = 0,
                    ErrorCode = ErrorCode.None,
                    ProducerId = txnMetadata.ProducerId,
                    ProducerEpoch = txnMetadata.ProducerEpoch,
                };
            }
        }

        // Check for existing transaction that needs cleanup (fencing scenario)
        if (txnMetadata.State == TransactionState.Ongoing ||
            txnMetadata.State == TransactionState.PrepareCommit ||
            txnMetadata.State == TransactionState.PrepareAbort)
        {
            // Producer crash recovery / fencing - abort pending transaction
            if (txnMetadata.Partitions.Count > 0)
            {
                _logger.LogInformation(
                    "Fencing producer {TransactionalId}: aborting in-flight transaction with {PartitionCount} partitions",
                    txnMetadata.TransactionalId,
                    txnMetadata.Partitions.Count);

                var abortOffset = await _markerWriter.WriteMarkersAsync(txnMetadata, commit: false, cancellationToken);
                _transactionIndex.AbortTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, abortOffset);
            }

            // Bump epoch and reset state
            txnMetadata.ProducerEpoch++;
            txnMetadata.State = TransactionState.Empty;
            txnMetadata.Partitions.Clear();
            _producerStateManager.UpdateEpoch(txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
        }
        else if (request.ProducerId == NoProducerId && request.ProducerEpoch == NoProducerEpoch)
        {
            // Fresh-init from a brand-new producer over an existing transactional-id
            // with no in-flight txn: bump the epoch so any zombie still using the
            // previous epoch is fenced on its very next request. Server-owned, not
            // client-driven.
            txnMetadata.ProducerEpoch++;
            _producerStateManager.UpdateEpoch(txnMetadata.ProducerId, txnMetadata.ProducerEpoch);
        }

        _statePersistence.PersistState(txnMetadata);

        return new InitProducerIdResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ProducerId = txnMetadata.ProducerId,
            ProducerEpoch = txnMetadata.ProducerEpoch
        };
    }

    /// <summary>
    /// Handles AddPartitionsToTxn request.
    /// </summary>
    public AddPartitionsToTxnResponse HandleAddPartitionsToTxn(AddPartitionsToTxnRequest request)
    {
        var results = new Dictionary<string, List<AddPartitionsToTxnResponse.PartitionResult>>();

        // Validate request
        var errorCode = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (errorCode != ErrorCode.None)
        {
            foreach (var (topic, partitions) in request.Topics)
            {
                results[topic] = partitions.Select(p => new AddPartitionsToTxnResponse.PartitionResult
                {
                    Partition = p,
                    ErrorCode = errorCode
                }).ToList();
            }

            return new AddPartitionsToTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                Results = results
            };
        }

        // Start transaction if not already started
        if (txnMetadata!.State == TransactionState.Empty ||
            txnMetadata.State == TransactionState.CompleteCommit ||
            txnMetadata.State == TransactionState.CompleteAbort)
        {
            txnMetadata.State = TransactionState.Ongoing;
            txnMetadata.Partitions.Clear();
            txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;
        }
        else
        {
            txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;
        }

        // Add partitions
        foreach (var (topic, partitions) in request.Topics)
        {
            var partitionResults = new List<AddPartitionsToTxnResponse.PartitionResult>();
            foreach (var partition in partitions)
            {
                var tp = new TopicPartition { Topic = topic, Partition = partition };
                txnMetadata.Partitions.Add(tp);
                partitionResults.Add(new AddPartitionsToTxnResponse.PartitionResult
                {
                    Partition = partition,
                    ErrorCode = ErrorCode.None
                });
            }
            results[topic] = partitionResults;
        }

        _statePersistence.PersistState(txnMetadata);

        return new AddPartitionsToTxnResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Results = results
        };
    }

    /// <summary>
    /// Handles AddOffsetsToTxn request - adds consumer group offsets to a transaction.
    /// </summary>
    public AddOffsetsToTxnResponse HandleAddOffsetsToTxn(AddOffsetsToTxnRequest request)
    {
        var errorCode = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (errorCode != ErrorCode.None)
        {
            return new AddOffsetsToTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = errorCode
            };
        }

        // Transaction must be ongoing or empty
        if (txnMetadata!.State != TransactionState.Ongoing && txnMetadata.State != TransactionState.Empty)
        {
            return new AddOffsetsToTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidTxnState
            };
        }

        txnMetadata.ConsumerGroups.Add(request.GroupId);
        if (txnMetadata.State == TransactionState.Empty)
        {
            txnMetadata.State = TransactionState.Ongoing;
        }
        txnMetadata.LastActivityTime = DateTimeOffset.UtcNow;

        _logger.LogDebug("Added consumer group {GroupId} to transaction {TransactionalId}",
            request.GroupId, request.TransactionalId);

        _statePersistence.PersistState(txnMetadata);

        return new AddOffsetsToTxnResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None
        };
    }

    /// <summary>
    /// Handles TxnOffsetCommit request - commits consumer offsets as part of a transaction.
    /// </summary>
    public TxnOffsetCommitResponse HandleTxnOffsetCommit(TxnOffsetCommitRequest request)
    {
        var results = new Dictionary<string, List<TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult>>();

        var errorCode = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (errorCode != ErrorCode.None)
        {
            foreach (var (topic, partitions) in request.Topics)
            {
                results[topic] = partitions.Select(p => new TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult
                {
                    Partition = p.Partition,
                    ErrorCode = errorCode
                }).ToList();
            }

            return new TxnOffsetCommitResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                Topics = results
            };
        }

        // Store pending offsets
        foreach (var (topic, partitions) in request.Topics)
        {
            var partitionResults = new List<TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult>();
            foreach (var partition in partitions)
            {
                txnMetadata!.PendingOffsets.Add(new PendingTxnOffset
                {
                    GroupId = request.GroupId,
                    Topic = topic,
                    Partition = partition.Partition,
                    Offset = partition.CommittedOffset,
                    Metadata = partition.Metadata
                });
                partitionResults.Add(new TxnOffsetCommitResponse.TxnOffsetCommitPartitionResult
                {
                    Partition = partition.Partition,
                    ErrorCode = ErrorCode.None
                });
            }
            results[topic] = partitionResults;
        }

        _logger.LogDebug("Staged {Count} offsets for transaction {TransactionalId}, group {GroupId}",
            request.Topics.Values.Sum(p => p.Count), request.TransactionalId, request.GroupId);

        _statePersistence.PersistState(txnMetadata!);

        return new TxnOffsetCommitResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Topics = results
        };
    }

    /// <summary>
    /// Handles EndTxn request (commit or abort).
    /// </summary>
    public async Task<EndTxnResponse> HandleEndTxnAsync(EndTxnRequest request, CancellationToken cancellationToken)
    {
        var errorCode = ValidateTransactionRequest(request.TransactionalId, request.ProducerId, request.ProducerEpoch, out var txnMetadata);
        if (errorCode != ErrorCode.None)
        {
            return new EndTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = errorCode
            };
        }

        // Allow EndTxn from Empty state (no-op transaction) or Ongoing state
        if (txnMetadata!.State != TransactionState.Ongoing && txnMetadata.State != TransactionState.Empty)
        {
            return new EndTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.InvalidTxnState
            };
        }

        // Handle empty transaction (no partitions, no data) - just transition state
        if (txnMetadata.State == TransactionState.Empty)
        {
            _logger.LogDebug("Completing empty transaction {TransactionalId}", txnMetadata.TransactionalId);
            txnMetadata.State = request.Committed ? TransactionState.CompleteCommit : TransactionState.CompleteAbort;
            _statePersistence.PersistState(txnMetadata);

            return new EndTxnResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.None
            };
        }

        // Transition to prepare state
        txnMetadata.State = request.Committed ? TransactionState.PrepareCommit : TransactionState.PrepareAbort;

        // Write transaction markers
        var markerOffset = await _markerWriter.WriteMarkersAsync(txnMetadata, request.Committed, cancellationToken);

        // Update TransactionIndex and commit/discard offsets
        if (request.Committed)
        {
            _transactionIndex.CommitTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, markerOffset);

            foreach (var pendingOffset in txnMetadata.PendingOffsets)
            {
                _offsetStore.CommitOffset(pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
                _logger.LogDebug("Committed transactional offset for group {GroupId}, {Topic}-{Partition} at offset {Offset}",
                    pendingOffset.GroupId, pendingOffset.Topic, pendingOffset.Partition, pendingOffset.Offset);
            }
        }
        else
        {
            _transactionIndex.AbortTransaction(txnMetadata.ProducerId, txnMetadata.Partitions, markerOffset);
            if (txnMetadata.PendingOffsets.Count > 0)
            {
                _logger.LogDebug("Discarded {Count} pending offsets due to transaction abort", txnMetadata.PendingOffsets.Count);
            }
        }

        // Complete transaction
        txnMetadata.State = request.Committed ? TransactionState.CompleteCommit : TransactionState.CompleteAbort;
        txnMetadata.Partitions.Clear();
        txnMetadata.ConsumerGroups.Clear();
        txnMetadata.PendingOffsets.Clear();

        _statePersistence.PersistState(txnMetadata);

        return new EndTxnResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None
        };
    }

    /// <summary>
    /// Validates a transaction request's transactional ID, producer ID, and epoch.
    /// </summary>
    private ErrorCode ValidateTransactionRequest(string? transactionalId, long producerId, short producerEpoch, out TransactionMetadata? txnMetadata)
    {
        txnMetadata = null;

        if (string.IsNullOrEmpty(transactionalId))
            return ErrorCode.InvalidTxnState;

        if (!_transactionsByTxnId.TryGetValue(transactionalId, out txnMetadata))
            return ErrorCode.UnknownProducerId;

        if (producerId != txnMetadata.ProducerId || producerEpoch != txnMetadata.ProducerEpoch)
        {
            return producerEpoch < txnMetadata.ProducerEpoch
                ? ErrorCode.InvalidProducerEpoch
                : ErrorCode.UnknownProducerId;
        }

        return ErrorCode.None;
    }

    /// <summary>
    /// Lists transactions with optional filters.
    /// </summary>
    /// <param name="statesFilter">Names of <see cref="TransactionState"/> values to match. Empty / null means all states.</param>
    /// <param name="producerIdFilter">Producer ids to match. Empty / null means all producers.</param>
    /// <param name="minDurationMs">KIP-994 (v1+): only return transactions whose last-activity timestamp is older than this many ms. Negative or zero means no duration filter.</param>
    /// <param name="transactionalIdPattern">KIP-1152 (v2+): regular expression that the transactional id must match. Null or empty means no pattern filter. Invalid regex falls through silently — Surgewave prefers a permissive listing over a hard error here, matching librdkafka's behaviour.</param>
    public IReadOnlyList<TransactionListing> ListTransactions(
        IEnumerable<string>? statesFilter = null,
        IEnumerable<long>? producerIdFilter = null,
        long minDurationMs = -1,
        string? transactionalIdPattern = null)
    {
        var stateSet = statesFilter?.Select(s => Enum.TryParse<TransactionState>(s, ignoreCase: true, out var state) ? state : TransactionState.Empty)
            .Where(s => s != TransactionState.Empty)
            .ToHashSet();
        var producerIdSet = producerIdFilter?.ToHashSet();

        var durationCutoff = minDurationMs > 0
            ? (long?)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - minDurationMs
            : null;

        System.Text.RegularExpressions.Regex? regex = null;
        if (!string.IsNullOrEmpty(transactionalIdPattern))
        {
            try
            {
                regex = new System.Text.RegularExpressions.Regex(
                    transactionalIdPattern,
                    System.Text.RegularExpressions.RegexOptions.CultureInvariant,
                    TimeSpan.FromMilliseconds(50));
            }
            catch (ArgumentException)
            {
                // Bad pattern: degrade to no filter rather than dropping all results.
                regex = null;
            }
        }

        return _transactionsByTxnId.Values
            .Where(t => stateSet == null || stateSet.Count == 0 || stateSet.Contains(t.State))
            .Where(t => producerIdSet == null || producerIdSet.Count == 0 || producerIdSet.Contains(t.ProducerId))
            .Where(t => durationCutoff == null || t.LastActivityTime.ToUnixTimeMilliseconds() <= durationCutoff.Value)
            .Where(t => regex == null || RegexMatchSafe(regex, t.TransactionalId))
            .Select(t => new TransactionListing(t.TransactionalId, t.ProducerId, t.State.ToString()))
            .ToList();
    }

    /// <summary>
    /// Apply a regex with the per-pattern <see cref="System.Text.RegularExpressions.RegexMatchTimeoutException"/>
    /// safety net. The Regex constructor sets a 50 ms MatchTimeout to prevent
    /// a malicious or pathological pattern (Cox-style backtracking bait, e.g.
    /// <c>(a+)+$</c> against a long pure-'a' input) from blocking the listing.
    /// When the timeout fires we treat the id as <c>not matched</c> rather
    /// than aborting the entire listing — the same defensive shape as the
    /// invalid-pattern-degrades-to-no-filter case at construction time.
    /// </summary>
    private static bool RegexMatchSafe(System.Text.RegularExpressions.Regex regex, string input)
    {
        try
        {
            return regex.IsMatch(input);
        }
        catch (System.Text.RegularExpressions.RegexMatchTimeoutException)
        {
            return false;
        }
    }

    /// <summary>
    /// KIP-664 <c>DescribeProducers</c> wire handler. Builds a
    /// <see cref="DescribeProducersResponse"/> by asking the producer-state
    /// manager which producer ids have written to (or hold a transaction on)
    /// each requested partition. Per Kafka the response is per-partition;
    /// unknown topics still produce a partition entry with
    /// <see cref="ErrorCode.UnknownTopicOrPartition"/> rather than dropping
    /// the row, so admin tools can correlate request and response indices.
    /// </summary>
    public DescribeProducersResponse HandleDescribeProducers(DescribeProducersRequest request)
    {
        var topicResponses = new List<DescribeProducersResponse.TopicResponse>(request.Topics.Count);
        foreach (var topic in request.Topics)
        {
            var partitionResponses = new List<DescribeProducersResponse.PartitionResponse>(topic.PartitionIndexes.Count);
            foreach (var partitionIndex in topic.PartitionIndexes)
            {
                var partition = new TopicPartition { Topic = topic.Name, Partition = partitionIndex };
                var producers = _producerStateManager.GetActiveProducersForPartition(partition);

                var producerStates = new List<DescribeProducersResponse.ProducerState>(producers.Count);
                foreach (var p in producers)
                {
                    // Surgewave doesn't track per-partition LastTimestamp on the
                    // producer state directly — fall back to -1 (KIP-664 says
                    // "may be -1 if not tracked"). CoordinatorEpoch / TxnStartOffset
                    // are also returned as -1 unless the producer is mid-txn,
                    // in which case the txn coordinator surfaces them through
                    // a future DescribeTransactions follow-up. The HasOngoing
                    // Transaction bit on the snapshot tells admin tools "yes,
                    // this producer holds a partition lock right now".
                    producerStates.Add(new DescribeProducersResponse.ProducerState
                    {
                        ProducerId = p.ProducerId,
                        ProducerEpoch = p.Epoch,
                        LastSequence = p.LastSequence,
                        LastTimestamp = -1,
                        CoordinatorEpoch = -1,
                        CurrentTxnStartOffset = p.HasOngoingTransaction ? 0 : -1,
                    });
                }

                partitionResponses.Add(new DescribeProducersResponse.PartitionResponse
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = ErrorCode.None,
                    ErrorMessage = null,
                    ActiveProducers = producerStates,
                });
            }
            topicResponses.Add(new DescribeProducersResponse.TopicResponse
            {
                Name = topic.Name,
                Partitions = partitionResponses,
            });
        }

        return new DescribeProducersResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Topics = topicResponses,
        };
    }

    /// <summary>
    /// Kafka-wire handler for <c>DescribeTransactions</c> (API key 65). Maps
    /// the existing <see cref="DescribeTransactions(IEnumerable{string})"/>
    /// projection onto the on-the-wire response shape so admin clients
    /// (Confluent.Kafka.Admin / Java AdminClient) can call the RPC directly
    /// instead of going through Surgewave's gRPC AdminService.
    /// </summary>
    public DescribeTransactionsResponse HandleDescribeTransactions(DescribeTransactionsRequest request)
    {
        var descriptions = DescribeTransactions(request.TransactionalIds);
        var states = new List<DescribeTransactionsResponse.TransactionState>(descriptions.Count);
        foreach (var d in descriptions)
        {
            // Existing DescribeTransactions returns ErrorCode=59 (UnknownProducerId)
            // for ids the broker has never seen; map straight through.
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
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            TransactionStates = states,
        };
    }

    /// <summary>
    /// Kafka-wire handler for <c>ListTransactions</c> (API key 66, v0–v2).
    /// Wraps the existing <see cref="ListTransactions"/> + KIP-994 / KIP-1152
    /// filter logic. The wire-level <c>DurationFilter</c> and
    /// <c>TransactionalIdPattern</c> fields read by the request parser flow
    /// straight through.
    /// </summary>
    public ListTransactionsResponse HandleListTransactions(ListTransactionsRequest request)
    {
        var listings = ListTransactions(
            statesFilter: request.StateFilters,
            producerIdFilter: request.ProducerIdFilters,
            minDurationMs: request.DurationFilter,
            transactionalIdPattern: request.TransactionalIdPattern);

        return new ListTransactionsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
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

    /// <summary>
    /// Describes transactions by transactional IDs.
    /// </summary>
    public IReadOnlyList<TransactionDescription> DescribeTransactions(IEnumerable<string> transactionalIds)
    {
        var result = new List<TransactionDescription>();
        foreach (var txnId in transactionalIds)
        {
            if (_transactionsByTxnId.TryGetValue(txnId, out var txn))
            {
                result.Add(new TransactionDescription(
                    txn.TransactionalId,
                    txn.State.ToString(),
                    txn.ProducerId,
                    txn.ProducerEpoch,
                    txn.TransactionTimeoutMs,
                    txn.LastActivityTime.ToUnixTimeMilliseconds(),
                    txn.Partitions.Select(p => (p.Topic, p.Partition)).ToList(),
                    0));
            }
            else
            {
                result.Add(new TransactionDescription(
                    txnId,
                    "Unknown",
                    -1,
                    -1,
                    0,
                    0,
                    new List<(string, int)>(),
                    59)); // UNKNOWN_PRODUCER_ID
            }
        }
        return result;
    }
}
