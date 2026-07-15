using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Replicates transaction markers (commit/abort) to follower brokers (#59 b5 — relocated to
/// the Kafka plugin). Implements the neutral <see cref="ITransactionMarkerReplicator"/> contract
/// so the broker's cluster-aware transaction coordinator can drive it without naming this
/// Kafka-wire type. Uses WriteTxnMarkersRequest to ensure durability before acknowledging.
/// </summary>
internal sealed partial class TransactionMarkerReplicator : ITransactionMarkerReplicator
{
    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly int _localBrokerId;
    private readonly ILogger<TransactionMarkerReplicator> _logger;
    private readonly TransactionMarkerReplicatorOptions _options;
    private int _correlationId;

    public TransactionMarkerReplicator(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        int localBrokerId,
        ILogger<TransactionMarkerReplicator> logger,
        TransactionMarkerReplicatorOptions? options = null)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _localBrokerId = localBrokerId;
        _logger = logger;
        _options = options ?? new TransactionMarkerReplicatorOptions();
    }

    /// <summary>
    /// Replicates transaction markers to all follower brokers that host replicas
    /// for the partitions involved in the transaction.
    /// </summary>
    /// <param name="txnMetadata">The transaction metadata.</param>
    /// <param name="commit">True for commit markers, false for abort markers.</param>
    /// <param name="coordinatorEpoch">The coordinator epoch for fencing.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Result indicating success or failure with details.</returns>
    public async Task<MarkerReplicationResult> ReplicateMarkersAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        IReadOnlyList<TopicPartition> partitions,
        bool commit,
        int coordinatorEpoch,
        CancellationToken cancellationToken)
    {
        var result = new MarkerReplicationResult();
        if (partitions.Count == 0)
        {
            result.IsSuccess = true;
            return result;
        }

        // Group partitions by follower broker, recording each partition's outcome (#72 Inc6): a
        // partition unknown to cluster state is a VISIBLE skip rather than a silent drop.
        var brokerPartitions = GroupPartitionsByFollowerBroker(partitions, result);

        // Log-only min.insync.replicas assessment, shared verbatim with the native replicator.
        AssessMinIsr(transactionalId, partitions);

        if (brokerPartitions.Count == 0)
        {
            LogNoFollowersToReplicate(transactionalId, partitions.Count);
            result.IsSuccess = true;
            return result;
        }

        var retryCount = 0;

        while (retryCount <= _options.MaxRetries)
        {
            var tasks = brokerPartitions
                .Select(kvp => SendMarkersWithRetryAsync(
                    kvp.Key,
                    producerId,
                    producerEpoch,
                    commit,
                    coordinatorEpoch,
                    kvp.Value,
                    cancellationToken))
                .ToList();

            var responses = await Task.WhenAll(tasks);

            // Collect results
            var failedBrokers = new Dictionary<int, List<PartitionTopicPair>>();
            foreach (var response in responses)
            {
                if (response.Success)
                {
                    result.SuccessfulBrokers.Add(response.BrokerId);
                }
                else
                {
                    result.FailedBrokers[response.BrokerId] = response.Error ?? "Unknown error";

                    // Collect partitions that need retry
                    if (brokerPartitions.TryGetValue(response.BrokerId, out var retryPartitions))
                    {
                        failedBrokers[response.BrokerId] = retryPartitions;
                    }
                }
            }

            // Check if we have enough replicas acknowledged
            if (MeetsReplicationRequirements(result, partitions))
            {
                result.IsSuccess = true;
                LogReplicationSucceeded(
                    transactionalId,
                    commit ? "COMMIT" : "ABORT",
                    result.SuccessfulBrokers.Count);
                return result;
            }

            // Retry failed brokers
            if (failedBrokers.Count > 0 && retryCount < _options.MaxRetries)
            {
                retryCount++;
                brokerPartitions = failedBrokers;

                LogRetryingReplication(
                    transactionalId,
                    retryCount,
                    _options.MaxRetries,
                    failedBrokers.Count);

                await Task.Delay(_options.RetryBackoffMs, cancellationToken);
            }
            else
            {
                break;
            }
        }

        result.IsSuccess = false;
        LogReplicationFailed(
            transactionalId,
            commit ? "COMMIT" : "ABORT",
            result.FailedBrokers.Count);

        return result;
    }

    /// <summary>
    /// Groups partitions by the follower brokers that host replicas.
    /// Excludes the local broker (leader).
    /// </summary>
    private Dictionary<int, List<PartitionTopicPair>> GroupPartitionsByFollowerBroker(
        IEnumerable<TopicPartition> partitions, MarkerReplicationResult result)
    {
        var brokerPartitions = new Dictionary<int, List<PartitionTopicPair>>();

        foreach (var tp in partitions)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state == null)
            {
                result.PartitionOutcomes[tp] = MarkerPartitionOutcome.SkippedUnknownPartition;
                continue;
            }

            result.PartitionOutcomes[tp] = MarkerPartitionOutcome.Replicated; // dispatched to its followers

            // Get all replicas except the local broker
            foreach (var replicaId in state.Replicas)
            {
                if (replicaId == _localBrokerId) continue;

                if (!brokerPartitions.TryGetValue(replicaId, out var list))
                {
                    list = [];
                    brokerPartitions[replicaId] = list;
                }

                list.Add(new PartitionTopicPair(tp.Topic, tp.Partition));
            }
        }

        return brokerPartitions;
    }

    // #72 Inc6 — log-only: report the involved partitions under min.insync.replicas via the one shared
    // assessment, so the Kafka-wire and native transports log an identical view.
    private void AssessMinIsr(string transactionalId, IReadOnlyList<TopicPartition> partitions)
    {
        var triples = new List<(TopicPartition, int, int)>(partitions.Count);
        foreach (var tp in partitions)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state is not null)
                triples.Add((tp, state.Isr.Count, state.MinInSyncReplicas));
        }

        var under = MarkerReplicationAssessment.UnderMinIsr(triples);
        if (under.Count > 0)
            LogUnderMinIsr(transactionalId, under.Count, string.Join(", ", under.Select(tp => $"{tp.Topic}-{tp.Partition}")));
    }

    /// <summary>
    /// Sends markers to a specific broker with retry on network errors.
    /// </summary>
    private async Task<BrokerReplicationResult> SendMarkersWithRetryAsync(
        int brokerId,
        long producerId,
        short producerEpoch,
        bool commit,
        int coordinatorEpoch,
        List<PartitionTopicPair> partitions,
        CancellationToken cancellationToken)
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker == null)
        {
            LogBrokerNotFound(brokerId);
            return new BrokerReplicationResult(brokerId, false, "Broker not found");
        }

        try
        {
            var request = BuildWriteTxnMarkersRequest(
                producerId,
                producerEpoch,
                commit,
                coordinatorEpoch,
                partitions);

            var response = await SendRequestAsync<WriteTxnMarkersResponse>(
                broker.Host,
                broker.Port,
                request,
                WriteTxnMarkersResponse.ReadFrom,
                cancellationToken);

            if (response == null)
            {
                return new BrokerReplicationResult(brokerId, false, "No response received");
            }

            // Check for errors in response
            var errors = ExtractErrors(response);
            if (errors.Count > 0)
            {
                LogPartitionErrors(brokerId, errors);
                return new BrokerReplicationResult(brokerId, false, $"Partition errors: {string.Join(", ", errors.Values)}");
            }

            LogMarkersSent(brokerId, partitions.Count, commit ? "COMMIT" : "ABORT");
            return new BrokerReplicationResult(brokerId, true, null);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            LogSendFailed(brokerId, ex);
            return new BrokerReplicationResult(brokerId, false, ex.Message);
        }
    }

    private WriteTxnMarkersRequest BuildWriteTxnMarkersRequest(
        long producerId,
        short producerEpoch,
        bool commit,
        int coordinatorEpoch,
        List<PartitionTopicPair> partitions)
    {
        // Group partitions by topic
        var topicPartitions = partitions
            .GroupBy(p => p.Topic)
            .Select(g => new WriteTxnMarkersRequest.TopicPartition
            {
                Topic = g.Key,
                PartitionIndexes = g.Select(p => p.Partition).ToList()
            })
            .ToList();

        return new WriteTxnMarkersRequest
        {
            ApiKey = ApiKey.WriteTxnMarkers,
            ApiVersion = 1, // v1 is flexible and includes CoordinatorEpoch
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = $"surgewave-txn-replicator-{_localBrokerId}",
            Markers =
            [
                new WriteTxnMarkersRequest.MarkerEntry
                {
                    ProducerId = producerId,
                    ProducerEpoch = producerEpoch,
                    TransactionResult = commit,
                    CoordinatorEpoch = coordinatorEpoch,
                    Topics = topicPartitions
                }
            ]
        };
    }

    private static Dictionary<string, ErrorCode> ExtractErrors(WriteTxnMarkersResponse response)
    {
        var errors = new Dictionary<string, ErrorCode>();

        foreach (var marker in response.Markers)
        {
            foreach (var topic in marker.Topics)
            {
                foreach (var partition in topic.Partitions)
                {
                    if (partition.ErrorCode != ErrorCode.None)
                    {
                        errors[$"{topic.Topic}-{partition.PartitionIndex}"] = partition.ErrorCode;
                    }
                }
            }
        }

        return errors;
    }

    private bool MeetsReplicationRequirements(
        MarkerReplicationResult result,
        IReadOnlyCollection<TopicPartition> partitions)
    {
        // For now, consider successful if at least one broker acknowledged
        // In a full implementation, this would check min.insync.replicas per partition
        return result.SuccessfulBrokers.Count > 0 || result.FailedBrokers.Count == 0;
    }

    private async Task<TResponse?> SendRequestAsync<TResponse>(
        string host,
        int port,
        KafkaRequest request,
        Func<KafkaProtocolReader, short, int, TResponse> responseReader,
        CancellationToken cancellationToken) where TResponse : KafkaResponse
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_options.TimeoutMs);

        var connection = await _connectionPool.GetConnectionAsync(host, port, timeoutCts.Token);
        try
        {
            var stream = connection.Stream;

            // Serialize request
            var requestBytes = request.Serialize();

            // Write size-prefixed message
            var sizeBuffer = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, requestBytes.Length);
            await stream.WriteAsync(sizeBuffer, timeoutCts.Token);
            await stream.WriteAsync(requestBytes, timeoutCts.Token);
            await stream.FlushAsync(timeoutCts.Token);

            // Read response size
            var responseSizeBuffer = new byte[4];
            await ReadExactlyAsync(stream, responseSizeBuffer, timeoutCts.Token);
            var responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer);

            // Read response body
            var responseBuffer = new byte[responseSize];
            await ReadExactlyAsync(stream, responseBuffer, timeoutCts.Token);

            // Parse response
            var reader = new KafkaProtocolReader(responseBuffer);
            var correlationId = reader.ReadInt32();

            return responseReader(reader, request.ApiVersion, correlationId);
        }
        finally
        {
            connection.Return();
        }
    }

    private static async Task ReadExactlyAsync(
        Stream stream,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken);
            if (read == 0)
                throw new EndOfStreamException("Connection closed while reading response");
            totalRead += read;
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "No follower brokers to replicate markers for transaction {TransactionalId} ({PartitionCount} partitions)")]
    private partial void LogNoFollowersToReplicate(string transactionalId, int partitionCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transaction {TransactionalId}: {Count} involved partition(s) below min.insync.replicas when writing markers: {Partitions}")]
    private partial void LogUnderMinIsr(string transactionalId, int count, string partitions);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} not found in cluster state")]
    private partial void LogBrokerNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Sent {MarkerType} markers to broker {BrokerId} for {PartitionCount} partitions")]
    private partial void LogMarkersSent(int brokerId, int partitionCount, string markerType);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send markers to broker {BrokerId}")]
    private partial void LogSendFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} returned partition errors: {Errors}")]
    private partial void LogPartitionErrors(int brokerId, Dictionary<string, ErrorCode> errors);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction {TransactionalId}: {MarkerType} markers replicated to {BrokerCount} brokers")]
    private partial void LogReplicationSucceeded(string transactionalId, string markerType, int brokerCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transaction {TransactionalId}: Failed to replicate {MarkerType} markers to {FailedCount} brokers")]
    private partial void LogReplicationFailed(string transactionalId, string markerType, int failedCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction {TransactionalId}: Retrying replication (attempt {RetryCount}/{MaxRetries}) for {BrokerCount} brokers")]
    private partial void LogRetryingReplication(string transactionalId, int retryCount, int maxRetries, int brokerCount);
}

/// <summary>
/// Configuration options for transaction marker replication.
/// </summary>
internal sealed class TransactionMarkerReplicatorOptions
{
    /// <summary>
    /// Timeout for marker replication in milliseconds.
    /// Default: 5000ms
    /// </summary>
    public int TimeoutMs { get; init; } = 5000;

    /// <summary>
    /// Maximum number of retry attempts.
    /// Default: 3
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Backoff delay between retries in milliseconds.
    /// Default: 100ms
    /// </summary>
    public int RetryBackoffMs { get; init; } = 100;
}

/// <summary>
/// Result of marker replication to a single broker.
/// </summary>
internal readonly record struct BrokerReplicationResult(int BrokerId, bool Success, string? Error);

/// <summary>
/// Helper struct for grouping partition info.
/// </summary>
internal readonly record struct PartitionTopicPair(string Topic, int Partition);
