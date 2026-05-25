using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Clustering;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Broker telemetry using System.Diagnostics.Metrics and ActivitySource (OpenTelemetry compatible).
/// Separate from logging - this provides structured metrics and distributed tracing.
/// </summary>
public sealed class BrokerMetrics : IDisposable, IClusteringMetrics
{
    public const string MeterName = "Kuestenlogik.Surgewave.Broker";
    public const string ActivitySourceName = "Kuestenlogik.Surgewave.Broker";

    private readonly Meter _meter;
    private readonly ActivitySource _activitySource;

    // === Connection Metrics ===
    private readonly Counter<long> _connectionsTotal;
    private readonly UpDownCounter<int> _activeConnections;

    // === Request Metrics ===
    private readonly Counter<long> _requestsTotal;
    private readonly Histogram<double> _requestDuration;

    // === Produce Metrics ===
    private readonly Counter<long> _messagesProducedTotal;
    private readonly Counter<long> _bytesProducedTotal;
    private readonly Histogram<double> _produceLatency;
    private readonly Counter<long> _produceErrorsTotal;

    // === Fetch Metrics ===
    private readonly Counter<long> _messagesFetchedTotal;
    private readonly Counter<long> _bytesFetchedTotal;
    private readonly Histogram<double> _fetchLatency;

    // === Topic/Partition Metrics ===
    private readonly ObservableGauge<int> _topicCount;
    private readonly ObservableGauge<long> _totalLogSize;
    private readonly ObservableGauge<int> _partitionCount;

    // === Consumer Group Metrics ===
    private readonly UpDownCounter<int> _activeConsumerGroups;
    private readonly Counter<long> _rebalancesTotal;
    private readonly Histogram<double> _commitLatency;

    // === Transaction Metrics ===
    private readonly Counter<long> _transactionsTotal;
    private readonly Counter<long> _transactionCommitsTotal;
    private readonly Counter<long> _transactionAbortsTotal;
    private readonly Histogram<double> _transactionDuration;
    private readonly Counter<long> _transactionTimeoutsTotal;
    private readonly Counter<long> _transactionFencingTotal;
    private readonly UpDownCounter<int> _activeTransactions;
    private readonly Counter<long> _pendingOffsetsTotal;

    // === Quota Metrics ===
    private readonly Counter<long> _throttledRequestsTotal;
    private readonly Histogram<double> _throttleTime;

    // === Error Metrics ===
    private readonly Counter<long> _errorsTotal;

    // === Data Integrity Metrics ===
    private readonly Counter<long> _corruptedBatchesTotal;
    private readonly Counter<long> _corruptedBytesTotal;
    private readonly Counter<long> _crcValidationsTotal;

    // === Replication Metrics ===
    private readonly UpDownCounter<int> _isrCount;
    private readonly Counter<long> _replicationBytesTotal;
    private readonly Histogram<double> _replicationLag;

    // === Shared Memory Metrics ===
    private readonly Counter<long> _shmConnectionsTotal;
    private readonly UpDownCounter<int> _shmConnectionsActive;
    private readonly Counter<long> _shmMessagesReceivedTotal;
    private readonly Counter<long> _shmMessagesSentTotal;
    private readonly Counter<long> _shmBytesReceivedTotal;
    private readonly Counter<long> _shmBytesSentTotal;
    private readonly Histogram<double> _shmRequestLatency;

    // === Consumer Lag Metrics ===
    private readonly ObservableGauge<long> _consumerLag;
    private readonly ObservableGauge<long> _maxConsumerLag;
    private readonly Counter<long> _lagWarningsTotal;
    private readonly Counter<long> _lagCriticalTotal;
    private readonly UpDownCounter<int> _groupsWithHighLag;

    // === Deduplication Metrics ===
    private readonly Counter<long> _deduplicatedMessagesTotal;
    private readonly ObservableGauge<long> _dedupWindowSize;

    // === Delayed Delivery Metrics ===
    private readonly Counter<long> _delayedMessagesTotal;
    private readonly ObservableGauge<long> _delayedMessagesPending;

    // === TTL Metrics ===
    private readonly Counter<long> _ttlMessagesTotal;
    private readonly Counter<long> _ttlExpiredMessagesTotal;
    private readonly ObservableGauge<long> _ttlTrackedMessages;

    // === Broker DLQ Metrics ===
    private readonly Counter<long> _dlqNacksTotal;
    private readonly Counter<long> _dlqRoutedTotal;
    private readonly Counter<long> _dlqRetriesTotal;

    // State accessors for observable gauges
    private Func<int>? _getTopicCount;
    private Func<long>? _getTotalLogSize;
    private Func<int>? _getPartitionCount;
    private Func<IEnumerable<Measurement<long>>>? _getLagMeasurements;
    private Func<long>? _getMaxLag;
    private Func<long>? _getDedupWindowSize;
    private Func<long>? _getDelayedPending;
    private Func<long>? _getTtlTracked;

    public BrokerMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        _activitySource = new ActivitySource(ActivitySourceName, "1.0.0");

        // === Connection Metrics ===
        _connectionsTotal = _meter.CreateCounter<long>(
            "surgewave_connections_total",
            description: "Total number of client connections established");

        _activeConnections = _meter.CreateUpDownCounter<int>(
            "surgewave_connections_active",
            description: "Current number of active client connections");

        // === Request Metrics ===
        _requestsTotal = _meter.CreateCounter<long>(
            "surgewave_requests_total",
            description: "Total number of requests processed by API type");

        _requestDuration = _meter.CreateHistogram<double>(
            "surgewave_request_duration_ms",
            unit: "ms",
            description: "Request processing duration in milliseconds");

        // === Produce Metrics ===
        _messagesProducedTotal = _meter.CreateCounter<long>(
            "surgewave_messages_produced_total",
            description: "Total number of messages produced");

        _bytesProducedTotal = _meter.CreateCounter<long>(
            "surgewave_bytes_produced_total",
            unit: "By",
            description: "Total bytes produced");

        _produceLatency = _meter.CreateHistogram<double>(
            "surgewave_produce_latency_ms",
            unit: "ms",
            description: "Produce request latency in milliseconds");

        _produceErrorsTotal = _meter.CreateCounter<long>(
            "surgewave_produce_errors_total",
            description: "Total number of produce errors");

        // === Fetch Metrics ===
        _messagesFetchedTotal = _meter.CreateCounter<long>(
            "surgewave_messages_fetched_total",
            description: "Total number of messages fetched");

        _bytesFetchedTotal = _meter.CreateCounter<long>(
            "surgewave_bytes_fetched_total",
            unit: "By",
            description: "Total bytes fetched");

        _fetchLatency = _meter.CreateHistogram<double>(
            "surgewave_fetch_latency_ms",
            unit: "ms",
            description: "Fetch request latency in milliseconds");

        // === Topic/Partition Metrics (Observable - pull-based) ===
        _topicCount = _meter.CreateObservableGauge(
            "surgewave_topics",
            () => _getTopicCount?.Invoke() ?? 0,
            description: "Number of topics");

        _totalLogSize = _meter.CreateObservableGauge(
            "surgewave_log_size_bytes",
            () => _getTotalLogSize?.Invoke() ?? 0,
            unit: "By",
            description: "Total log size in bytes across all partitions");

        _partitionCount = _meter.CreateObservableGauge(
            "surgewave_partitions",
            () => _getPartitionCount?.Invoke() ?? 0,
            description: "Total number of partitions");

        // === Consumer Group Metrics ===
        _activeConsumerGroups = _meter.CreateUpDownCounter<int>(
            "surgewave_consumer_groups_active",
            description: "Number of active consumer groups");

        _rebalancesTotal = _meter.CreateCounter<long>(
            "surgewave_consumer_group_rebalances_total",
            description: "Total number of consumer group rebalances");

        _commitLatency = _meter.CreateHistogram<double>(
            "surgewave_commit_latency_ms",
            unit: "ms",
            description: "Offset commit latency in milliseconds");

        // === Transaction Metrics ===
        _transactionsTotal = _meter.CreateCounter<long>(
            "surgewave_transactions_total",
            description: "Total number of transactions started");

        _transactionCommitsTotal = _meter.CreateCounter<long>(
            "surgewave_transaction_commits_total",
            description: "Total number of transactions committed");

        _transactionAbortsTotal = _meter.CreateCounter<long>(
            "surgewave_transaction_aborts_total",
            description: "Total number of transactions aborted");

        _transactionDuration = _meter.CreateHistogram<double>(
            "surgewave_transaction_duration_ms",
            unit: "ms",
            description: "Transaction duration in milliseconds");

        _transactionTimeoutsTotal = _meter.CreateCounter<long>(
            "surgewave_transaction_timeouts_total",
            description: "Total number of transactions timed out");

        _transactionFencingTotal = _meter.CreateCounter<long>(
            "surgewave_transaction_fencing_total",
            description: "Total number of producer fencing events");

        _activeTransactions = _meter.CreateUpDownCounter<int>(
            "surgewave_transactions_active",
            description: "Number of currently active transactions");

        _pendingOffsetsTotal = _meter.CreateCounter<long>(
            "surgewave_transaction_pending_offsets_total",
            description: "Total number of pending transactional offsets staged");

        // === Quota Metrics ===
        _throttledRequestsTotal = _meter.CreateCounter<long>(
            "surgewave_throttled_requests_total",
            description: "Total number of requests throttled due to quota");

        _throttleTime = _meter.CreateHistogram<double>(
            "surgewave_throttle_time_ms",
            unit: "ms",
            description: "Time requests were throttled in milliseconds");

        // === Error Metrics ===
        _errorsTotal = _meter.CreateCounter<long>(
            "surgewave_errors_total",
            description: "Total number of errors by type");

        // === Data Integrity Metrics ===
        _corruptedBatchesTotal = _meter.CreateCounter<long>(
            "surgewave_corrupted_batches_total",
            description: "Total number of corrupted record batches detected");

        _corruptedBytesTotal = _meter.CreateCounter<long>(
            "surgewave_corrupted_bytes_total",
            unit: "By",
            description: "Total bytes in corrupted record batches");

        _crcValidationsTotal = _meter.CreateCounter<long>(
            "surgewave_crc_validations_total",
            description: "Total number of CRC validations performed");

        // === Replication Metrics ===
        _isrCount = _meter.CreateUpDownCounter<int>(
            "surgewave_isr_count",
            description: "Number of in-sync replicas");

        _replicationBytesTotal = _meter.CreateCounter<long>(
            "surgewave_replication_bytes_total",
            unit: "By",
            description: "Total bytes replicated");

        _replicationLag = _meter.CreateHistogram<double>(
            "surgewave_replication_lag_ms",
            unit: "ms",
            description: "Replication lag in milliseconds");

        // === Shared Memory Metrics ===
        _shmConnectionsTotal = _meter.CreateCounter<long>(
            "surgewave_shm_connections_total",
            description: "Total number of shared memory client connections");

        _shmConnectionsActive = _meter.CreateUpDownCounter<int>(
            "surgewave_shm_connections_active",
            description: "Current number of active shared memory client connections");

        _shmMessagesReceivedTotal = _meter.CreateCounter<long>(
            "surgewave_shm_messages_received_total",
            description: "Total messages received via shared memory");

        _shmMessagesSentTotal = _meter.CreateCounter<long>(
            "surgewave_shm_messages_sent_total",
            description: "Total messages sent via shared memory");

        _shmBytesReceivedTotal = _meter.CreateCounter<long>(
            "surgewave_shm_bytes_received_total",
            unit: "By",
            description: "Total bytes received via shared memory");

        _shmBytesSentTotal = _meter.CreateCounter<long>(
            "surgewave_shm_bytes_sent_total",
            unit: "By",
            description: "Total bytes sent via shared memory");

        _shmRequestLatency = _meter.CreateHistogram<double>(
            "surgewave_shm_request_latency_us",
            unit: "us",
            description: "Shared memory request latency in microseconds");

        // === Consumer Lag Metrics ===
        _consumerLag = _meter.CreateObservableGauge(
            "surgewave_consumer_lag",
            () => _getLagMeasurements?.Invoke() ?? [],
            description: "Consumer lag per consumer group/topic/partition");

        _maxConsumerLag = _meter.CreateObservableGauge(
            "surgewave_consumer_lag_max",
            () => _getMaxLag?.Invoke() ?? 0,
            description: "Maximum consumer lag across all groups");

        _lagWarningsTotal = _meter.CreateCounter<long>(
            "surgewave_consumer_lag_warnings_total",
            description: "Total number of consumer lag warning threshold breaches");

        _lagCriticalTotal = _meter.CreateCounter<long>(
            "surgewave_consumer_lag_critical_total",
            description: "Total number of consumer lag critical threshold breaches");

        _groupsWithHighLag = _meter.CreateUpDownCounter<int>(
            "surgewave_consumer_groups_high_lag",
            description: "Number of consumer groups currently exceeding lag threshold");

        // === Deduplication Metrics ===
        _deduplicatedMessagesTotal = _meter.CreateCounter<long>(
            "surgewave_deduplicated_messages_total",
            description: "Total number of duplicate messages detected and rejected");

        _dedupWindowSize = _meter.CreateObservableGauge(
            "surgewave_dedup_window_size",
            () => _getDedupWindowSize?.Invoke() ?? 0,
            description: "Current number of entries in the deduplication window");

        // === Delayed Delivery Metrics ===
        _delayedMessagesTotal = _meter.CreateCounter<long>(
            "surgewave_delayed_messages_total",
            description: "Total number of messages with delayed delivery");

        _delayedMessagesPending = _meter.CreateObservableGauge(
            "surgewave_delayed_messages_pending",
            () => _getDelayedPending?.Invoke() ?? 0,
            description: "Current number of messages pending delayed delivery");

        // === TTL Metrics ===
        _ttlMessagesTotal = _meter.CreateCounter<long>(
            "surgewave_ttl_messages_total",
            description: "Total number of messages with TTL headers");

        _ttlExpiredMessagesTotal = _meter.CreateCounter<long>(
            "surgewave_ttl_expired_messages_total",
            description: "Total number of messages filtered due to TTL expiry");

        _ttlTrackedMessages = _meter.CreateObservableGauge(
            "surgewave_ttl_tracked_messages",
            () => _getTtlTracked?.Invoke() ?? 0,
            description: "Current number of messages tracked for TTL expiry");

        // === Broker DLQ Metrics ===
        _dlqNacksTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_nacks_total",
            description: "Total number of message nacks received");

        _dlqRoutedTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_routed_total",
            description: "Total number of messages routed to DLQ after max retries");

        _dlqRetriesTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_retries_total",
            description: "Total number of message retry attempts via broker DLQ");
    }

    /// <summary>
    /// Register callbacks for observable gauges (pull-based metrics)
    /// </summary>
    public void RegisterStateAccessors(Func<int> getTopicCount, Func<long> getTotalLogSize, Func<int>? getPartitionCount = null)
    {
        _getTopicCount = getTopicCount;
        _getTotalLogSize = getTotalLogSize;
        _getPartitionCount = getPartitionCount;
    }

    /// <summary>
    /// Register callbacks for consumer lag metrics.
    /// </summary>
    public void RegisterLagAccessors(
        Func<IEnumerable<Measurement<long>>> getLagMeasurements,
        Func<long> getMaxLag)
    {
        _getLagMeasurements = getLagMeasurements;
        _getMaxLag = getMaxLag;
    }

    // === Activity/Tracing Methods ===

    /// <summary>
    /// Start a produce activity for distributed tracing
    /// </summary>
    public Activity? StartProduceActivity(string topic, int partition)
    {
        var activity = _activitySource.StartActivity("surgewave.produce", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.destination.partition.id", partition);
        activity?.SetTag("messaging.operation", "publish");
        return activity;
    }

    /// <summary>
    /// Start a fetch activity for distributed tracing
    /// </summary>
    public Activity? StartFetchActivity(string topic, int partition)
    {
        var activity = _activitySource.StartActivity("surgewave.fetch", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("messaging.source.name", topic);
        activity?.SetTag("messaging.source.partition.id", partition);
        activity?.SetTag("messaging.operation", "receive");
        return activity;
    }

    /// <summary>
    /// Start a transaction activity for distributed tracing
    /// </summary>
    public Activity? StartTransactionActivity(string transactionalId)
    {
        var activity = _activitySource.StartActivity("surgewave.transaction", ActivityKind.Internal);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("surgewave.transactional_id", transactionalId);
        return activity;
    }

    /// <summary>
    /// Start a generic request activity
    /// </summary>
    public Activity? StartRequestActivity(string apiKey)
    {
        var activity = _activitySource.StartActivity($"surgewave.request.{apiKey.ToLowerInvariant()}", ActivityKind.Server);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("surgewave.api_key", apiKey);
        return activity;
    }

    // === Connection Tracking ===
    public void RecordConnectionOpened() => _connectionsTotal.Add(1);
    public void IncrementActiveConnections() => _activeConnections.Add(1);
    public void DecrementActiveConnections() => _activeConnections.Add(-1);

    // === Request Tracking ===
    public void RecordRequest(string apiKey, double durationMs)
    {
        var tags = new TagList { { "api_key", apiKey } };
        _requestsTotal.Add(1, tags);
        _requestDuration.Record(durationMs, tags);
    }

    // === Produce Tracking ===
    public void RecordProduce(string topic, int partition, int messageCount, long bytes, double latencyMs)
    {
        var tags = new TagList { { "topic", topic }, { "partition", partition } };
        _messagesProducedTotal.Add(messageCount, tags);
        _bytesProducedTotal.Add(bytes, tags);
        _produceLatency.Record(latencyMs, tags);
    }

    public void RecordProduceError(string topic, int partition, string errorCode)
    {
        var tags = new TagList { { "topic", topic }, { "partition", partition }, { "error_code", errorCode } };
        _produceErrorsTotal.Add(1, tags);
    }

    // === Fetch Tracking ===
    public void RecordFetch(string topic, int partition, int messageCount, long bytes, double latencyMs)
    {
        var tags = new TagList { { "topic", topic }, { "partition", partition } };
        _messagesFetchedTotal.Add(messageCount, tags);
        _bytesFetchedTotal.Add(bytes, tags);
        _fetchLatency.Record(latencyMs, tags);
    }

    // === Consumer Group Tracking ===
    public void IncrementActiveConsumerGroups() => _activeConsumerGroups.Add(1);
    public void DecrementActiveConsumerGroups() => _activeConsumerGroups.Add(-1);

    public void RecordRebalance(string groupId)
    {
        _rebalancesTotal.Add(1, new TagList { { "group_id", groupId } });
    }

    public void RecordCommit(string groupId, double latencyMs)
    {
        _commitLatency.Record(latencyMs, new TagList { { "group_id", groupId } });
    }

    // === Consumer Lag Tracking ===

    /// <summary>
    /// Record a consumer lag warning threshold breach.
    /// </summary>
    public void RecordLagWarning(string groupId, string topic, int partition, long lag)
    {
        _lagWarningsTotal.Add(1, new TagList
        {
            { "group_id", groupId },
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Record a consumer lag critical threshold breach.
    /// </summary>
    public void RecordLagCritical(string groupId, string topic, int partition, long lag)
    {
        _lagCriticalTotal.Add(1, new TagList
        {
            { "group_id", groupId },
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Increment count of groups with high lag.
    /// </summary>
    public void IncrementGroupsWithHighLag() => _groupsWithHighLag.Add(1);

    /// <summary>
    /// Decrement count of groups with high lag.
    /// </summary>
    public void DecrementGroupsWithHighLag() => _groupsWithHighLag.Add(-1);

    // === Transaction Tracking ===
    public void RecordTransactionStarted(string transactionalId)
    {
        _transactionsTotal.Add(1, new TagList { { "transactional_id", transactionalId } });
        _activeTransactions.Add(1);
    }

    public void RecordTransactionCommitted(string transactionalId, double durationMs)
    {
        var tags = new TagList { { "transactional_id", transactionalId } };
        _transactionCommitsTotal.Add(1, tags);
        _transactionDuration.Record(durationMs, tags);
        _activeTransactions.Add(-1);
    }

    public void RecordTransactionAborted(string transactionalId, double durationMs)
    {
        var tags = new TagList { { "transactional_id", transactionalId } };
        _transactionAbortsTotal.Add(1, tags);
        _transactionDuration.Record(durationMs, tags);
        _activeTransactions.Add(-1);
    }

    public void RecordTransactionTimeout(string transactionalId)
    {
        _transactionTimeoutsTotal.Add(1, new TagList { { "transactional_id", transactionalId } });
        _activeTransactions.Add(-1);
    }

    public void RecordTransactionFencing(string transactionalId)
    {
        _transactionFencingTotal.Add(1, new TagList { { "transactional_id", transactionalId } });
    }

    public void RecordPendingOffsets(string transactionalId, int count)
    {
        _pendingOffsetsTotal.Add(count, new TagList { { "transactional_id", transactionalId } });
    }

    // === Quota Tracking ===
    public void RecordThrottle(string clientId, double throttleTimeMs)
    {
        var tags = new TagList { { "client_id", clientId } };
        _throttledRequestsTotal.Add(1, tags);
        _throttleTime.Record(throttleTimeMs, tags);
    }

    // === Error Tracking ===
    public void RecordError(string errorType)
    {
        _errorsTotal.Add(1, new TagList { { "type", errorType } });
    }

    public void RecordError(string errorType, string topic, int partition)
    {
        _errorsTotal.Add(1, new TagList { { "type", errorType }, { "topic", topic }, { "partition", partition } });
    }

    // === Data Integrity Tracking ===

    /// <summary>
    /// Record a corrupted batch detected during read.
    /// </summary>
    public void RecordCorruptedBatch(string topic, int partition, long batchOffset, int batchBytes)
    {
        _corruptedBatchesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
        _corruptedBytesTotal.Add(batchBytes, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Record CRC validation performed.
    /// </summary>
    public void RecordCrcValidation(string topic, int partition, bool valid)
    {
        _crcValidationsTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition },
            { "valid", valid.ToString().ToLowerInvariant() }
        });
    }

    /// <summary>
    /// Record multiple CRC validations.
    /// </summary>
    public void RecordCrcValidations(string topic, int partition, int count)
    {
        _crcValidationsTotal.Add(count, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    // === Replication Tracking ===
    public void IncrementIsrCount(string topic, int partition)
    {
        _isrCount.Add(1, new TagList { { "topic", topic }, { "partition", partition } });
    }

    public void DecrementIsrCount(string topic, int partition)
    {
        _isrCount.Add(-1, new TagList { { "topic", topic }, { "partition", partition } });
    }

    // IClusteringMetrics implementation
    public void RecordReplicaJoinedIsr(string topic, int partition) => IncrementIsrCount(topic, partition);
    public void RecordReplicaLeftIsr(string topic, int partition) => DecrementIsrCount(topic, partition);

    public void RecordReplicationBytes(string topic, int partition, long bytes)
    {
        _replicationBytesTotal.Add(bytes, new TagList { { "topic", topic }, { "partition", partition } });
    }

    public void RecordReplicationLag(string topic, int partition, double lagMs)
    {
        _replicationLag.Record(lagMs, new TagList { { "topic", topic }, { "partition", partition } });
    }

    // === Shared Memory Tracking ===

    /// <summary>
    /// Record a new shared memory client connection.
    /// </summary>
    public void RecordShmConnectionOpened(Guid clientId)
    {
        _shmConnectionsTotal.Add(1);
        _shmConnectionsActive.Add(1);
    }

    /// <summary>
    /// Record a shared memory client disconnection.
    /// </summary>
    public void RecordShmConnectionClosed(Guid clientId)
    {
        _shmConnectionsActive.Add(-1);
    }

    /// <summary>
    /// Record a message received via shared memory.
    /// </summary>
    public void RecordShmMessageReceived(int bytes)
    {
        _shmMessagesReceivedTotal.Add(1);
        _shmBytesReceivedTotal.Add(bytes);
    }

    /// <summary>
    /// Record a message sent via shared memory.
    /// </summary>
    public void RecordShmMessageSent(int bytes)
    {
        _shmMessagesSentTotal.Add(1);
        _shmBytesSentTotal.Add(bytes);
    }

    /// <summary>
    /// Record shared memory request latency in microseconds.
    /// </summary>
    public void RecordShmRequestLatency(double latencyMicroseconds)
    {
        _shmRequestLatency.Record(latencyMicroseconds);
    }

    /// <summary>
    /// Start a shared memory request activity for distributed tracing.
    /// </summary>
    public Activity? StartShmRequestActivity(Guid clientId, string opCode)
    {
        var activity = _activitySource.StartActivity("surgewave.shm.request", ActivityKind.Server);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("surgewave.transport", "shared_memory");
        activity?.SetTag("surgewave.client_id", clientId.ToString());
        activity?.SetTag("surgewave.op_code", opCode);
        return activity;
    }

    // === Deduplication Tracking ===

    /// <summary>
    /// Record a deduplicated (rejected) message.
    /// </summary>
    public void RecordDeduplication(string topic, int partition)
    {
        _deduplicatedMessagesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Register callback for deduplication window size gauge.
    /// </summary>
    public void RegisterDedupAccessor(Func<long> getDedupWindowSize)
    {
        _getDedupWindowSize = getDedupWindowSize;
    }

    // === Delayed Delivery Tracking ===

    /// <summary>
    /// Record a message with delayed delivery.
    /// </summary>
    public void RecordDelayedMessage(string topic, int partition)
    {
        _delayedMessagesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Register callback for delayed messages pending gauge.
    /// </summary>
    public void RegisterDelayAccessor(Func<long> getDelayedPending)
    {
        _getDelayedPending = getDelayedPending;
    }

    // === TTL Tracking ===

    /// <summary>
    /// Record a message with TTL header.
    /// </summary>
    public void RecordTtlMessage(string topic, int partition)
    {
        _ttlMessagesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Record a message filtered due to TTL expiry.
    /// </summary>
    public void RecordTtlExpired(string topic, int partition)
    {
        _ttlExpiredMessagesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Register callback for TTL tracked messages gauge.
    /// </summary>
    public void RegisterTtlAccessor(Func<long> getTtlTracked)
    {
        _getTtlTracked = getTtlTracked;
    }

    // === Broker DLQ Tracking ===

    /// <summary>
    /// Record a nack received by the broker DLQ manager.
    /// </summary>
    public void RecordDlqNack(string topic, int partition)
    {
        _dlqNacksTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Record a message routed to DLQ after exceeding max retries.
    /// </summary>
    public void RecordDlqRouted(string topic, int partition)
    {
        _dlqRoutedTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    /// <summary>
    /// Record a retry attempt by the broker DLQ manager.
    /// </summary>
    public void RecordDlqRetry(string topic, int partition)
    {
        _dlqRetriesTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition }
        });
    }

    public void Dispose()
    {
        _meter.Dispose();
        _activitySource.Dispose();
    }
}
