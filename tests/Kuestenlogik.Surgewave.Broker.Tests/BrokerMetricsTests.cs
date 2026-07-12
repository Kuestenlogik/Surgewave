using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for BrokerMetrics telemetry class.
/// Verifies metrics are recorded correctly and activities are created for distributed tracing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class BrokerMetricsTests : IDisposable
{
    private readonly BrokerMetrics _metrics;
    private readonly MeterListener _meterListener;
    private readonly ActivityListener _activityListener;
    private readonly Dictionary<string, List<(object Value, TagList Tags)>> _recordedMetrics = new();
    private readonly List<Activity> _recordedActivities = [];

    public BrokerMetricsTests()
    {
        _metrics = new BrokerMetrics();

        // Set up meter listener to capture metrics
        _meterListener = new MeterListener();
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            // Scope to THIS test's BrokerMetrics instance only. Filtering by meter name alone
            // captures instruments from other BrokerMetrics instances created by tests running in
            // parallel (they share the meter name), which non-deterministically leaks extra
            // measurements into _recordedMetrics and made ActiveConnections_TracksUpAndDown flaky.
            if (ReferenceEquals(instrument.Meter, _metrics.Meter))
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
        _meterListener.Start();

        // Set up activity listener to capture activities
        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == BrokerMetrics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => _recordedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_activityListener);
    }

    private void OnMeasurementRecorded<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        where T : struct
    {
        if (!_recordedMetrics.TryGetValue(instrument.Name, out var list))
        {
            list = [];
            _recordedMetrics[instrument.Name] = list;
        }

        var tagList = new TagList();
        foreach (var tag in tags)
        {
            tagList.Add(tag.Key, tag.Value);
        }
        list.Add((measurement!, tagList));
    }

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
        _metrics.Dispose();
    }

    [Fact]
    public void RecordConnectionOpened_IncrementsCounter()
    {
        // Act
        _metrics.RecordConnectionOpened();
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_connections_total"));
        var values = _recordedMetrics["surgewave_connections_total"];
        Assert.Single(values);
        Assert.Equal(1L, values[0].Value);
    }

    [Fact]
    public void ActiveConnections_TracksUpAndDown()
    {
        // Act
        _metrics.IncrementActiveConnections();
        _metrics.IncrementActiveConnections();
        _metrics.DecrementActiveConnections();
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_connections_active"));
        var values = _recordedMetrics["surgewave_connections_active"];
        Assert.Equal(3, values.Count);
        Assert.Equal(1, values[0].Value);
        Assert.Equal(1, values[1].Value);
        Assert.Equal(-1, values[2].Value);
    }

    [Fact]
    public void RecordRequest_RecordsCountAndDuration()
    {
        // Act
        _metrics.RecordRequest("Produce", 15.5);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_requests_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_request_duration_ms"));

        var requestCount = _recordedMetrics["surgewave_requests_total"];
        Assert.Single(requestCount);
        Assert.Equal(1L, requestCount[0].Value);

        var duration = _recordedMetrics["surgewave_request_duration_ms"];
        Assert.Single(duration);
        Assert.Equal(15.5, duration[0].Value);
    }

    [Fact]
    public void RecordProduce_RecordsMessagesAndBytes()
    {
        // Act
        _metrics.RecordProduce("test-topic", 0, messageCount: 10, bytes: 1024, latencyMs: 5.2);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_messages_produced_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_bytes_produced_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_produce_latency_ms"));

        var messageCount = _recordedMetrics["surgewave_messages_produced_total"][0];
        Assert.Equal(10L, messageCount.Value);

        var bytesCount = _recordedMetrics["surgewave_bytes_produced_total"][0];
        Assert.Equal(1024L, bytesCount.Value);
    }

    [Fact]
    public void RecordProduceError_RecordsWithTags()
    {
        // Act
        _metrics.RecordProduceError("test-topic", 0, "UNKNOWN_TOPIC_OR_PARTITION");
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_produce_errors_total"));
        var error = _recordedMetrics["surgewave_produce_errors_total"][0];
        Assert.Equal(1L, error.Value);
    }

    [Fact]
    public void RecordFetch_RecordsMessagesAndBytes()
    {
        // Act
        _metrics.RecordFetch("test-topic", 1, messageCount: 50, bytes: 8192, latencyMs: 12.3);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_messages_fetched_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_bytes_fetched_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_fetch_latency_ms"));
    }

    [Fact]
    public void ConsumerGroupMetrics_TracksGroups()
    {
        // Act
        _metrics.IncrementActiveConsumerGroups();
        _metrics.RecordRebalance("test-group");
        _metrics.RecordCommit("test-group", 3.5);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_consumer_groups_active"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_consumer_group_rebalances_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_commit_latency_ms"));
    }

    [Fact]
    public void TransactionMetrics_TracksTransactions()
    {
        // Act
        _metrics.RecordTransactionStarted("txn-1");
        _metrics.RecordTransactionCommitted("txn-1", 100.5);
        _metrics.RecordTransactionAborted("txn-2", 50.3);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_transactions_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_transaction_commits_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_transaction_aborts_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_transaction_duration_ms"));
    }

    [Fact]
    public void QuotaMetrics_TracksThrottling()
    {
        // Act
        _metrics.RecordThrottle("client-1", 500.0);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_throttled_requests_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_throttle_time_ms"));
    }

    [Fact]
    public void ErrorMetrics_TracksErrors()
    {
        // Act
        _metrics.RecordError("CONNECTION_ERROR");
        _metrics.RecordError("TIMEOUT", "test-topic", 0);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_errors_total"));
        Assert.Equal(2, _recordedMetrics["surgewave_errors_total"].Count);
    }

    [Fact]
    public void ReplicationMetrics_TracksReplication()
    {
        // Act
        _metrics.IncrementIsrCount("test-topic", 0);
        _metrics.RecordReplicationBytes("test-topic", 0, 4096);
        _metrics.RecordReplicationLag("test-topic", 0, 25.5);
        _meterListener.RecordObservableInstruments();

        // Assert
        Assert.True(_recordedMetrics.ContainsKey("surgewave_isr_count"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_replication_bytes_total"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_replication_lag_ms"));
    }

    [Fact]
    public void StartProduceActivity_CreatesActivityWithTags()
    {
        // Act
        using var activity = _metrics.StartProduceActivity("test-topic", 0);

        // Assert
        Assert.NotNull(activity);
        Assert.Equal("surgewave.produce", activity.OperationName);
        Assert.Equal(ActivityKind.Producer, activity.Kind);
        Assert.Equal("surgewave", activity.GetTagItem("messaging.system"));
        Assert.Equal("test-topic", activity.GetTagItem("messaging.destination.name"));
        Assert.Equal(0, activity.GetTagItem("messaging.destination.partition.id"));
    }

    [Fact]
    public void StartFetchActivity_CreatesActivityWithTags()
    {
        // Act
        using var activity = _metrics.StartFetchActivity("test-topic", 1);

        // Assert
        Assert.NotNull(activity);
        Assert.Equal("surgewave.fetch", activity.OperationName);
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("surgewave", activity.GetTagItem("messaging.system"));
        Assert.Equal("test-topic", activity.GetTagItem("messaging.source.name"));
        Assert.Equal(1, activity.GetTagItem("messaging.source.partition.id"));
    }

    [Fact]
    public void StartTransactionActivity_CreatesActivityWithTags()
    {
        // Act
        using var activity = _metrics.StartTransactionActivity("txn-producer-1");

        // Assert
        Assert.NotNull(activity);
        Assert.Equal("surgewave.transaction", activity.OperationName);
        Assert.Equal(ActivityKind.Internal, activity.Kind);
        Assert.Equal("surgewave", activity.GetTagItem("messaging.system"));
        Assert.Equal("txn-producer-1", activity.GetTagItem("surgewave.transactional_id"));
    }

    [Fact]
    public void StartRequestActivity_CreatesActivityWithTags()
    {
        // Act
        using var activity = _metrics.StartRequestActivity("Metadata");

        // Assert
        Assert.NotNull(activity);
        Assert.Equal("surgewave.request.metadata", activity.OperationName);
        Assert.Equal(ActivityKind.Server, activity.Kind);
        Assert.Equal("Metadata", activity.GetTagItem("surgewave.api_key"));
    }

    [Fact]
    public void RegisterStateAccessors_ObservableGaugesWork()
    {
        // Arrange
        int topicCount = 5;
        long logSize = 1024 * 1024;
        int partitionCount = 10;

        _metrics.RegisterStateAccessors(
            () => topicCount,
            () => logSize,
            () => partitionCount);

        // Act
        _meterListener.RecordObservableInstruments();

        // Assert - observable gauges should be recorded when listener polls them
        Assert.True(_recordedMetrics.ContainsKey("surgewave_topics"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_log_size_bytes"));
        Assert.True(_recordedMetrics.ContainsKey("surgewave_partitions"));
    }
}
