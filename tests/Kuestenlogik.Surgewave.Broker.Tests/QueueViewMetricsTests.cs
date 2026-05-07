using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the metrics counters (TotalAcked, TotalNacked, TotalRejected, TotalExpired,
/// TotalRedelivered, TotalReceived) and the GetInFlightMessages snapshot on QueueView.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class QueueViewMetricsTests : IAsyncLifetime, IDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly QueueViewConfig _config;

    private const string TestTopic = "metrics-test-topic";
    private const int TestPartition = 0;

    public QueueViewMetricsTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-metrics-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());

        _config = new QueueViewConfig
        {
            Enabled = true,
            VisibilityTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryCount = 5,
            DlqTopicSuffix = ".dlq",
            CleanupInterval = TimeSpan.FromMinutes(60), // No automatic cleanup during tests
            MaxInFlightPerConsumer = 100
        };
    }

    public async ValueTask InitializeAsync()
    {
        await _logManager.CreateTopicAsync(TestTopic, partitionCount: 1, replicationFactor: 1);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
        _logManager.Dispose();
        try
        {
            if (Directory.Exists(_testDirectory))
                Directory.Delete(_testDirectory, recursive: true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    // -------------------------------------------------------------------------
    // Helper utilities (mirrored from QueueViewTests)
    // -------------------------------------------------------------------------

    private static byte[] CreateMinimalRecordBatch()
    {
        var batch = new byte[Core.KafkaConstants.RecordBatch.HeaderSize + 10];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);
        BinaryPrimitives.WriteInt64BigEndian(
            batch.AsSpan(Core.KafkaConstants.RecordBatch.BaseTimestampOffset, 8),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);
        return batch;
    }

    private QueueView CreateView(LogManager? logManager = null)
    {
        var log = _logManager.GetOrCreateLog(new TopicPartition { Topic = TestTopic, Partition = TestPartition });
        return new QueueView(log, _config, NullLogger.Instance, logManager);
    }

    private async Task AppendMessagesAsync(int count)
    {
        var log = _logManager.GetOrCreateLog(new TopicPartition { Topic = TestTopic, Partition = TestPartition });
        for (int i = 0; i < count; i++)
            await log.AppendBatchAsync(CreateMinimalRecordBatch());
    }

    // -------------------------------------------------------------------------
    // 1. Initial state — all counters zero
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Metrics_InitialState_AllCountersAreZero()
    {
        await using var view = CreateView();

        Assert.Equal(0L, view.TotalAcked);
        Assert.Equal(0L, view.TotalNacked);
        Assert.Equal(0L, view.TotalRejected);
        Assert.Equal(0L, view.TotalExpired);
        Assert.Equal(0L, view.TotalRedelivered);
        Assert.Equal(0L, view.TotalReceived);
    }

    // -------------------------------------------------------------------------
    // 2. TotalReceived increments on Receive
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TotalReceived_IncrementsByOnePerMessage()
    {
        await AppendMessagesAsync(3);
        await using var view = CreateView();

        await view.ReceiveAsync(TestPartition, maxMessages: 3);

        Assert.Equal(3L, view.TotalReceived);
    }

    [Fact]
    public async Task TotalReceived_IncrementsAcrossMultipleReceiveCalls()
    {
        await AppendMessagesAsync(2);
        await using var view = CreateView();

        var first = await view.ReceiveAsync(TestPartition, maxMessages: 1);
        view.Ack(first[0].MessageId);
        await view.ReceiveAsync(TestPartition, maxMessages: 1);

        Assert.Equal(2L, view.TotalReceived);
    }

    // -------------------------------------------------------------------------
    // 3. TotalAcked increments on Ack
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TotalAcked_IncrementsOnEachAck()
    {
        await AppendMessagesAsync(3);
        await using var view = CreateView();

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 3);
        foreach (var msg in messages)
            view.Ack(msg.MessageId);

        Assert.Equal(3L, view.TotalAcked);
    }

    [Fact]
    public async Task TotalAcked_DoesNotIncrementOnUnknownMessageId()
    {
        await using var view = CreateView();

        view.Ack("nonexistent-message-id");

        Assert.Equal(0L, view.TotalAcked);
    }

    // -------------------------------------------------------------------------
    // 4. TotalNacked increments on Nack
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TotalNacked_IncrementsOnNackWithRequeue()
    {
        await AppendMessagesAsync(1);
        await using var view = CreateView();

        var messages = await view.ReceiveAsync(TestPartition);
        view.Nack(messages[0].MessageId, requeue: true);

        Assert.Equal(1L, view.TotalNacked);
    }

    [Fact]
    public async Task TotalNacked_IncrementsOnNackWithoutRequeue()
    {
        await AppendMessagesAsync(1);
        await using var view = CreateView();

        var messages = await view.ReceiveAsync(TestPartition);
        view.Nack(messages[0].MessageId, requeue: false);

        Assert.Equal(1L, view.TotalNacked);
    }

    [Fact]
    public async Task TotalNacked_DoesNotIncrementOnUnknownMessageId()
    {
        await using var view = CreateView();

        view.Nack("nonexistent-message-id", requeue: true);

        Assert.Equal(0L, view.TotalNacked);
    }

    // -------------------------------------------------------------------------
    // 5. TotalRejected increments on RejectAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TotalRejected_IncrementsOnRejectAsync()
    {
        await AppendMessagesAsync(2);
        await using var view = CreateView(_logManager);

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 2);
        await view.RejectAsync(messages[0].MessageId);
        await view.RejectAsync(messages[1].MessageId);

        Assert.Equal(2L, view.TotalRejected);
    }

    [Fact]
    public async Task TotalRejected_DoesNotIncrementOnUnknownMessageId()
    {
        await using var view = CreateView();

        await view.RejectAsync("nonexistent-message-id");

        Assert.Equal(0L, view.TotalRejected);
    }

    // -------------------------------------------------------------------------
    // 6. TotalRedelivered increments when messages come from re-delivery queue
    // -------------------------------------------------------------------------

    [Fact]
    public async Task TotalRedelivered_IncrementsWhenMessageIsRedeliveredAfterNack()
    {
        await AppendMessagesAsync(1);
        await using var view = CreateView();

        var first = await view.ReceiveAsync(TestPartition);
        view.Nack(first[0].MessageId, requeue: true);

        // Re-deliver the nacked message
        var second = await view.ReceiveAsync(TestPartition);
        Assert.Single(second);

        Assert.Equal(1L, view.TotalRedelivered);
        // TotalReceived counts both the initial delivery and the redelivery
        Assert.Equal(2L, view.TotalReceived);
    }

    // -------------------------------------------------------------------------
    // 7. GetInFlightMessages returns snapshot of current in-flight messages
    // -------------------------------------------------------------------------

    [Fact]
    public async Task GetInFlightMessages_ReturnsEmptyWhenNoneInFlight()
    {
        await using var view = CreateView();

        var snapshot = view.GetInFlightMessages();

        Assert.Empty(snapshot);
    }

    [Fact]
    public async Task GetInFlightMessages_ReturnsAllCurrentlyInFlightMessages()
    {
        await AppendMessagesAsync(3);
        await using var view = CreateView();

        await view.ReceiveAsync(TestPartition, maxMessages: 3);
        var snapshot = view.GetInFlightMessages();

        Assert.Equal(3, snapshot.Count);
    }

    [Fact]
    public async Task GetInFlightMessages_DoesNotIncludeAckedMessages()
    {
        await AppendMessagesAsync(2);
        await using var view = CreateView();

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 2);
        view.Ack(messages[0].MessageId);

        var snapshot = view.GetInFlightMessages();

        Assert.Single(snapshot);
        Assert.Equal(messages[1].MessageId, snapshot[0].MessageId);
    }

    [Fact]
    public async Task GetInFlightMessages_SnapshotIsIndependentOfLaterChanges()
    {
        await AppendMessagesAsync(2);
        await using var view = CreateView();

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 2);
        // Take snapshot while both messages are in-flight
        var snapshot = view.GetInFlightMessages();

        // Ack one after snapshot was taken
        view.Ack(messages[0].MessageId);

        // Snapshot should still reflect the state at time of call
        Assert.Equal(2, snapshot.Count);
        // But live count should be 1
        Assert.Equal(1, view.InFlightCount);
    }

    // -------------------------------------------------------------------------
    // 8. Metrics accumulate correctly across mixed operations
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Metrics_AccumulateCorrectlyAcrossMixedOperations()
    {
        // 5 messages: ack 2, nack 1 (requeue), reject 1, nack 1 (drop)
        await AppendMessagesAsync(5);
        await using var view = CreateView(_logManager);

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 5);
        Assert.Equal(5, messages.Count);

        view.Ack(messages[0].MessageId);
        view.Ack(messages[1].MessageId);
        view.Nack(messages[2].MessageId, requeue: true);
        await view.RejectAsync(messages[3].MessageId);
        view.Nack(messages[4].MessageId, requeue: false);

        Assert.Equal(5L, view.TotalReceived);
        Assert.Equal(2L, view.TotalAcked);
        Assert.Equal(2L, view.TotalNacked);  // both requeue=true and requeue=false count
        Assert.Equal(1L, view.TotalRejected);
        Assert.Equal(0L, view.TotalExpired); // no timer-triggered expiry happened
        Assert.Equal(0L, view.TotalRedelivered); // requeue puts it back but no second receive yet
    }
}
