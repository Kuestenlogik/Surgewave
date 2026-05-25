using System.Buffers.Binary;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for QueueView, QueueViewManager, and related queue-semantics components.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class QueueViewTests : IAsyncLifetime, IDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly QueueViewConfig _config;

    private const string TestTopic = "queue-test-topic";
    private const int TestPartition = 0;

    public QueueViewTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-queue-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());

        _config = new QueueViewConfig
        {
            Enabled = true,
            VisibilityTimeout = TimeSpan.FromSeconds(30),
            MaxDeliveryCount = 3,
            DlqTopicSuffix = ".dlq",
            CleanupInterval = TimeSpan.FromMinutes(60), // Disable automatic cleanup during tests
            MaxInFlightPerConsumer = 100
        };
    }

    public async ValueTask InitializeAsync()
    {
        await _logManager.CreateTopicAsync(TestTopic, partitionCount: 3, replicationFactor: 1);
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
    // Helper utilities
    // -------------------------------------------------------------------------

    private static byte[] CreateMinimalRecordBatch()
    {
        var batch = new byte[Core.KafkaConstants.RecordBatch.HeaderSize + 10];
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), 0);                   // baseOffset
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);   // batchLength
        BinaryPrimitives.WriteInt64BigEndian(
            batch.AsSpan(Core.KafkaConstants.RecordBatch.BaseTimestampOffset, 8),
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());                            // baseTimestamp
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);                  // recordCount = 1
        return batch;
    }

    private QueueView CreateView(
        IPartitionLog log,
        QueueViewConfig? config = null,
        LogManager? logManager = null)
    {
        var cfg = config ?? _config;
        return new QueueView(log, cfg, NullLogger.Instance, logManager);
    }

    private IPartitionLog GetLog(int partition = TestPartition) =>
        _logManager.GetOrCreateLog(new TopicPartition { Topic = TestTopic, Partition = partition });

    // -------------------------------------------------------------------------
    // 1. Receive returns messages from the log
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Receive_ReturnsMessageFromLog()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 1);

        Assert.Single(messages);
        Assert.Equal(TestTopic, messages[0].Topic);
        Assert.Equal(TestPartition, messages[0].Partition);
        Assert.NotNull(messages[0].Body);
    }

    [Fact]
    public async Task Receive_ReturnsMultipleMessages()
    {
        var log = GetLog();
        for (int i = 0; i < 3; i++)
            await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 3);

        Assert.Equal(3, messages.Count);
    }

    [Fact]
    public async Task Receive_MessageHasCorrectMessageId()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition);

        Assert.Single(messages);
        Assert.Contains($"{TestTopic}-{TestPartition}-", messages[0].MessageId);
    }

    [Fact]
    public async Task Receive_PlacesMessageInFlight()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        Assert.Equal(0, view.InFlightCount);

        await view.ReceiveAsync(TestPartition);

        Assert.Equal(1, view.InFlightCount);
    }

    // -------------------------------------------------------------------------
    // 2. Ack removes from in-flight and advances offset
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Ack_RemovesFromInFlight()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition);
        Assert.Equal(1, view.InFlightCount);

        var acked = view.Ack(messages[0].MessageId);

        Assert.True(acked);
        Assert.Equal(0, view.InFlightCount);
    }

    [Fact]
    public async Task Ack_AdvancesCommittedOffset()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition);
        var expectedOffset = messages[0].Offset;

        view.Ack(messages[0].MessageId);

        Assert.Equal(expectedOffset, view.CommittedOffset(TestPartition));
    }

    [Fact]
    public async Task Ack_UnknownMessageId_ReturnsFalse()
    {
        var log = GetLog();
        await using var view = CreateView(log);

        var result = view.Ack("nonexistent-message-id");

        Assert.False(result);
    }

    [Fact]
    public async Task Ack_CommittedOffset_NeverDecreases()
    {
        var log = GetLog();
        // Append two batches
        await log.AppendBatchAsync(CreateMinimalRecordBatch());
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 2);
        Assert.Equal(2, messages.Count);

        // Ack second (higher offset) first
        view.Ack(messages[1].MessageId);
        var afterSecond = view.CommittedOffset(TestPartition);

        // Then ack first (lower offset)
        view.Ack(messages[0].MessageId);
        var afterFirst = view.CommittedOffset(TestPartition);

        // Committed offset must not decrease
        Assert.Equal(afterSecond, afterFirst);
    }

    // -------------------------------------------------------------------------
    // ExtendVisibility (KIP-932 Renew)
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ExtendVisibility_Default_ResetsExpiryToVisibilityTimeoutFromNow()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        var shortConfig = new QueueViewConfig
        {
            Enabled = true,
            VisibilityTimeout = TimeSpan.FromMilliseconds(50),
            MaxDeliveryCount = 5,
            CleanupInterval = TimeSpan.FromMinutes(60),
            MaxInFlightPerConsumer = 100,
        };
        await using var view = CreateView(log, shortConfig);

        var msgs = await view.ReceiveAsync(TestPartition);
        var msg = Assert.Single(msgs);
        var initialExpiry = ((InFlightMessage)msg).ExpiresAt;
        var initialDelivery = msg.DeliveryCount;

        await Task.Delay(60); // expire the original lease
        var renewed = view.ExtendVisibility(msg.MessageId);

        Assert.True(renewed);
        var renewedMsg = (InFlightMessage)Assert.Single(view.GetInFlightMessages());
        Assert.True(renewedMsg.ExpiresAt > initialExpiry);
        Assert.Equal(initialDelivery, renewedMsg.DeliveryCount); // KIP-932: Renew must NOT bump delivery count
    }

    [Fact]
    public async Task ExtendVisibility_WithExtension_AddsToCurrentExpiry()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var msgs = await view.ReceiveAsync(TestPartition);
        var msg = Assert.Single(msgs);
        var firstExpiry = ((InFlightMessage)msg).ExpiresAt;

        var renewed = view.ExtendVisibility(msg.MessageId, TimeSpan.FromMinutes(1));

        Assert.True(renewed);
        var inflight = (InFlightMessage)Assert.Single(view.GetInFlightMessages());
        Assert.True(inflight.ExpiresAt - firstExpiry >= TimeSpan.FromSeconds(59));
    }

    [Fact]
    public void ExtendVisibility_UnknownMessageId_ReturnsFalse()
    {
        var log = GetLog();
        var view = CreateView(log);
        Assert.False(view.ExtendVisibility("does-not-exist"));
    }

    // -------------------------------------------------------------------------
    // 3. Nack with requeue makes message re-deliverable
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Nack_WithRequeue_MessageReDelivered()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var first = await view.ReceiveAsync(TestPartition);
        Assert.Single(first);
        var firstMsgId = first[0].MessageId;

        var nacked = view.Nack(firstMsgId, requeue: true);
        Assert.True(nacked);
        Assert.Equal(0, view.InFlightCount);

        // Second receive should get the re-queued message
        var second = await view.ReceiveAsync(TestPartition);
        Assert.Single(second);
        Assert.Equal(firstMsgId, second[0].MessageId);
    }

    [Fact]
    public async Task Nack_WithRequeue_IncreasesDeliveryCount()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var first = await view.ReceiveAsync(TestPartition);
        Assert.Equal(1, first[0].DeliveryCount);

        view.Nack(first[0].MessageId, requeue: true);

        var second = await view.ReceiveAsync(TestPartition);
        Assert.Single(second);
        Assert.Equal(2, second[0].DeliveryCount);
    }

    // -------------------------------------------------------------------------
    // 4. Nack without requeue drops message
    // -------------------------------------------------------------------------

    [Fact]
    public async Task Nack_WithoutRequeue_DropsMessage()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var first = await view.ReceiveAsync(TestPartition);
        Assert.Single(first);

        view.Nack(first[0].MessageId, requeue: false);
        Assert.Equal(0, view.InFlightCount);

        // No more messages in log — should return empty
        var second = await view.ReceiveAsync(TestPartition);
        Assert.Empty(second);
    }

    [Fact]
    public async Task Nack_UnknownMessageId_ReturnsFalse()
    {
        var log = GetLog();
        await using var view = CreateView(log);

        Assert.False(view.Nack("no-such-id"));
    }

    // -------------------------------------------------------------------------
    // 5. Visibility timeout → message becomes available again
    // -------------------------------------------------------------------------

    [Fact]
    public async Task VisibilityTimeout_ExpiredMessage_ReQueuedOnNack()
    {
        // Simulate timeout by nacking the expired message with requeue=true
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);
        var first = await view.ReceiveAsync(TestPartition);
        Assert.Single(first);
        Assert.Equal(1, view.InFlightCount);

        // Simulate the visibility window expiring by nacking with requeue
        view.Nack(first[0].MessageId, requeue: true);

        var second = await view.ReceiveAsync(TestPartition);
        Assert.Single(second);
        Assert.Equal(first[0].MessageId, second[0].MessageId);
        Assert.Equal(2, second[0].DeliveryCount);
    }

    [Fact]
    public async Task VisibilityTimeout_SetOnDelivery()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        var now = DateTimeOffset.UtcNow;
        await using var view = CreateView(log);
        var messages = await view.ReceiveAsync(TestPartition);

        Assert.Single(messages);
        // ExpiresAt should be roughly now + VisibilityTimeout
        Assert.True(messages[0].ExpiresAt > now);
        Assert.True(messages[0].ExpiresAt <= now + _config.VisibilityTimeout + TimeSpan.FromSeconds(2));
    }

    // -------------------------------------------------------------------------
    // 6. MaxDeliveryCount → message sent to DLQ after N attempts
    // -------------------------------------------------------------------------

    [Fact]
    public async Task RejectAsync_RoutesDlqTopic_WhenLogManagerAvailable()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = new QueueView(log, _config, NullLogger.Instance, _logManager);
        var messages = await view.ReceiveAsync(TestPartition);
        Assert.Single(messages);

        var rejected = await view.RejectAsync(messages[0].MessageId);

        Assert.True(rejected);
        Assert.Equal(0, view.InFlightCount);

        // DLQ topic must have been auto-created
        var dlqTopic = _config.GetDlqTopicName(TestTopic);
        var dlqMeta = _logManager.GetTopicMetadata(dlqTopic);
        Assert.NotNull(dlqMeta);
    }

    [Fact]
    public async Task RejectAsync_UnknownMessageId_ReturnsFalse()
    {
        var log = GetLog();
        await using var view = CreateView(log);

        var result = await view.RejectAsync("no-such-id");
        Assert.False(result);
    }

    [Fact]
    public async Task RejectAsync_NoLogManager_StillRemovesFromInFlight()
    {
        var log = GetLog();
        await log.AppendBatchAsync(CreateMinimalRecordBatch());

        // No logManager — DLQ write skipped but message still removed
        await using var view = CreateView(log, logManager: null);
        var messages = await view.ReceiveAsync(TestPartition);
        Assert.Single(messages);

        var rejected = await view.RejectAsync(messages[0].MessageId);

        Assert.True(rejected);
        Assert.Equal(0, view.InFlightCount);
    }

    // -------------------------------------------------------------------------
    // 7. Multiple partitions tracked independently
    // -------------------------------------------------------------------------

    [Fact]
    public async Task MultiplePartitions_TrackedIndependently()
    {
        var log0 = GetLog(partition: 0);
        var log1 = GetLog(partition: 1);

        await log0.AppendBatchAsync(CreateMinimalRecordBatch());
        await log1.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view0 = CreateView(log0);
        await using var view1 = CreateView(log1);

        var msgs0 = await view0.ReceiveAsync(0);
        var msgs1 = await view1.ReceiveAsync(1);

        Assert.Single(msgs0);
        Assert.Single(msgs1);
        Assert.NotEqual(msgs0[0].MessageId, msgs1[0].MessageId);

        // Ack partition 0 only
        view0.Ack(msgs0[0].MessageId);

        Assert.Equal(0, view0.InFlightCount);
        Assert.Equal(1, view1.InFlightCount);

        Assert.Equal(msgs0[0].Offset, view0.CommittedOffset(0));
        Assert.Equal(-1L, view1.CommittedOffset(1));
    }

    [Fact]
    public async Task CommittedOffset_StartsAtMinusOne_WhenNoAck()
    {
        var log = GetLog();
        await using var view = CreateView(log);

        Assert.Equal(-1L, view.CommittedOffset(TestPartition));
    }

    // -------------------------------------------------------------------------
    // 8. Concurrent receive/ack from multiple threads
    // -------------------------------------------------------------------------

    [Fact]
    public async Task ConcurrentReceiveAndAck_AllMessagesHandled()
    {
        const int messageCount = 30;

        var log = GetLog();
        for (int i = 0; i < messageCount; i++)
            await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);

        // Sequential receives first to populate in-flight
        var received = new List<IInFlightMessage>();
        var msgs = await view.ReceiveAsync(TestPartition, maxMessages: messageCount);
        received.AddRange(msgs);

        // Concurrent acks
        var ackedCount = 0;
        var tasks = received.Select(msg => Task.Run(() =>
        {
            if (view.Ack(msg.MessageId))
                Interlocked.Increment(ref ackedCount);
        }));

        await Task.WhenAll(tasks);

        Assert.Equal(received.Count, ackedCount);
        Assert.Equal(0, view.InFlightCount);
    }

    [Fact]
    public async Task ConcurrentNackAndAck_NoDeadlock()
    {
        var log = GetLog();
        for (int i = 0; i < 20; i++)
            await log.AppendBatchAsync(CreateMinimalRecordBatch());

        await using var view = CreateView(log);

        var messages = await view.ReceiveAsync(TestPartition, maxMessages: 20);

        var tasks = messages.Select((msg, idx) => Task.Run(() =>
        {
            if (idx % 2 == 0)
                view.Ack(msg.MessageId);
            else
                view.Nack(msg.MessageId, requeue: false);
        }));

        // Must not deadlock or throw
        await Task.WhenAll(tasks);
        Assert.Equal(0, view.InFlightCount);
    }

    // -------------------------------------------------------------------------
    // 9. QueueViewManager creates/retrieves/removes views
    // -------------------------------------------------------------------------

    [Fact]
    public async Task QueueViewManager_GetOrCreate_ReturnsSameInstance()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory, _logManager);

        var log = GetLog();
        var view1 = manager.GetOrCreate(TestTopic, log);
        var view2 = manager.GetOrCreate(TestTopic, log);

        Assert.Same(view1, view2);
    }

    [Fact]
    public async Task QueueViewManager_Get_ReturnsNullForUnknownTopic()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory);

        var result = manager.Get("nonexistent-topic");

        Assert.Null(result);
    }

    [Fact]
    public async Task QueueViewManager_ActiveTopics_ListsEnrolledTopics()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory);

        var log = GetLog();
        manager.GetOrCreate(TestTopic, log);

        Assert.Contains(TestTopic, manager.ActiveTopics);
    }

    [Fact]
    public async Task QueueViewManager_Remove_RemovesView()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory);

        var log = GetLog();
        manager.GetOrCreate(TestTopic, log);
        Assert.NotNull(manager.Get(TestTopic));

        manager.Remove(TestTopic);
        await Task.Delay(100); // Allow background dispose to complete

        Assert.Null(manager.Get(TestTopic));
    }

    [Fact]
    public async Task QueueViewManager_Remove_ThenGetOrCreate_ReturnsNewInstance()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory);

        var log = GetLog();
        var view1 = manager.GetOrCreate(TestTopic, log);

        manager.Remove(TestTopic);
        await Task.Delay(100); // Allow background dispose to complete

        var view2 = manager.GetOrCreate(TestTopic, log);

        Assert.NotSame(view1, view2);
    }

    [Fact]
    public async Task QueueViewManager_TotalInFlightCount_IsZeroInitially()
    {
        using var loggerFactory = LoggerFactory.Create(_ => { });
        await using var manager = new QueueViewManager(_config, loggerFactory);

        var log0 = GetLog(0);
        var log1 = GetLog(1);
        _ = manager.GetOrCreate("topic-a", log0);
        _ = manager.GetOrCreate("topic-b", log1);

        Assert.Equal(0, manager.TotalInFlightCount);
    }

    // -------------------------------------------------------------------------
    // 10. Config helper
    // -------------------------------------------------------------------------

    [Fact]
    public void QueueViewConfig_GetDlqTopicName_AppendsSuffix()
    {
        var config = new QueueViewConfig { DlqTopicSuffix = ".dlq" };

        Assert.Equal("my-topic.dlq", config.GetDlqTopicName("my-topic"));
    }

    [Fact]
    public void QueueViewConfig_GetDlqTopicName_UsesDefaultSuffix()
    {
        var config = new QueueViewConfig();

        Assert.Equal("test.dlq", config.GetDlqTopicName("test"));
    }

    [Fact]
    public void QueueViewConfig_DefaultValues_AreCorrect()
    {
        var config = new QueueViewConfig();

        Assert.Equal(TimeSpan.FromSeconds(30), config.VisibilityTimeout);
        Assert.Equal(5, config.MaxDeliveryCount);
        Assert.Equal(".dlq", config.DlqTopicSuffix);
        Assert.Equal(TimeSpan.FromSeconds(10), config.CleanupInterval);
        Assert.Equal(1000, config.MaxInFlightPerConsumer);
    }
}
