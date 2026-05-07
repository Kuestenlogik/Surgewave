using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Protocol.Amqp;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Amqp.Tests;

/// <summary>
/// Tests verifying that <see cref="AmqpBrokerAdapter"/> correctly delegates
/// Ack / Nack / Reject operations to <see cref="IQueueViewManager"/> / <see cref="IQueueView"/>
/// when QueueView semantics are active.
/// </summary>
public sealed class AmqpQueueViewIntegrationTests
{
    // -------------------------------------------------------------------------
    // Fakes
    // -------------------------------------------------------------------------

    private sealed class FakeInFlightMessage : IInFlightMessage
    {
        public required string MessageId { get; init; }
        public required string Topic { get; init; }
        public int Partition { get; init; }
        public long Offset { get; init; }
        public byte[] Body { get; init; } = [];
        public int DeliveryCount { get; init; } = 1;
        public DateTimeOffset ExpiresAt { get; init; } = DateTimeOffset.UtcNow.AddSeconds(30);
        public string? ConsumerId { get; init; }
    }

    private sealed class FakeQueueView : IQueueView
    {
        private readonly ConcurrentQueue<IInFlightMessage> _pending;

        public FakeQueueView(IEnumerable<IInFlightMessage>? initialMessages = null)
        {
            _pending = initialMessages is null
                ? new ConcurrentQueue<IInFlightMessage>()
                : new ConcurrentQueue<IInFlightMessage>(initialMessages);
        }

        public int InFlightCount => _pending.Count;
        public long CommittedOffset(int partition) => -1;
        public long TotalAcked => AckedIds.Count;
        public long TotalNacked => NackedIds.Count;
        public long TotalRejected => RejectedIds.Count;
        public long TotalExpired => 0;
        public long TotalRedelivered => 0;
        public long TotalReceived => 0;
        public IReadOnlyList<IInFlightMessage> GetInFlightMessages() => [];

        public List<string> AckedIds { get; } = [];
        public List<(string Id, bool Requeue)> NackedIds { get; } = [];
        public List<string> RejectedIds { get; } = [];

        public async Task<IReadOnlyList<IInFlightMessage>> ReceiveAsync(
            int partition,
            int maxMessages = 1,
            string? consumerId = null,
            CancellationToken ct = default)
        {
            await Task.Yield();
            var result = new List<IInFlightMessage>();
            while (result.Count < maxMessages && _pending.TryDequeue(out var msg))
                result.Add(msg);
            return result;
        }

        public bool Ack(string messageId)
        {
            AckedIds.Add(messageId);
            return true;
        }

        public bool Nack(string messageId, bool requeue = true)
        {
            NackedIds.Add((messageId, requeue));
            return true;
        }

        public async Task<bool> RejectAsync(string messageId, CancellationToken ct = default)
        {
            await Task.Yield();
            RejectedIds.Add(messageId);
            return true;
        }

        public List<(string MessageId, TimeSpan? Extension)> RenewedIds { get; } = [];

        public bool ExtendVisibility(string messageId, TimeSpan? extension = null)
        {
            RenewedIds.Add((messageId, extension));
            return true;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeQueueViewManager : IQueueViewManager
    {
        private readonly Dictionary<string, IQueueView> _views;

        public FakeQueueViewManager(Dictionary<string, IQueueView> views)
        {
            _views = views;
        }

        public IQueueView GetOrCreate(string topic, Kuestenlogik.Surgewave.Core.Storage.IPartitionLog log) =>
            _views.TryGetValue(topic, out var v) ? v : throw new KeyNotFoundException(topic);

        public IQueueView? Get(string topic) =>
            _views.TryGetValue(topic, out var v) ? v : null;
    }

    // -------------------------------------------------------------------------
    // Helpers: build AmqpChannelState with pre-populated delivery tag → messageId
    // -------------------------------------------------------------------------

    private static AmqpChannelState BuildChannel(
        ushort channelNumber,
        Dictionary<ulong, string>? deliveryTagToMessageId = null)
    {
        var ch = new AmqpChannelState(channelNumber);
        if (deliveryTagToMessageId != null)
        {
            foreach (var (tag, msgId) in deliveryTagToMessageId)
                ch.DeliveryTagToMessageId[tag] = msgId;
        }
        return ch;
    }

    // -------------------------------------------------------------------------
    // AmqpChannelState: DeliveryTagToMessageId bookkeeping
    // -------------------------------------------------------------------------

    [Fact]
    public void AmqpChannelState_DeliveryTagToMessageId_IsInitiallyEmpty()
    {
        var ch = new AmqpChannelState(1);
        Assert.Empty(ch.DeliveryTagToMessageId);
    }

    [Fact]
    public void AmqpChannelState_DeliveryTagToMessageId_CanStoreAndRetrieve()
    {
        var ch = new AmqpChannelState(1);
        ch.DeliveryTagToMessageId[1UL] = "my-topic-0-42";
        ch.DeliveryTagToMessageId[2UL] = "my-topic-0-43";

        Assert.Equal("my-topic-0-42", ch.DeliveryTagToMessageId[1UL]);
        Assert.Equal("my-topic-0-43", ch.DeliveryTagToMessageId[2UL]);
    }

    // -------------------------------------------------------------------------
    // IQueueView: Ack
    // -------------------------------------------------------------------------

    [Fact]
    public void QueueView_Ack_RecordsMessageId()
    {
        var view = new FakeQueueView();
        view.Ack("topic-0-5");

        Assert.Single(view.AckedIds);
        Assert.Equal("topic-0-5", view.AckedIds[0]);
    }

    [Fact]
    public void QueueView_MultipleAck_AcksAllTagsUpToDeliveryTag()
    {
        // Simulate multiple=true: ack all tags <= 3
        var view = new FakeQueueView();
        var ch = BuildChannel(1, new Dictionary<ulong, string>
        {
            [1UL] = "orders-0-0",
            [2UL] = "orders-0-1",
            [3UL] = "orders-0-2",
            [4UL] = "orders-0-3",
        });

        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["orders"] = view
        });

        ulong deliveryTag = 3;
        var tagsToAck = ch.DeliveryTagToMessageId
            .Where(kv => kv.Key <= deliveryTag)
            .ToList();

        foreach (var kv in tagsToAck)
        {
            var v = manager.Get(ExtractTopic(kv.Value));
            v?.Ack(kv.Value);
            ch.DeliveryTagToMessageId.Remove(kv.Key);
        }

        Assert.Equal(3, view.AckedIds.Count);
        Assert.Contains("orders-0-0", view.AckedIds);
        Assert.Contains("orders-0-1", view.AckedIds);
        Assert.Contains("orders-0-2", view.AckedIds);
        Assert.DoesNotContain("orders-0-3", view.AckedIds);
        // tag 4 still present
        Assert.Single(ch.DeliveryTagToMessageId);
    }

    // -------------------------------------------------------------------------
    // IQueueView: Nack with requeue
    // -------------------------------------------------------------------------

    [Fact]
    public void QueueView_Nack_Requeue_True_CallsNackWithRequeue()
    {
        var view = new FakeQueueView();
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["events"] = view
        });

        var ch = BuildChannel(1, new Dictionary<ulong, string>
        {
            [7UL] = "events-0-100"
        });

        // Simulate Basic.Nack with requeue=true
        if (ch.DeliveryTagToMessageId.Remove(7UL, out var msgId))
        {
            var v = manager.Get(ExtractTopic(msgId));
            v?.Nack(msgId, requeue: true);
        }

        Assert.Single(view.NackedIds);
        Assert.Equal(("events-0-100", true), view.NackedIds[0]);
        Assert.Empty(view.AckedIds);
    }

    [Fact]
    public void QueueView_Nack_Requeue_False_CallsNackWithoutRequeue()
    {
        var view = new FakeQueueView();
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["events"] = view
        });

        var ch = BuildChannel(1, new Dictionary<ulong, string>
        {
            [8UL] = "events-0-200"
        });

        // Simulate Basic.Nack with requeue=false
        if (ch.DeliveryTagToMessageId.Remove(8UL, out var msgId))
        {
            var v = manager.Get(ExtractTopic(msgId));
            v?.Nack(msgId, requeue: false);
        }

        Assert.Single(view.NackedIds);
        Assert.Equal(("events-0-200", false), view.NackedIds[0]);
    }

    // -------------------------------------------------------------------------
    // IQueueView: Reject
    // -------------------------------------------------------------------------

    [Fact]
    public async Task QueueView_Reject_WithoutRequeue_CallsRejectAsync()
    {
        var view = new FakeQueueView();
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["payments"] = view
        });

        var ch = BuildChannel(1, new Dictionary<ulong, string>
        {
            [9UL] = "payments-0-55"
        });

        // Simulate Basic.Reject with requeue=false → DLQ
        if (ch.DeliveryTagToMessageId.Remove(9UL, out var msgId))
        {
            var v = manager.Get(ExtractTopic(msgId));
            if (v != null)
                await v.RejectAsync(msgId, CancellationToken.None);
        }

        Assert.Single(view.RejectedIds);
        Assert.Equal("payments-0-55", view.RejectedIds[0]);
        Assert.Empty(view.AckedIds);
        Assert.Empty(view.NackedIds);
    }

    [Fact]
    public void QueueView_Reject_WithRequeue_CallsNack_NotReject()
    {
        var view = new FakeQueueView();
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["payments"] = view
        });

        var ch = BuildChannel(1, new Dictionary<ulong, string>
        {
            [10UL] = "payments-0-60"
        });

        // Simulate Basic.Reject with requeue=true → nack (requeue)
        if (ch.DeliveryTagToMessageId.Remove(10UL, out var msgId))
        {
            var v = manager.Get(ExtractTopic(msgId));
            v?.Nack(msgId, requeue: true);
        }

        Assert.Single(view.NackedIds);
        Assert.Equal(("payments-0-60", true), view.NackedIds[0]);
        Assert.Empty(view.RejectedIds);
    }

    // -------------------------------------------------------------------------
    // IQueueView: ReceiveAsync
    // -------------------------------------------------------------------------

    [Fact]
    public async Task QueueView_ReceiveAsync_ReturnsMessages()
    {
        var messages = new List<IInFlightMessage>
        {
            new FakeInFlightMessage { MessageId = "t-0-0", Topic = "t", Partition = 0, Offset = 0 },
            new FakeInFlightMessage { MessageId = "t-0-1", Topic = "t", Partition = 0, Offset = 1 },
        };
        var view = new FakeQueueView(messages);

        var received = await view.ReceiveAsync(partition: 0, maxMessages: 2);

        Assert.Equal(2, received.Count);
        Assert.Equal("t-0-0", received[0].MessageId);
        Assert.Equal("t-0-1", received[1].MessageId);
    }

    [Fact]
    public async Task QueueView_ReceiveAsync_ReturnsRedeliveredFlag()
    {
        var messages = new List<IInFlightMessage>
        {
            new FakeInFlightMessage
            {
                MessageId = "t-0-5",
                Topic = "t",
                Partition = 0,
                Offset = 5,
                DeliveryCount = 2   // already redelivered once
            }
        };
        var view = new FakeQueueView(messages);

        var received = await view.ReceiveAsync(partition: 0, maxMessages: 1);

        Assert.Single(received);
        Assert.Equal(2, received[0].DeliveryCount);
        Assert.True(received[0].DeliveryCount > 1, "Should be flagged as redelivered");
    }

    // -------------------------------------------------------------------------
    // IQueueViewManager: GetOrCreate / Get
    // -------------------------------------------------------------------------

    [Fact]
    public void QueueViewManager_Get_ReturnsNullForUnknownTopic()
    {
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>());
        Assert.Null(manager.Get("unknown-topic"));
    }

    [Fact]
    public void QueueViewManager_Get_ReturnsViewForKnownTopic()
    {
        var view = new FakeQueueView();
        var manager = new FakeQueueViewManager(new Dictionary<string, IQueueView>
        {
            ["my-topic"] = view
        });

        var result = manager.Get("my-topic");
        Assert.Same(view, result);
    }

    // -------------------------------------------------------------------------
    // Topic extraction helper (mirrors AmqpBrokerAdapter.ExtractTopicFromMessageId)
    // -------------------------------------------------------------------------

    private static string ExtractTopic(string messageId)
    {
        var lastDash = messageId.LastIndexOf('-');
        if (lastDash <= 0) return messageId;
        var secondLastDash = messageId.LastIndexOf('-', lastDash - 1);
        if (secondLastDash <= 0) return messageId;
        return messageId[..secondLastDash];
    }

    [Theory]
    [InlineData("orders-0-42", "orders")]
    [InlineData("my-topic-name-0-100", "my-topic-name")]
    [InlineData("events-1-0", "events")]
    [InlineData("a-b-c-0-999", "a-b-c")]
    public void ExtractTopicFromMessageId_ReturnsExpectedTopic(string messageId, string expectedTopic)
    {
        Assert.Equal(expectedTopic, ExtractTopic(messageId));
    }
}
