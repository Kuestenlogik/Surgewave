using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Xunit;

// NOT ...Tests.Consumer: that would shadow the Client.Consumer namespace for every
// test file that references it relatively (e.g. Consumer.AutoOffsetReset).
namespace Kuestenlogik.Surgewave.Client.Tests.Facades;

/// <summary>
/// Deterministic facade tests for the decoded-batch buffer in
/// <see cref="SurgewaveConsumer{TKey, TValue}"/> (#80 C1, pinned per #102) over the
/// in-memory <see cref="FakeSurgewaveTransport"/> — no broker, no real sockets.
/// </summary>
public class SurgewaveConsumerBufferTests
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(500);

    private static Task<SurgewaveConsumer<string, string>> CreateConsumerAsync(
        FakeSurgewaveTransport fake, string? groupId = null)
        => SurgewaveConsumer<string, string>.CreateAsync(o =>
        {
            o.BootstrapServers = "fake:1";
            o.GroupId = groupId;
            o.AutoOffsetReset = AutoOffsetReset.Earliest;
            o.EnableAutoCommit = false;
            o.TransportFactory = () => fake;
        });

    private static void Seed(FakeSurgewaveTransport fake, string topic, int partition, int count, int startIndex = 0)
    {
        for (int i = 0; i < count; i++)
            fake.Append(topic, partition, key: null, Encoding.UTF8.GetBytes($"m{startIndex + i}"));
    }

    [Fact]
    public async Task WholeBatch_ServedInOffsetOrder_FromExactlyOneFetch()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake);
        consumer.Subscribe("orders");

        for (int i = 0; i < 10; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.NotNull(result);
            Assert.Equal(i, result!.Offset);
            Assert.Equal($"m{i}", result.Value);
        }

        // One broker batch, ten deliveries: messages 2..10 must come from the
        // decoded buffer, not from re-fetching the batch per message.
        Assert.Equal(1, fake.FetchCount);
    }

    [Fact]
    public async Task Seek_InvalidatesBuffer_NextConsumeRefetchesFromSeekOffset()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake);
        consumer.Subscribe("orders");

        for (int i = 0; i < 3; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(1, fake.FetchCount);

        consumer.Seek("orders", 0, 0);

        var result = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(result);
        Assert.Equal(0, result!.Offset); // relocated — a stale buffered record (offset 3) must not serve
        Assert.Equal(2, fake.FetchCount);
    }

    [Fact]
    public async Task Assign_InvalidatesBuffer_NextConsumeRefetchesFromAssignedOffset()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake);
        consumer.Assign("orders", 0, 0);

        for (int i = 0; i < 3; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(1, fake.FetchCount);

        consumer.Assign("orders", 0, 7);

        var result = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(result);
        Assert.Equal(7, result!.Offset);
        Assert.Equal(2, fake.FetchCount);
    }

    [Fact]
    public async Task PartialConsume_Commit_CommitsConsumedPlusOne_NotBufferedEnd()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake, groupId: "g1");
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 3; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, result!.Offset);
        }

        await consumer.CommitAsync();

        // The whole batch (0..9) sits decoded in the buffer, but only 0..2 were
        // consumed — the commit must be consumed+1, never the fetched-ahead end.
        var commit = Assert.Single(fake.CommitRequests);
        Assert.Equal(("g1", "orders", 0, 3L), (commit.GroupId, commit.Topic, commit.Partition, commit.Offset));
    }

    [Fact]
    public async Task RetentionGap_JumpsToLatest_WithoutServingStaleData()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake);
        consumer.Subscribe("orders");

        for (int i = 0; i < 10; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);

        // Retention wipes 10..19; new data starts at offset 20.
        fake.SetEarliestOffset("orders", 0, 20);
        Seed(fake, "orders", 0, 3, startIndex: 20); // offsets 20..22

        // The fetch at offset 10 hits the gap → the facade jumps to latest (23)
        // rather than serving anything stale. The jump consumes this call.
        var gapResult = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Null(gapResult);
        Assert.Equal(23, consumer.Position("orders", 0));

        // New data after the jump is consumed normally.
        fake.Append("orders", 0, key: null, Encoding.UTF8.GetBytes("fresh"));
        var fresh = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(fresh);
        Assert.Equal(23, fresh!.Offset);
        Assert.Equal("fresh", fresh.Value);
    }

    [Theory]
    [InlineData(0)] // poison record served straight from the fetch path
    [InlineData(1)] // poison record served from the decoded buffer
    public async Task DeserializerFailure_DoesNotAdvancePosition_RecordIsRetried_CommitStaysSafe(int poisonOffset)
    {
        var fake = new FakeSurgewaveTransport();
        for (int i = 0; i < 3; i++)
            fake.Append("orders", 0, key: null, Encoding.UTF8.GetBytes(i == poisonOffset ? "poison" : $"m{i}"));

        var deserializer = new PoisonOnceDeserializer("poison");
        await using var consumer = await SurgewaveConsumer<string, string>.CreateAsync(o =>
        {
            o.BootstrapServers = "fake:1";
            o.GroupId = "g1";
            o.AutoOffsetReset = AutoOffsetReset.Earliest;
            o.EnableAutoCommit = false;
            o.ValueDeserializer = deserializer;
            o.TransportFactory = () => fake;
        });
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < poisonOffset; i++)
        {
            var r = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, r!.Offset);
        }

        await Assert.ThrowsAsync<FormatException>(() => consumer.ConsumeAsync(ConsumeTimeout));

        // The failed record was never delivered — position must not have moved, and a
        // commit now must not skip past it (that would be silent loss under auto-commit).
        Assert.Equal(poisonOffset, consumer.Position("orders", 0));
        await consumer.CommitAsync();
        Assert.Equal(poisonOffset, fake.CommitRequests[^1].Offset);

        // The record is retried on the next call (deserializer succeeds once reset).
        for (int i = poisonOffset; i < 3; i++)
        {
            var r = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, r!.Offset);
        }
    }

    private sealed class PoisonOnceDeserializer(string poison) : IDeserializer<string>
    {
        private bool _thrown;

        public string Deserialize(ReadOnlySpan<byte> data, string topic)
        {
            var value = Encoding.UTF8.GetString(data);
            if (value == poison && !_thrown)
            {
                _thrown = true;
                throw new FormatException("poison record");
            }
            return value;
        }
    }

    [Fact]
    public async Task ExplicitCommitToEarlierOffset_RewindsPosition_AndReservesRecords()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 0, 10);

        await using var consumer = await CreateConsumerAsync(fake, groupId: "g1");
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 7; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);

        // An explicit commit below the current position rewinds it — the decoded
        // buffer (whose cursor only skips forward) must not keep serving from 7.
        await consumer.CommitAsync("orders", 0, 3);
        Assert.Equal(3, consumer.Position("orders", 0));

        var reserved = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(reserved);
        Assert.Equal(3, reserved!.Offset);
    }

    [Fact]
    public async Task ConnectionLoss_DropsBuffers_AndResumesAtConsumedPositionViaRefetch()
    {
        var fake = new FakeSurgewaveTransport();
        fake.CreateTopic("orders", 2);
        Seed(fake, "orders", 0, 2);
        Seed(fake, "orders", 1, 10);

        await using var consumer = await CreateConsumerAsync(fake);
        consumer.Subscribe("orders");

        // Drain partition 0 (2 msgs), then partition 1 delivers its first message
        // and leaves 9 decoded messages buffered.
        var offsets = new List<(int Partition, long Offset)>();
        for (int i = 0; i < 3; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            offsets.Add((result!.Partition, result.Offset));
        }
        Assert.Equal([(0, 0L), (0, 1L), (1, 0L)], offsets);
        var fetchesBeforeLoss = fake.FetchCount;

        fake.SimulateConnectionLoss();

        // Next consume: the partition-0 fetch throws → reconnect clears ALL
        // decoded buffers → partition 1 must be re-fetched at the consumed
        // position (offset 1), not served from the stale buffer.
        var afterReconnect = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(afterReconnect);
        Assert.Equal(1, afterReconnect!.Partition);
        Assert.Equal(1, afterReconnect.Offset);

        var fetchesAfterLoss = fake.FetchRequests.Skip(fetchesBeforeLoss).ToList();
        Assert.Contains(fetchesAfterLoss, f => f.Partition == 1 && f.Offset == 1);
    }
}
