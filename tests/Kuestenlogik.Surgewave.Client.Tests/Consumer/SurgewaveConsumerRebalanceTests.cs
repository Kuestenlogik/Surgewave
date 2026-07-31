using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

// NOT ...Tests.Consumer: that would shadow the Client.Consumer namespace for every
// test file that references it relatively (e.g. Consumer.AutoOffsetReset).
namespace Kuestenlogik.Surgewave.Client.Tests.Facades;

/// <summary>
/// Pins the consumer-group rebalance model (#116): the heartbeat task only DETECTS
/// that a rejoin is due, the rejoin itself runs on the consumer thread inside
/// ConsumeAsync. That makes consumer state single-writer, keeps the rejoin trigger
/// reachable through the header-error path the real broker uses, and stops a rejoin
/// from leaking a heartbeat loop.
/// </summary>
public class SurgewaveConsumerRebalanceTests
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    private const int TenMessageFetchBytes = 1300; // see SurgewaveConsumerPrefetchTests

    private static Task<SurgewaveConsumer<string, string>> CreateConsumerAsync(
        FakeSurgewaveTransport fake, bool enablePrefetch = false, bool enableAutoCommit = false)
        => SurgewaveConsumer<string, string>.CreateAsync(o =>
        {
            o.BootstrapServers = "fake:1";
            o.GroupId = "g1";
            o.AutoOffsetReset = AutoOffsetReset.Earliest;
            o.EnableAutoCommit = enableAutoCommit;
            o.AutoCommitIntervalMs = 1;
            o.FetchMaxBytes = TenMessageFetchBytes;
            o.EnablePrefetch = enablePrefetch;
            o.HeartbeatIntervalMs = 30; // fast heartbeat so tests observe signals promptly
            o.TransportFactory = () => fake;
        });

    /// <summary>
    /// Arms a failing heartbeat and returns once the client has PROCESSED it. Delivery
    /// alone is not enough: the response is handed out before the heartbeat loop runs its
    /// catch block. The loop is sequential, so the arrival of the following heartbeat
    /// proves the failed one was handled and the rejoin is flagged.
    /// </summary>
    private static async Task FailHeartbeatAndAwaitProcessingAsync(
        FakeSurgewaveTransport fake, SurgewaveErrorCode errorCode)
    {
        await fake.FailNextHeartbeatAsync(errorCode).WaitAsync(TestTimeout);

        var before = fake.HeartbeatCount;
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (fake.HeartbeatCount <= before && DateTime.UtcNow < deadline)
            await Task.Delay(5);
        Assert.True(fake.HeartbeatCount > before, "timed out waiting for the heartbeat after the failed one");
    }

    private static void Seed(FakeSurgewaveTransport fake, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var value = new byte[100];
            Encoding.UTF8.GetBytes($"m{i}").CopyTo(value, 0);
            fake.Append("orders", 0, key: null, value);
        }
    }

    [Theory]
    [InlineData(SurgewaveErrorCode.RebalanceInProgress)]
    [InlineData(SurgewaveErrorCode.IllegalGeneration)]
    [InlineData(SurgewaveErrorCode.UnknownMemberId)]
    public async Task GroupErrorInHeartbeatHeader_TriggersRejoinOnNextConsume(SurgewaveErrorCode errorCode)
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, 5);

        await using var consumer = await CreateConsumerAsync(fake);
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");
        Assert.Equal(1, fake.JoinGroupCount);

        // The error arrives in the response header, so the client's command layer throws
        // before parsing the payload — that path must still flag the rejoin.
        await FailHeartbeatAndAwaitProcessingAsync(fake, errorCode);

        // The heartbeat task itself must NOT rejoin (that would mutate consumer state
        // from a second thread) — the rejoin happens on the next consume.
        Assert.Equal(1, fake.JoinGroupCount);

        var result = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(2, fake.JoinGroupCount);
        Assert.NotNull(result);
        Assert.Equal(0, result!.Offset); // rejoined, no commits exist → back to earliest

        // UnknownMemberId means the broker forgot us: the rejoin must ask for a fresh
        // member id instead of reusing the stale one.
        var memberIds = fake.IssuedMemberIds;
        if (errorCode == SurgewaveErrorCode.UnknownMemberId)
            Assert.NotEqual(memberIds[0], memberIds[1]);
        else
            Assert.Equal(memberIds[0], memberIds[1]);
    }

    [Fact]
    public async Task RejoinNeverRunsWhileTheConsumerIsInsideAnInFlightFetch()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, 20);

        var fetchHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFetch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fake.OnFetchAsync = async req =>
        {
            if (req.Offset == 10)
            {
                fetchHeld.TrySetResult();
                await releaseFetch.Task;
            }
        };

        await using var consumer = await CreateConsumerAsync(fake);
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 10; i++)
        {
            var r = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, r!.Offset);
        }

        // Block the consumer inside the fetch for the next batch, then signal a rebalance.
        var boundaryConsume = consumer.ConsumeAsync(TimeSpan.FromSeconds(5));
        await fetchHeld.Task.WaitAsync(TestTimeout);

        await FailHeartbeatAndAwaitProcessingAsync(fake, SurgewaveErrorCode.RebalanceInProgress);

        // No rejoin may happen while the consumer thread sits in the fetch — that was
        // the race that let a stale result overwrite the rejoined position (#116).
        Assert.Equal(1, fake.JoinGroupCount);

        releaseFetch.SetResult();
        var boundary = await boundaryConsume.WaitAsync(TestTimeout);

        // The in-flight batch completes normally (it was fetched for the still-valid
        // position); the rejoin runs at the start of the following consume.
        Assert.Equal(10, boundary!.Offset);
        Assert.Equal(1, fake.JoinGroupCount);

        var afterRejoin = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(2, fake.JoinGroupCount);
        Assert.Equal(0, afterRejoin!.Offset);
    }

    [Fact]
    public async Task RepeatedRejoins_DoNotLeakHeartbeatLoops()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, 50);

        var consumer = await CreateConsumerAsync(fake);
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int rebalance = 0; rebalance < 3; rebalance++)
        {
            await FailHeartbeatAndAwaitProcessingAsync(fake, SurgewaveErrorCode.RebalanceInProgress);
            _ = await consumer.ConsumeAsync(ConsumeTimeout);
        }
        Assert.Equal(4, fake.JoinGroupCount); // initial join + 3 rejoins

        await consumer.DisposeAsync();

        // Every leaked loop would keep heartbeating on its never-cancelled token: after
        // dispose the count must be frozen. Two heartbeat intervals of slack.
        var afterDispose = fake.HeartbeatCount;
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.Equal(afterDispose, fake.HeartbeatCount);
    }

    [Fact]
    public async Task AutoCommit_IsDrivenByConsume_NotByABackgroundTimer()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, 5);

        await using var consumer = await CreateConsumerAsync(fake, enableAutoCommit: true);
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        // No poll yet — nothing may commit, no matter how much time passes (a timer task
        // would read the position dictionary from a second thread, #116).
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Empty(fake.CommitRequests);

        _ = await consumer.ConsumeAsync(ConsumeTimeout);
        _ = await consumer.ConsumeAsync(ConsumeTimeout);

        // Consuming drives the commit; it reflects consumed positions only.
        Assert.NotEmpty(fake.CommitRequests);
        Assert.True(fake.CommitRequests[^1].Offset <= 2);
    }

    [Fact]
    public async Task AutoCommit_CommitsFinalPositionOnDispose()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, 5);

        var consumer = await CreateConsumerAsync(fake, enableAutoCommit: true);
        // A long interval means no poll-driven commit can fire during the test — only
        // the final commit on dispose can produce one.
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 3; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);

        await consumer.DisposeAsync();

        // Without a background timer, closing must still persist the progress made
        // since the last poll-driven commit (#116).
        Assert.Equal(3, fake.CommitRequests[^1].Offset);
    }
}
