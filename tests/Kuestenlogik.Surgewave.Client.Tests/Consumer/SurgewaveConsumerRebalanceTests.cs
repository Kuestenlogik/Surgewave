using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

// NOT ...Tests.Consumer: that would shadow the Client.Consumer namespace for every
// test file that references it relatively (e.g. Consumer.AutoOffsetReset).
namespace Kuestenlogik.Surgewave.Client.Tests.Facades;

/// <summary>
/// Pins the fix for the adversarial-review finding on #80 C2: a heartbeat-driven
/// background rebalance that lands while a fetch (or adopted prefetch) is awaited
/// must cause that result to be DISCARDED — it must never overwrite the rejoined
/// position or serve stale records. The fake's fetch hook holds the in-flight fetch
/// open while the rebalance completes, so the race window is exercised
/// deterministically, not by timing luck.
/// <para>
/// The heartbeat error is a SYNTHETIC trigger for the client-internal rejoin path:
/// the real broker currently never emits RebalanceInProgress on heartbeat (see the
/// rebalance-threading issue for the broker-driven flow). What this pins is the
/// discard/no-overwrite invariant of ConsumeAsync, which holds regardless of what
/// eventually invokes the rejoin.
/// </para>
/// </summary>
public class SurgewaveConsumerRebalanceTests
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    private const int TenMessageFetchBytes = 1300; // see SurgewaveConsumerPrefetchTests

    private static Task<SurgewaveConsumer<string, string>> CreateConsumerAsync(
        FakeSurgewaveTransport fake, bool enablePrefetch)
        => SurgewaveConsumer<string, string>.CreateAsync(o =>
        {
            o.BootstrapServers = "fake:1";
            o.GroupId = "g1";
            o.AutoOffsetReset = AutoOffsetReset.Earliest;
            o.EnableAutoCommit = false;
            o.FetchMaxBytes = TenMessageFetchBytes;
            o.EnablePrefetch = enablePrefetch;
            o.HeartbeatIntervalMs = 50; // fast heartbeat so the test can drive a rebalance promptly
            o.TransportFactory = () => fake;
        });

    private static void Seed(FakeSurgewaveTransport fake, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var value = new byte[100];
            Encoding.UTF8.GetBytes($"m{i}").CopyTo(value, 0);
            fake.Append("orders", 0, key: null, value);
        }
    }

    private static long? TryPosition(SurgewaveConsumer<string, string> consumer)
    {
        try { return consumer.Position("orders", 0); }
        catch { return null; } // mid-rebalance the partition is briefly unassigned
    }

    private static async Task DriveRebalanceAsync(FakeSurgewaveTransport fake, SurgewaveConsumer<string, string> consumer)
    {
        fake.NextHeartbeatErrorCode = (ushort)SurgewaveErrorCode.RebalanceInProgress;
        var deadline = DateTime.UtcNow.AddSeconds(5);
        // Rejoin done = a second JoinGroup was served AND the position was re-resolved
        // to earliest (no commits exist), i.e. back to 0.
        while ((fake.JoinGroupCount < 2 || TryPosition(consumer) != 0) && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        Assert.True(fake.JoinGroupCount >= 2, "rebalance did not rejoin in time");
        Assert.Equal(0, TryPosition(consumer));
    }

    [Theory]
    [InlineData(true)]  // race window: awaiting the adopted background prefetch
    [InlineData(false)] // race window: awaiting the synchronous fetch
    public async Task BackgroundRebalance_DuringInFlightFetch_DiscardsStaleResult(bool enablePrefetch)
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

        await using var consumer = await CreateConsumerAsync(fake, enablePrefetch);
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 10; i++)
        {
            var r = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, r!.Offset);
        }

        // This call crosses the batch boundary at offset 10 and blocks inside the
        // held fetch (the prefetch it adopts, or the synchronous one).
        var boundaryConsume = consumer.ConsumeAsync(TimeSpan.FromSeconds(5));
        await fetchHeld.Task.WaitAsync(TestTimeout);

        // While it is suspended, a rebalance rejoins and resets the position to 0.
        await DriveRebalanceAsync(fake, consumer);

        // Now the stale batch (10..19) completes — it must be discarded. Two legal
        // schedules exist: the call was already suspended in the held fetch (result
        // null after the discard), or it started late, after the rebalance, and
        // served offset 0 regularly. Serving anything from the stale batch is the bug.
        releaseFetch.SetResult();
        var boundary = await boundaryConsume.WaitAsync(TestTimeout);
        Assert.True(boundary is null || boundary.Offset == 0,
            $"stale pre-rebalance record served: offset {boundary?.Offset}");

        // The rejoined position was not overwritten: consumption restarts from 0.
        var expectedNext = boundary is null ? 0L : 1L;
        var next = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(next);
        Assert.Equal(expectedNext, next!.Offset);
    }
}
