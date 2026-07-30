using System.Text;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Xunit;

// NOT ...Tests.Consumer: that would shadow the Client.Consumer namespace for every
// test file that references it relatively (e.g. Consumer.AutoOffsetReset).
namespace Kuestenlogik.Surgewave.Client.Tests.Facades;

/// <summary>
/// Deterministic tests for the opt-in background prefetch in
/// <see cref="SurgewaveConsumer{TKey, TValue}"/> (#80 C2). Batches are sized to
/// exactly 10 messages via <c>FetchMaxBytes</c>, and the fake's fetch hook gates
/// the background fetch so ordering claims never rest on real timing.
/// </summary>
public class SurgewaveConsumerPrefetchTests
{
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    // Message on the fake's wire: offset(8) + timestamp(8) + keyLen(4) + valueLen(4)
    // + value(100) + empty header block(4) = 128 bytes → 1300 max bytes yields
    // batches of exactly 10.
    private const int TenMessageFetchBytes = 1300;

    private static Task<SurgewaveConsumer<string, string>> CreateConsumerAsync(
        FakeSurgewaveTransport fake, bool enablePrefetch, string? groupId = null)
        => SurgewaveConsumer<string, string>.CreateAsync(o =>
        {
            o.BootstrapServers = "fake:1";
            o.GroupId = groupId;
            o.AutoOffsetReset = AutoOffsetReset.Earliest;
            o.EnableAutoCommit = false;
            o.FetchMaxBytes = TenMessageFetchBytes;
            o.EnablePrefetch = enablePrefetch;
            o.TransportFactory = () => fake;
        });

    private static void Seed(FakeSurgewaveTransport fake, string topic, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var value = new byte[100];
            Encoding.UTF8.GetBytes($"m{i}").CopyTo(value, 0);
            fake.Append(topic, 0, key: null, value);
        }
    }

    [Fact]
    public async Task Prefetch_FetchesNextBatchInBackground_AndAdoptsItWithoutSyncFetch()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 20);

        var prefetchSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fake.OnFetchAsync = req =>
        {
            if (req.Offset == 10) prefetchSeen.TrySetResult();
            return Task.CompletedTask;
        };

        await using var consumer = await CreateConsumerAsync(fake, enablePrefetch: true);
        consumer.Subscribe("orders");

        var first = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(0, first!.Offset);

        // The fetch for the second batch happens in the background after adopting
        // the first batch — before any further consume call.
        await prefetchSeen.Task.WaitAsync(TestTimeout);

        for (int i = 1; i < 20; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.NotNull(result);
            Assert.Equal(i, result!.Offset);
        }

        // The batch boundary at offset 10 was crossed via the prefetched result:
        // no synchronous (long-polling) fetch was ever issued for it.
        Assert.DoesNotContain(fake.FetchRequests, f => f.Offset == 10 && f.MaxWaitMs > 0);
        Assert.Contains(fake.FetchRequests, f => f.Offset == 10 && f.MaxWaitMs == 0);
    }

    [Fact]
    public async Task Prefetch_ResultIsDiscardedAfterSeek_NeverServed()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 20);

        var prefetchArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePrefetch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fake.OnFetchAsync = async req =>
        {
            if (req.Offset == 10)
            {
                prefetchArrived.TrySetResult();
                await releasePrefetch.Task;
            }
        };

        await using var consumer = await CreateConsumerAsync(fake, enablePrefetch: true);
        consumer.Subscribe("orders");

        var first = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(0, first!.Offset);
        await prefetchArrived.Task.WaitAsync(TestTimeout);

        // Relocate while the prefetch for offset 10 is still in flight, then let it
        // complete: its result must be discarded, not served.
        consumer.Seek("orders", 0, 0);
        releasePrefetch.SetResult();

        var afterSeek = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.NotNull(afterSeek);
        Assert.Equal(0, afterSeek!.Offset);
    }

    [Fact]
    public async Task Prefetch_CommitReflectsOnlyConsumedOffsets()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 20);

        await using var consumer = await CreateConsumerAsync(fake, enablePrefetch: true, groupId: "g1");
        await consumer.SubscribeAsync(TestContext.Current.CancellationToken, "orders");

        for (int i = 0; i < 3; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, result!.Offset);
        }

        // Batch 2 (offsets 10..19) may already be prefetched — the commit must still
        // be consumed+1, never anything the prefetcher has seen.
        await consumer.CommitAsync();
        Assert.Equal(3, fake.CommitRequests[^1].Offset);

        for (int i = 3; i < 16; i++)
        {
            var result = await consumer.ConsumeAsync(ConsumeTimeout);
            Assert.Equal(i, result!.Offset);
        }

        await consumer.CommitAsync();
        Assert.Equal(16, fake.CommitRequests[^1].Offset);
    }

    [Fact]
    public async Task PrefetchDisabledByDefault_BatchBoundaryUsesSynchronousFetch()
    {
        var fake = new FakeSurgewaveTransport();
        Seed(fake, "orders", 20);

        await using var consumer = await CreateConsumerAsync(fake, enablePrefetch: false);
        consumer.Subscribe("orders");

        for (int i = 0; i < 10; i++)
            _ = await consumer.ConsumeAsync(ConsumeTimeout);

        // Give a background fetch time to show up if one were (incorrectly) armed.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(1, fake.FetchCount);

        var boundary = await consumer.ConsumeAsync(ConsumeTimeout);
        Assert.Equal(10, boundary!.Offset);
        Assert.Equal(2, fake.FetchCount);
        Assert.All(fake.FetchRequests, f => Assert.True(f.MaxWaitMs > 0));
    }
}
