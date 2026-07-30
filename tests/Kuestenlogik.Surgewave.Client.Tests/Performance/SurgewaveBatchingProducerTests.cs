using System.Text;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Xunit;

// NOT ...Tests.Performance to avoid shadowing relative references the way a
// ...Tests.Consumer namespace shadowed Client.Consumer.
namespace Kuestenlogik.Surgewave.Client.Tests.Components;

/// <summary>
/// Deterministic semantics tests for <see cref="SurgewaveBatchingProducer"/> (#102)
/// over the in-memory fake transport. Linger is set to one hour throughout so only
/// size limits and explicit flushes trigger sends — no timing dependence.
/// </summary>
public class SurgewaveBatchingProducerTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    private static async Task<SurgewaveNativeClient> CreateClientAsync(FakeSurgewaveTransport fake)
    {
        var client = new SurgewaveNativeClient(fake);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        return client;
    }

    [Fact]
    public async Task ProduceAndWait_ReturnsBaseOffsetPlusIndex_AndBatchesBySize()
    {
        var fake = new FakeSurgewaveTransport();
        await using var client = await CreateClientAsync(fake);
        await using var producer = new SurgewaveBatchingProducer(
            client, "t", 0, maxBatchSize: 5, lingerTime: TimeSpan.FromHours(1), maxInFlight: 1);

        var tasks = new List<Task<long>>();
        for (int i = 0; i < 20; i++)
            tasks.Add(producer.ProduceAndWaitAsync(null, Encoding.UTF8.GetBytes($"v{i}")));

        var offsets = await Task.WhenAll(tasks).WaitAsync(TestTimeout);

        // Each caller gets the broker offset of its own message: baseOffset + index.
        Assert.Equal(Enumerable.Range(0, 20).Select(i => (long)i), offsets);

        // 20 messages, size limit 5, linger 1h → exactly 4 full batches.
        Assert.Equal(4, fake.ProduceCount);
        Assert.All(fake.ProduceRequests, r => Assert.Equal(5, r.MessageCount));

        // FIFO end to end: broker log matches produce order.
        var log = fake.GetLog("t", 0);
        Assert.Equal(20, log.Count);
        for (int i = 0; i < 20; i++)
            Assert.Equal($"v{i}", Encoding.UTF8.GetString(log[i].Value));
    }

    [Fact]
    public async Task Flush_DispatchesPartialBatch_AndPendingProducesComplete()
    {
        var fake = new FakeSurgewaveTransport();
        await using var client = await CreateClientAsync(fake);
        await using var producer = new SurgewaveBatchingProducer(
            client, "t", 0, maxBatchSize: 100, lingerTime: TimeSpan.FromHours(1), maxInFlight: 1);

        var tasks = new List<Task<long>>();
        for (int i = 0; i < 3; i++)
            tasks.Add(producer.ProduceAndWaitAsync(null, Encoding.UTF8.GetBytes($"v{i}")));

        // Under the size limit and linger is 1h — only the flush can dispatch.
        await producer.FlushAsync(TestContext.Current.CancellationToken).WaitAsync(TestTimeout);
        var offsets = await Task.WhenAll(tasks).WaitAsync(TestTimeout);

        Assert.Equal([0L, 1L, 2L], offsets);
        var produce = Assert.Single(fake.ProduceRequests);
        Assert.Equal(3, produce.MessageCount);
    }

    [Fact]
    public async Task MaxInFlightOne_DoesNotDispatchNextBatch_WhileFirstIsInFlight()
    {
        var fake = new FakeSurgewaveTransport();
        var firstBatchGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstBatchArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var produceCalls = 0;
        fake.OnProduceAsync = _ =>
        {
            if (Interlocked.Increment(ref produceCalls) == 1)
            {
                firstBatchArrived.TrySetResult();
                return firstBatchGate.Task;
            }
            return Task.CompletedTask;
        };

        await using var client = await CreateClientAsync(fake);
        await using var producer = new SurgewaveBatchingProducer(
            client, "t", 0, maxBatchSize: 2, lingerTime: TimeSpan.FromHours(1), maxInFlight: 1);

        var tasks = new List<Task<long>>();
        for (int i = 0; i < 4; i++)
            tasks.Add(producer.ProduceAndWaitAsync(null, Encoding.UTF8.GetBytes($"v{i}")));

        // Wait until batch 1 (v0,v1) is actually held at the fake — without this,
        // a starved thread pool could delay the batcher's first iteration past the
        // window below and fail the assert with produceCalls == 0.
        await firstBatchArrived.Task.WaitAsync(TestTimeout);

        // With maxInFlight=1 batch 2 must not be dispatched while batch 1 is
        // un-acked. Give a wrong implementation time to show the second call,
        // then assert it didn't.
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref produceCalls));

        firstBatchGate.SetResult();
        var offsets = await Task.WhenAll(tasks).WaitAsync(TestTimeout);

        Assert.Equal([0L, 1L, 2L, 3L], offsets);
        var log = fake.GetLog("t", 0);
        for (int i = 0; i < 4; i++)
            Assert.Equal($"v{i}", Encoding.UTF8.GetString(log[i].Value));
    }
}
