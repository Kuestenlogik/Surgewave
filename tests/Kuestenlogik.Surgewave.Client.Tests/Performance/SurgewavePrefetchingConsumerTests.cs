using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Client.Tests.Fakes;
using Xunit;

// NOT ...Tests.Performance to avoid shadowing relative references the way a
// ...Tests.Consumer namespace shadowed Client.Consumer.
namespace Kuestenlogik.Surgewave.Client.Tests.Components;

/// <summary>
/// Deterministic semantics tests for <see cref="SurgewavePrefetchingConsumer"/> (#102):
/// the fetcher runs ahead of consumption and messages are handed out strictly in
/// offset order from the buffered batches.
/// </summary>
public class SurgewavePrefetchingConsumerTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task FetcherRunsAheadOfConsumption_AndHandsOutInStrictOffsetOrder()
    {
        var fake = new FakeSurgewaveTransport();
        for (int i = 0; i < 100; i++)
            fake.Append("t", 0, key: null, new byte[100]); // ~128 bytes/message on the wire

        // Signals as soon as the background fetcher asks for an offset beyond the
        // first batch — before the test consumed anything at all.
        var fetchedAhead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fake.OnFetchAsync = req =>
        {
            if (req.Offset > 0) fetchedAhead.TrySetResult();
            return Task.CompletedTask;
        };

        await using var client = new SurgewaveNativeClient(fake);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        // ~30 messages per fetch → several batches for 100 messages.
        await using var consumer = new SurgewavePrefetchingConsumer(
            client, "t", 0, startOffset: 0, maxBytesPerFetch: 3900);

        // Prefetch happens without any consumption.
        await fetchedAhead.Task.WaitAsync(TestTimeout);

        for (int i = 0; i < 100; i++)
        {
            var msg = await consumer.ConsumeAsync(TestContext.Current.CancellationToken)
                .AsTask().WaitAsync(TestTimeout);
            Assert.NotNull(msg);
            Assert.Equal(i, msg!.Offset);
        }
    }

    /// <summary>
    /// The fetcher swallowed every failure and asked again every 100 ms, on a connection nothing would be answered
    /// on any more, and a caller waited for a message with no end (#289).
    /// </summary>
    [Fact]
    public async Task WhenTheConnectionIsLost_WhatWasFetchedIsHandedOut_AndTheNextCallFailsWithWhy()
    {
        var ct = TestContext.Current.CancellationToken;
        var fake = new FakeSurgewaveTransport();
        for (int i = 0; i < 3; i++)
            fake.Append("t", 0, key: null, new byte[100]);

        // The fetch behind the three messages finds the connection gone.
        fake.OnFetchAsync = req =>
        {
            if (req.Offset >= 3) fake.SimulateConnectionLoss();
            return Task.CompletedTask;
        };

        await using var client = new SurgewaveNativeClient(fake);
        await client.ConnectAsync(ct);
        await using var consumer = new SurgewavePrefetchingConsumer(client, "t", 0, startOffset: 0);

        for (int i = 0; i < 3; i++)
        {
            var msg = await consumer.ConsumeAsync(ct).AsTask().WaitAsync(TestTimeout, ct);
            Assert.Equal(i, msg!.Offset);
        }

        var lost = await Assert.ThrowsAsync<IOException>(() => consumer.ConsumeAsync(ct).AsTask().WaitAsync(TestTimeout, ct));
        Assert.Contains("connection loss", lost.Message, StringComparison.Ordinal);

        // And every call after it, the ones that do not wait as well.
        Assert.Throws<IOException>(() => consumer.Consume());
        Assert.Throws<IOException>(() => consumer.ConsumeBatch());
        await Assert.ThrowsAsync<IOException>(() => consumer.ConsumeBatchAsync(ct).AsTask().WaitAsync(TestTimeout, ct));
    }
}
