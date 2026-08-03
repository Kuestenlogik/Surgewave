using System.Threading.Channels;
using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Coverage for the multiplexer mechanics of <see cref="SurgewaveBrokerObservability"/>
/// — separate from the pipeline-wiring tests, these tests exercise the
/// channel/subscription behaviour directly so we don't need the broker
/// stood up to pin down the drop-policy, fan-out, and cancellation paths.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ObservabilityMultiplexTests
{
    private static SurgewaveBrokerEvent MakeEvent(long offset) => new(
        SurgewaveBrokerEventKind.Produced,
        Topic: "t",
        Partition: 0,
        Offset: offset,
        Principal: null,
        RejectReason: null,
        Consumers: null,
        Key: null,
        Value: null,
        Timestamp: DateTimeOffset.UtcNow);

    [Fact]
    public async Task MultipleSubscribersEachReceiveEveryEvent()
    {
        var observability = new SurgewaveBrokerObservability(NullLogger<SurgewaveBrokerObservability>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var a = observability.ObserveAsync(cts.Token).GetAsyncEnumerator();
        await using var b = observability.ObserveAsync(cts.Token).GetAsyncEnumerator();

        // Starting the enumerators registers both subscriptions: ObserveAsync adds itself to the
        // subscriber list before its first await, so MoveNextAsync has already registered by the
        // time it returns its (still pending) task. The previous version slept 50 ms and hoped —
        // which held until the suite got busy enough to miss the window, and then a subscriber that
        // registered after the publish saw nothing.
        var nextA = a.MoveNextAsync();
        var nextB = b.MoveNextAsync();
        Assert.True(observability.HasSubscribers);

        observability.Publish(MakeEvent(1));
        observability.Publish(MakeEvent(2));
        observability.Publish(MakeEvent(3));

        var aResults = await DrainAsync(a, nextA, count: 3);
        var bResults = await DrainAsync(b, nextB, count: 3);

        Assert.Equal([1L, 2L, 3L], aResults);
        Assert.Equal([1L, 2L, 3L], bResults);
    }

    [Fact]
    public async Task DropOldestKeepsBrokerUnblockedWhenSubscriberIsSlow()
    {
        // Tiny per-subscriber capacity so we can force drops without
        // pushing a million events. Publisher never blocks — that's the
        // whole point of DropOldest.
        var observability = new SurgewaveBrokerObservability(
            NullLogger<SurgewaveBrokerObservability>.Instance,
            subscriberCapacity: 4);

        // A subscription that takes one event and then parks. Anything beyond capacity has to get
        // dropped by the writer.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await using var subscriber = observability.ObserveAsync(cts.Token).GetAsyncEnumerator();

        // Registers the subscription — deterministically, rather than by sleeping (see the
        // multiplex test above).
        var firstMove = subscriber.MoveNextAsync();
        Assert.True(observability.HasSubscribers);

        // Publish well beyond the 4-slot window. Every call must return
        // synchronously — Publish never awaits the channel, the
        // DropOldest policy discards the oldest buffered event.
        for (var i = 0; i < 100; i++)
        {
            observability.Publish(MakeEvent(i));
        }

        Assert.True(await firstMove);
        Assert.NotNull(subscriber.Current);
    }

    [Fact]
    public async Task CancellationUnsubscribesCleanly()
    {
        var observability = new SurgewaveBrokerObservability(NullLogger<SurgewaveBrokerObservability>.Instance);

        using var cts = new CancellationTokenSource();
        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in observability.ObserveAsync(cts.Token))
                {
                    // never breaks via enumeration — cancellation ends it
                }
            }
            catch (OperationCanceledException) { /* expected */ }
        }, cts.Token);

        // Wait for the subscription to actually exist instead of assuming 50 ms is enough: on a busy
        // machine it is not, and cancelling before anything registered would test nothing.
        var registrationDeadline = DateTime.UtcNow.AddSeconds(10);
        while (!observability.HasSubscribers && DateTime.UtcNow < registrationDeadline)
            await Task.Delay(5);

        Assert.True(observability.HasSubscribers, "the consumer never registered");

        cts.Cancel();
        await consumer; // no exception propagates past the cancellation

        // After cancellation, publishing must not throw even though the
        // subscriber list still contains stale entries briefly — the
        // finally-block in ObserveAsync removes them, but Publish must be
        // tolerant of a writer that completed in parallel.
        observability.Publish(MakeEvent(1));
    }

    /// <summary>
    /// Reads <paramref name="count"/> offsets from an enumerator whose first move is already in
    /// flight — that pending move is what registered the subscription, so it must be awaited rather
    /// than restarted.
    /// </summary>
    private static async Task<List<long>> DrainAsync(
        IAsyncEnumerator<SurgewaveBrokerEvent> enumerator, ValueTask<bool> pendingMove, int count)
    {
        var offsets = new List<long>(count);

        var hasNext = await pendingMove;
        while (hasNext)
        {
            offsets.Add(enumerator.Current.Offset!.Value);
            if (offsets.Count >= count)
                break;

            hasNext = await enumerator.MoveNextAsync();
        }

        return offsets;
    }
}
