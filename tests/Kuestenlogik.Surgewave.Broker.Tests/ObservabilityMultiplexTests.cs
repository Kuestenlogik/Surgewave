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

        var a = CollectAsync(observability, count: 3);
        var b = CollectAsync(observability, count: 3);
        // Subscribers register inside CollectAsync; give both a moment to
        // enter ObserveAsync before we start publishing.
        await Task.Delay(50);

        observability.Publish(MakeEvent(1));
        observability.Publish(MakeEvent(2));
        observability.Publish(MakeEvent(3));

        var aResults = await a;
        var bResults = await b;

        Assert.Equal([1L, 2L, 3L], aResults.Select(e => e.Offset!.Value));
        Assert.Equal([1L, 2L, 3L], bResults.Select(e => e.Offset!.Value));
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

        // Start a subscription that reads exactly one event, then parks.
        // Anything beyond capacity has to get dropped by the writer.
        using var cts = new CancellationTokenSource();
        var readOne = Task.Run(async () =>
        {
            await foreach (var ev in observability.ObserveAsync(cts.Token))
            {
                return ev;
            }
            return null!;
        }, cts.Token);

        // Let the subscription register before publishing.
        await Task.Delay(50);

        // Publish well beyond the 4-slot window. Every call must return
        // synchronously — Publish never awaits the channel, the
        // DropOldest policy discards the oldest buffered event.
        for (var i = 0; i < 100; i++)
        {
            observability.Publish(MakeEvent(i));
        }

        var first = await readOne;
        Assert.NotNull(first);
        cts.Cancel();
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

        await Task.Delay(50);
        cts.Cancel();
        await consumer; // no exception propagates past the cancellation

        // After cancellation, publishing must not throw even though the
        // subscriber list still contains stale entries briefly — the
        // finally-block in ObserveAsync removes them, but Publish must be
        // tolerant of a writer that completed in parallel.
        observability.Publish(MakeEvent(1));
    }

    private static Task<List<SurgewaveBrokerEvent>> CollectAsync(
        ISurgewaveBrokerObservability observability, int count)
    {
        return Task.Run(async () =>
        {
            var list = new List<SurgewaveBrokerEvent>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await foreach (var ev in observability.ObserveAsync(cts.Token))
            {
                list.Add(ev);
                if (list.Count >= count) break;
            }
            return list;
        });
    }
}
