using Kuestenlogik.Surgewave.Core.Observability;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Coverage for the zero-cost hot-path gate: <see cref="ISurgewaveBrokerObservability.HasSubscribers"/>
/// must flip as observers join and leave, and the
/// <see cref="SurgewaveObservabilityExtensions.AddSurgewaveBrokerObservability"/> DI extension
/// must honour <c>Surgewave:Observability:Enabled</c>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ObservabilityHasSubscribersTests
{
    [Fact]
    public void HasSubscribers_is_false_until_first_observer_subscribes()
    {
        var observability = new SurgewaveBrokerObservability(NullLogger<SurgewaveBrokerObservability>.Instance);
        Assert.False(observability.HasSubscribers);
    }

    [Fact]
    public async Task HasSubscribers_flips_true_while_an_observer_is_active()
    {
        var observability = new SurgewaveBrokerObservability(NullLogger<SurgewaveBrokerObservability>.Instance);
        using var cts = new CancellationTokenSource();

        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in observability.ObserveAsync(cts.Token))
                {
                    // stays in the enumeration until cancellation
                }
            }
            catch (OperationCanceledException) { /* expected */ }
        }, cts.Token);

        // ObserveAsync registers the subscription before the first yield;
        // give the worker thread a moment to enter the lock + update the
        // volatile counter.
        await WaitForAsync(() => observability.HasSubscribers, TimeSpan.FromSeconds(2));
        Assert.True(observability.HasSubscribers);

        await cts.CancelAsync();
        await consumer;

        // Finally block unregisters the subscription and decrements the counter.
        await WaitForAsync(() => !observability.HasSubscribers, TimeSpan.FromSeconds(2));
        Assert.False(observability.HasSubscribers);
    }

    [Fact]
    public async Task HasSubscribers_reflects_multiple_concurrent_observers()
    {
        var observability = new SurgewaveBrokerObservability(NullLogger<SurgewaveBrokerObservability>.Instance);

        using var ctsA = new CancellationTokenSource();
        using var ctsB = new CancellationTokenSource();

        var a = Task.Run(async () =>
        {
            try { await foreach (var _ in observability.ObserveAsync(ctsA.Token)) { } }
            catch (OperationCanceledException) { }
        }, ctsA.Token);
        var b = Task.Run(async () =>
        {
            try { await foreach (var _ in observability.ObserveAsync(ctsB.Token)) { } }
            catch (OperationCanceledException) { }
        }, ctsB.Token);

        await WaitForAsync(() => observability.HasSubscribers, TimeSpan.FromSeconds(2));
        Assert.True(observability.HasSubscribers);

        await ctsA.CancelAsync();
        await a;
        // B is still observing.
        await WaitForAsync(() => observability.HasSubscribers, TimeSpan.FromSeconds(2));
        Assert.True(observability.HasSubscribers);

        await ctsB.CancelAsync();
        await b;
        await WaitForAsync(() => !observability.HasSubscribers, TimeSpan.FromSeconds(2));
        Assert.False(observability.HasSubscribers);
    }

    [Fact]
    public void AddSurgewaveBrokerObservability_registers_by_default()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build(); // no Surgewave:Observability section

        services.AddSurgewaveBrokerObservability(config);
        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<ISurgewaveBrokerObservability>());
        Assert.NotNull(provider.GetService<SurgewaveBrokerObservability>());
    }

    [Fact]
    public void AddSurgewaveBrokerObservability_skips_registration_when_disabled()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Observability:Enabled"] = "false"
            })
            .Build();

        services.AddSurgewaveBrokerObservability(config);
        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<ISurgewaveBrokerObservability>());
        Assert.Null(provider.GetService<SurgewaveBrokerObservability>());
    }

    [Fact]
    public void AddSurgewaveBrokerObservability_honours_custom_SubscriberCapacity()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:Observability:SubscriberCapacity"] = "256"
            })
            .Build();

        services.AddSurgewaveBrokerObservability(config);
        using var provider = services.BuildServiceProvider();

        // Capacity is a private implementation detail, but the options can be inspected
        // directly — if the config binding works, the capacity makes it to
        // SurgewaveObservabilityOptions and downstream into the constructed channel.
        var opts = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SurgewaveObservabilityOptions>>().Value;
        Assert.Equal(256, opts.SubscriberCapacity);
    }

    private static async Task WaitForAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(20);
        }
    }
}
