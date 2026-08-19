using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.Hosting;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Lifecycle;

/// <summary>
/// Hosts a <see cref="GroupCoordinatorSweepService"/> over the lifetime of a
/// <see cref="WebApplication"/>. The service is created and disposed inside the
/// background task so the call site doesn't have to satisfy CA2000 with a
/// long-lived local; ownership stays with this single method.
/// </summary>
internal static class GroupCoordinatorSweepRunner
{
    public static void Start(
        WebApplication app,
        ConsumerGroupV2Coordinator consumerGroupV2,
        ShareGroupCoordinator shareGroup,
        StreamsGroupCoordinator streamsGroup)
    {
        var logger = app.Services.GetRequiredService<ILogger<GroupCoordinatorSweepService>>();
        var stopping = app.Lifetime.ApplicationStopping;

        _ = Task.Run(() => RunAsync(consumerGroupV2, shareGroup, streamsGroup, logger, stopping));
    }

    private static async Task RunAsync(
        ConsumerGroupV2Coordinator consumerGroupV2,
        ShareGroupCoordinator shareGroup,
        StreamsGroupCoordinator streamsGroup,
        ILogger<GroupCoordinatorSweepService> logger,
        CancellationToken stopping)
    {
        await using var sweep = new GroupCoordinatorSweepService(consumerGroupV2, shareGroup, streamsGroup, logger);

        await sweep.StartAsync(stopping).ConfigureAwait(false);
        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* host stopping */ }

        await sweep.StopAsync(CancellationToken.None).ConfigureAwait(false);
    }
}
