using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Hosting;

/// <summary>
/// Periodically sweeps stale members from the three KIP-848-style coordinators
/// (<see cref="ConsumerGroupV2Coordinator"/>, <see cref="ShareGroupCoordinator"/>,
/// <see cref="StreamsGroupCoordinator"/>). Without this background task a member
/// that died right before the rest of the group went silent would only be evicted
/// when the next heartbeat finally arrived — in pathological cases never. The
/// default sweep interval is 30s, which is roughly two-thirds of the per-group
/// stale timeout so empty groups GC quickly without thrashing.
/// </summary>
public sealed class GroupCoordinatorSweepService : IHostedService, IAsyncDisposable
{
    private readonly ConsumerGroupV2Coordinator _consumerGroupV2;
    private readonly ShareGroupCoordinator _shareGroup;
    private readonly StreamsGroupCoordinator _streamsGroup;
    private readonly ILogger _logger;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;

    public GroupCoordinatorSweepService(
        ConsumerGroupV2Coordinator consumerGroupV2,
        ShareGroupCoordinator shareGroup,
        StreamsGroupCoordinator streamsGroup,
        ILogger<GroupCoordinatorSweepService> logger,
        TimeSpan? interval = null)
    {
        _consumerGroupV2 = consumerGroupV2;
        _shareGroup = shareGroup;
        _streamsGroup = streamsGroup;
        _logger = logger;
        _interval = interval ?? TimeSpan.FromSeconds(30);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_loop is not null) return Task.CompletedTask;

        _logger.LogInformation("GroupCoordinatorSweepService started; interval={Interval}", _interval);
        _loop = Task.Run(() => RunAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is { } t)
        {
            try { await t.WaitAsync(cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) { /* shutdown deadline hit */ }
        }
    }

    /// <summary>
    /// Runs one sweep iteration. Public so tests can drive the sweep deterministically
    /// without waiting on the timer.
    /// </summary>
    public void SweepOnce()
    {
        try
        {
            _consumerGroupV2.SweepStaleMembers();
            _shareGroup.SweepStaleMembers();
            _streamsGroup.SweepStaleMembers();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GroupCoordinatorSweepService: sweep iteration failed");
        }
    }

    private async Task RunAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(_interval);
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                SweepOnce();
            }
        }
        catch (OperationCanceledException)
        {
            // Graceful shutdown.
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_cts.IsCancellationRequested) await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is { } t)
        {
            try { await t.ConfigureAwait(false); } catch { /* ignore */ }
        }
        _cts.Dispose();
    }
}
