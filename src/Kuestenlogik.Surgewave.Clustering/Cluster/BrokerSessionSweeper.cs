using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Drives <see cref="ClusterMembershipService.ExpireStaleSessions"/> on a timer, so a
/// broker that stops heartbeating is eventually noticed (#123).
/// </summary>
/// <remarks>
/// <para>
/// The native registration path was request-driven end to end: it learned about a broker
/// when the broker spoke, and nothing ever asked whether one had stopped. This is the
/// missing half — the only part of the mechanism that has to know about time.
/// </para>
/// <para>
/// Separate from the membership service on purpose. That class is the authority on who is
/// registered and at which epoch; giving it a timer would make every test that touches
/// registration also a test about scheduling. Here the loop lives alone, and expiry stays
/// a pure function of a clock reading.
/// </para>
/// <para>
/// Runs on every node rather than only on the controller. On a follower
/// <c>_registrations</c> is empty — the native heartbeat is follower-to-controller only —
/// so the sweep finds nothing and costs a dictionary walk over zero entries. Gating it on
/// leadership would mean tracking leadership changes here for no benefit.
/// </para>
/// </remarks>
public sealed class BrokerSessionSweeper : IAsyncDisposable
{
    private readonly ClusterMembershipService _membership;
    private readonly TimeSpan _sessionTimeout;
    private readonly TimeSpan _sweepInterval;
    private readonly ILogger<BrokerSessionSweeper> _logger;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loop;

    public BrokerSessionSweeper(
        ClusterMembershipService membership,
        TimeSpan sessionTimeout,
        ILogger<BrokerSessionSweeper> logger)
    {
        _membership = membership;
        _sessionTimeout = sessionTimeout;
        _logger = logger;

        // Sweeping far more often than the timeout only adds walks; far less often adds
        // up to a whole interval of latency onto every detection. Half the timeout bounds
        // the added latency at half the timeout, which is the usual trade.
        _sweepInterval = TimeSpan.FromMilliseconds(Math.Max(500, sessionTimeout.TotalMilliseconds / 2));
    }

    /// <summary>Starts the sweep loop. Idempotent.</summary>
    public void Start()
    {
        _loop ??= Task.Run(() => SweepLoopAsync(_cts.Token));
    }

    private async Task SweepLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_sweepInterval);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await timer.WaitForNextTickAsync(ct).ConfigureAwait(false);
                _membership.ExpireStaleSessions(DateTime.UtcNow, _sessionTimeout);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A failed sweep must not end the loop: the next tick tries again, and a
                // detector that stops detecting after one bad pass is worse than none,
                // because nothing says it stopped.
                _logger.LogError(ex, "Broker session sweep failed; the next tick will retry");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);

        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); }
            catch (OperationCanceledException) { /* expected */ }
        }

        _cts.Dispose();
    }
}
