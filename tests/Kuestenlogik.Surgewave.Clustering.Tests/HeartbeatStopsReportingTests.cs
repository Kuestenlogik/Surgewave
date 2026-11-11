using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A heartbeat manager that has been stopped reports no further peer failed.
///
/// <para>One health check goes through every watched peer and awaits the failure handler of each it finds failed.
/// The stop was only looked at between two checks: a check held up in the handler of one peer went on to the next
/// peer when that handler returned, however long ago the manager had been stopped, and raised that peer's failure on
/// a broker that was shutting down — on a controller that had stopped already, see
/// <see cref="StoppedControllerDecidesNothingTests"/>.</para>
/// </summary>
public sealed class HeartbeatStopsReportingTests
{
    private const int LocalBroker = 1;

    [Fact(Timeout = 60_000)]
    public async Task AStopDuringTheHandlingOfOneFailure_ReportsNoFurtherPeerFailed()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var config = new ClusteringConfig { BrokerId = LocalBroker, Host = "127.0.0.1", Port = 9092 };

        await using var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, new ClusterState(), config);

        // Two watched peers, both past the failure count: the next health check finds both failed.
        foreach (var peer in new[] { 2, 3 })
        {
            _ = heartbeats.ProcessHeartbeat(new HeartbeatRequest(peer, 0, 0, LocalBroker, 0));
            for (var i = 0; i < config.MaxHeartbeatFailures; i++)
                heartbeats.AllBrokerHealth[peer].RecordFailure();
        }

        var reported = new List<int>();
        var handling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        heartbeats.OnBrokerFailed += async (_, e) =>
        {
            lock (reported) reported.Add(e.BrokerId);
            handling.TrySetResult();
            await handled.Task;
        };

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var check = heartbeats.CheckBrokerHealthAsync(stop.Token);

        // Stopped while the first failure is being handled, as a broker is whose shutdown ends that handling.
        await handling.Task.WaitAsync(cancellationToken);
        await stop.CancelAsync();
        handled.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => check);

        var first = Assert.Single(reported);
        var other = first == 2 ? 3 : 2;
        Assert.True(heartbeats.AllBrokerHealth[other].IsAlive,
            $"broker {other} was marked failed by a heartbeat manager that had been stopped");
    }
}
