using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A leader reports an ISR change until the controller took it (#230). It added a caught-up follower to its own view
/// first and reported once, fire-and-forget: a report lost — no controller known yet, or one unreachable — was never
/// sent again, since the leader's view already had the follower, and the controller kept the leader alone in the ISR.
/// A controlled shutdown then found no in-sync successor and elected uncleanly.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class IsrReportTests
{
    private const int Leader = 0;
    private const int Follower = 1;

    [Fact]
    public async Task AnIsrChangeTheControllerDidNotTakeIsReportedAgain()
    {
        var controller = new Controller(answers: [false, true]);
        var (rm, tp) = NewLeader(controller);

        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 0); // caught up: joins the ISR, the report is lost
        await rm.IsrReportOf(tp);
        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 0); // no change in the leader's view, reported again
        await rm.IsrReportOf(tp);
        rm.UpdateFollowerFetchPosition(tp, Follower, fetchOffset: 0); // the controller has it: nothing more to report
        await rm.IsrReportOf(tp);

        Assert.Equal(2, controller.Reports.Count);
        Assert.Equal([Leader, Follower], controller.Reports[1].Order());
    }

    private static (ReplicaManager Rm, TopicPartition Tp) NewLeader(IIsrChangeNotifier controller)
    {
        var config = new ClusteringConfig { BrokerId = Leader, Host = "localhost", Port = 9092 };
        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = Leader, Host = "localhost", Port = 9092 });
        state.AddBroker(new BrokerNode { BrokerId = Follower, Host = "localhost", Port = 9093 });
        var logs = new LogManager(Path.Combine(Path.GetTempPath(), $"surgewave-isr-report-{Guid.NewGuid():N}"), new MemoryLogSegmentFactory());
        var rm = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config,
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport(), isrChangeNotifier: controller);

        var tp = new TopicPartition { Topic = "isr-report", Partition = 0 };
        state.AssignReplicas(tp, [Leader, Follower]);
        state.ElectLeader(tp, Leader);
        state.UpdateIsr(tp, [Leader]);
        rm.BecomeLeaderAsync(tp, leaderEpoch: 1, CancellationToken.None).GetAwaiter().GetResult();
        return (rm, tp);
    }

    // A controller that takes or loses each report as told, and keeps the ISRs it was sent.
    private sealed class Controller(bool[] answers) : IIsrChangeNotifier
    {
        public List<IReadOnlyList<int>> Reports { get; } = [];

        public Task<bool> NotifyIsrChangedAsync(TopicPartition tp, int leaderId, int leaderEpoch, IReadOnlyList<int> isr, CancellationToken ct = default)
        {
            lock (Reports)
            {
                Reports.Add(isr);
                return Task.FromResult(answers[Math.Min(Reports.Count, answers.Length) - 1]);
            }
        }
    }
}
