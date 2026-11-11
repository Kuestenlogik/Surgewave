using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A broker watches every peer it sends heartbeats to, from the first heartbeat on — not only the peers it knew when
/// its heartbeat manager started, or has heard from since.
///
/// <para>The health record of a peer used to be made at the start, for the brokers in the cluster state then, and
/// otherwise by the first heartbeat the peer answered or sent. A broker that learned of a peer later and never heard
/// from it sent it heartbeats for good, counted none of the failures and never reported it failed: the embedded
/// runtime reads its cluster nodes after the heartbeat manager has started, so a broker whose peer died within its
/// first heartbeat interval never noticed. As the controller it left the dead broker in every ISR and leading its
/// partitions — the promoted controller of <c>ControllerFailoverTests</c>, about one CI run in two.</para>
/// </summary>
public sealed class HeartbeatTracksEveryPeerTests
{
    private const int LocalBroker = 1;
    private const int Peer = 2;

    [Fact(Timeout = 60_000)]
    public async Task APeerLearnedAfterTheStart_ThatNeverAnswers_IsReportedFailed()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        // Bound and not listening: the port is this test's own, and every connection to it is refused.
        using var deadPeer = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        deadPeer.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var deadPort = ((IPEndPoint)deadPeer.LocalEndPoint!).Port;

        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = LocalBroker, Host = "127.0.0.1", Port = 9092, ReplicationPort = 10092 });

        await using var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, state,
            new ClusteringConfig
            {
                BrokerId = LocalBroker,
                Host = "127.0.0.1",
                Port = 9092,
                HeartbeatIntervalMs = 100,
                HeartbeatTimeoutMs = 1_000,
            });

        var failed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        heartbeats.OnBrokerFailed += (_, e) =>
        {
            failed.TrySetResult(e.BrokerId);
            return Task.CompletedTask;
        };

        await heartbeats.StartAsync(cancellationToken);
        Assert.Empty(heartbeats.AllBrokerHealth);

        // Learned after the start, as the embedded runtime learns its cluster nodes and every broker learns of one
        // that registers later.
        state.AddBroker(new BrokerNode { BrokerId = Peer, Host = "127.0.0.1", Port = 9093, ReplicationPort = deadPort });

        Assert.Equal(Peer, await failed.Task.WaitAsync(cancellationToken));
        Assert.False(heartbeats.IsBrokerAlive(Peer));
    }
}
