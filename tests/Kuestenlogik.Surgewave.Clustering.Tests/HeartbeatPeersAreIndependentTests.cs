using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A peer's heartbeat does not wait for another peer's (#251).
/// </summary>
/// <remarks>
/// A round of heartbeats ended when every peer had answered or failed, and the next round began an interval after
/// that. A peer that took its time — a host that drops packets instead of refusing the connection, a broker that
/// accepts and does not answer — held the round for as long as its connect attempts or the heartbeat timeout last,
/// and the peers that had answered at once got their next heartbeat that much later: later than the timeout they are
/// given, so they were reported failed, and the controller took them out of the ISRs and moved their leaderships.
/// The slow peer here accepts the connection and says nothing, for a timeout far longer than the test waits.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class HeartbeatPeersAreIndependentTests
{
    private const int LocalBroker = 1;
    private const int SilentPeer = 2;
    private const int LivePeer = 3;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task APeerThatAnswersIsSentItsHeartbeatsWhileAnotherDoesNotAnswer()
    {
        var ct = TestContext.Current.CancellationToken;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var silent = new TcpListener(IPAddress.Loopback, 0);
        using var live = new TcpListener(IPAddress.Loopback, 0);
        silent.Start();
        live.Start();

        var held = new TaskCompletionSource<TcpClient>(TaskCreationOptions.RunContinuationsAsynchronously);
        var silentPeer = HoldAsync(silent, held, stop.Token);
        var answered = new HeartbeatCount();
        var livePeer = AnswerAsync(live, answered, stop.Token);

        var state = new ClusterState();
        state.AddBroker(new BrokerNode { BrokerId = LocalBroker, Host = "127.0.0.1", Port = 9092, ReplicationPort = 10092 });
        state.AddBroker(new BrokerNode { BrokerId = SilentPeer, Host = "127.0.0.1", Port = 9093, ReplicationPort = Port(silent) });
        state.AddBroker(new BrokerNode { BrokerId = LivePeer, Host = "127.0.0.1", Port = 9094, ReplicationPort = Port(live) });

        var heartbeats = new HeartbeatManager(
            NullLogger<HeartbeatManager>.Instance, state,
            new ClusteringConfig
            {
                BrokerId = LocalBroker,
                Host = "127.0.0.1",
                Port = 9092,
                HeartbeatIntervalMs = 50,
                // What the silent peer is given to answer, and what it holds a round for where a round waits for it.
                HeartbeatTimeoutMs = 120_000,
                MaxHeartbeatFailures = int.MaxValue,
            });
        var failed = new List<int>();
        heartbeats.OnBrokerFailed += (_, e) =>
        {
            lock (failed)
                failed.Add(e.BrokerId);
            return Task.CompletedTask;
        };

        try
        {
            await heartbeats.StartAsync(ct);

            // The silent peer has the connection and the heartbeat sent on it, and answers neither.
            using var connection = await held.Task.WaitAsync(Bound, ct);

            // The live peer goes on getting its heartbeats, an interval apart.
            await answered.FurtherAsync(5).WaitAsync(Bound, ct);

            await heartbeats.CheckBrokerHealthAsync(ct);
            Assert.True(heartbeats.IsBrokerAlive(LivePeer));
            lock (failed)
                Assert.Empty(failed);
        }
        finally
        {
            await heartbeats.DisposeAsync();
            await stop.CancelAsync();
            silent.Stop();
            live.Stop();
            await Task.WhenAll(silentPeer, livePeer);
        }
    }

    private static int Port(TcpListener listener) => ((IPEndPoint)listener.LocalEndpoint).Port;

    // Accepts the first connection and keeps it, unanswered.
    private static async Task HoldAsync(TcpListener listener, TaskCompletionSource<TcpClient> held, CancellationToken ct)
    {
        try
        {
            held.TrySetResult(await listener.AcceptTcpClientAsync(ct));
        }
        catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
        {
            // The test is over.
        }
    }

    // Accepts one connection and answers every heartbeat on it, as a broker's replication server does.
    private static async Task AnswerAsync(TcpListener listener, HeartbeatCount answered, CancellationToken ct)
    {
        try
        {
            using var client = await listener.AcceptTcpClientAsync(ct);
            var stream = client.GetStream();
            var size = new byte[4];

            while (true)
            {
                await stream.ReadExactlyAsync(size, ct);
                var request = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
                await stream.ReadExactlyAsync(request, ct);

                // [size][correlation id][broker id][broker epoch][timestamp][is controller]; the request's
                // correlation id follows its API key and version.
                var response = new byte[4 + 21];
                BinaryPrimitives.WriteInt32BigEndian(response, 21);
                request.AsSpan(4, 4).CopyTo(response.AsSpan(4));
                BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(8), LivePeer);
                BinaryPrimitives.WriteInt64BigEndian(response.AsSpan(16), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                await stream.WriteAsync(response, ct);

                answered.Add();
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
        {
            // The test is over, or the manager closed its connection.
        }
    }

    private sealed class HeartbeatCount
    {
        private readonly Lock _lock = new();
        private int _awaited;
        private TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completed by the <paramref name="count"/>-th heartbeat answered from now on.</summary>
        public Task FurtherAsync(int count)
        {
            lock (_lock)
            {
                _reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _awaited = count;
                return _reached.Task;
            }
        }

        public void Add()
        {
            lock (_lock)
            {
                if (_awaited > 0 && --_awaited == 0)
                    _reached.TrySetResult();
            }
        }
    }
}
