using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc6b — the paced lifecycle loop: it registers (retrying while no controller answers), then
/// heartbeats, and drops back to re-registering when the controller reports a stale epoch. Driven
/// against a fake <see cref="IBrokerLifecycleRpc"/> so the state machine is tested without a wire.
/// </summary>
public class BrokerLifecycleLoopTests
{
    private sealed class FakeRpc : IBrokerLifecycleRpc
    {
        private readonly ConcurrentQueue<ClusterRpcStatus> _registerScript;
        private readonly Func<long, ClusterRpcStatus> _heartbeatStatus;
        private long _epoch;

        public int RegisterCalls;
        public int HeartbeatCalls;

        public FakeRpc(IEnumerable<ClusterRpcStatus> registerScript, Func<long, ClusterRpcStatus>? heartbeatStatus = null)
        {
            _registerScript = new(registerScript);
            _heartbeatStatus = heartbeatStatus ?? (_ => ClusterRpcStatus.None);
        }

        public Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct = default)
        {
            Interlocked.Increment(ref RegisterCalls);
            var status = _registerScript.TryDequeue(out var s) ? s : ClusterRpcStatus.None;
            if (status == ClusterRpcStatus.None)
                return Task.FromResult(new BrokerRegistrationOutcome(ClusterRpcStatus.None, Interlocked.Increment(ref _epoch)));
            return Task.FromResult(new BrokerRegistrationOutcome(status, -1));
        }

        public Task<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default)
        {
            Interlocked.Increment(ref HeartbeatCalls);
            var status = _heartbeatStatus(input.BrokerEpoch);
            return Task.FromResult(new BrokerHeartbeatOutcome(status, IsFenced: status != ClusterRpcStatus.None, IsCaughtUp: true, ShouldShutDown: false));
        }
    }

    private static ClusteringConfig Config() => new()
    {
        BrokerId = 2,
        Host = "localhost",
        Port = 9094,
        ReplicationPort = 10094,
        ClusterNodes = "0:localhost:9092", // non-empty so the loop runs
        HeartbeatIntervalMs = 30,
        RebalanceCheckIntervalSeconds = 5,
    };

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (condition())
                return;
            await Task.Delay(15);
        }
        Assert.Fail("condition not met within timeout");
    }

    [Fact]
    public async Task Loop_RetriesRegistrationUntilControllerAnswers_ThenHeartbeats()
    {
        // First two registration attempts see no controller, the third succeeds.
        var rpc = new FakeRpc([ClusterRpcStatus.NotController, ClusterRpcStatus.NotController, ClusterRpcStatus.None]);
        await using var loop = new BrokerLifecycleLoop(rpc, Config(), NullLogger<BrokerLifecycleLoop>.Instance);

        await loop.StartAsync(CancellationToken.None);

        await WaitUntilAsync(() => loop.BrokerEpoch > 0);
        Assert.True(rpc.RegisterCalls >= 3);

        // Once registered it heartbeats.
        await WaitUntilAsync(() => rpc.HeartbeatCalls > 0);
    }

    [Fact]
    public async Task Loop_StaleEpochHeartbeat_DropsBackToReRegister()
    {
        // Registration always succeeds; the first heartbeat reports StaleBrokerEpoch, forcing a
        // re-register (a second RegisterAsync call).
        var firstHeartbeat = true;
        var rpc = new FakeRpc(
            registerScript: [],
            heartbeatStatus: _ =>
            {
                if (firstHeartbeat) { firstHeartbeat = false; return ClusterRpcStatus.StaleBrokerEpoch; }
                return ClusterRpcStatus.None;
            });
        await using var loop = new BrokerLifecycleLoop(rpc, Config(), NullLogger<BrokerLifecycleLoop>.Instance);

        await loop.StartAsync(CancellationToken.None);

        // Registered, one stale heartbeat, then re-register → at least 2 register calls.
        await WaitUntilAsync(() => rpc.RegisterCalls >= 2 && rpc.HeartbeatCalls >= 1);
    }

    [Fact]
    public async Task Loop_Standalone_NoClusterNodes_DoesNotRun()
    {
        var config = Config();
        config.ClusterNodes = "";
        var rpc = new FakeRpc([]);
        await using var loop = new BrokerLifecycleLoop(rpc, config, NullLogger<BrokerLifecycleLoop>.Instance);

        await loop.StartAsync(CancellationToken.None);
        await Task.Delay(150);

        Assert.Equal(0, rpc.RegisterCalls);
        Assert.Equal(0, rpc.HeartbeatCalls);
    }
}
