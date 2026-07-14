using Kuestenlogik.Surgewave.Clustering.Cluster;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc5 — the SHARED controller-epoch fence. Both the Kafka-wire InterBrokerApiHandler and the
/// native ClusterStateInterBrokerService fence controller pushes through
/// <see cref="ClusterState.TryAdvanceControllerEpoch(int,int,ControllerPushWire?)"/>, so a stale push on either wire is rejected
/// against the epoch the OTHER wire may already have advanced (the pre-Inc5 Kafka handler fenced on
/// a private field and could regress a natively-advanced epoch during split-brain failover).
/// </summary>
public class ControllerEpochFenceTests
{
    [Fact]
    public void StaleEpoch_IsRejected_AndStateUnchanged()
    {
        var state = new ClusterState { ControllerId = 9, ControllerEpoch = 6 };

        // A delayed push from a demoted controller (epoch 5 < 6) must not regress anything.
        Assert.False(state.TryAdvanceControllerEpoch(controllerId: 4, controllerEpoch: 5));
        Assert.Equal(9, state.ControllerId);
        Assert.Equal(6, state.ControllerEpoch);
    }

    [Fact]
    public void EqualEpoch_IsAccepted()
    {
        // Kafka semantics: only strictly older epochs are fenced (re-pushes at the same epoch pass).
        var state = new ClusterState { ControllerId = 1, ControllerEpoch = 3 };

        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 1, controllerEpoch: 3));
        Assert.Equal(3, state.ControllerEpoch);
    }

    [Fact]
    public void NewerEpoch_AdvancesControllerIdAndEpoch()
    {
        var state = new ClusterState { ControllerId = 1, ControllerEpoch = 3 };

        Assert.True(state.TryAdvanceControllerEpoch(controllerId: 7, controllerEpoch: 4));
        Assert.Equal(7, state.ControllerId);
        Assert.Equal(4, state.ControllerEpoch);
    }

    [Fact]
    public async Task ConcurrentAdvances_NeverRegress()
    {
        var state = new ClusterState();

        // Hammer the fence from many threads with mixed epochs; the final epoch must be the maximum
        // ever accepted — a lost check-then-set interleaving would let a lower epoch overwrite it.
        var epochs = Enumerable.Range(1, 200).ToArray();
        await Task.WhenAll(epochs.Select(e => Task.Run(() => state.TryAdvanceControllerEpoch(e, e))));

        Assert.Equal(200, state.ControllerEpoch);
        Assert.Equal(200, state.ControllerId);
    }
}
