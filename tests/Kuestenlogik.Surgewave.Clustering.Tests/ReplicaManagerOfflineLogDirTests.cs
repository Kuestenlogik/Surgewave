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
/// The broker half of #162: once its log directory has failed, a broker takes up no leadership or followership of a
/// partition in it, and as a leader it does not judge its followers by a silence that is its own failure.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ReplicaManagerOfflineLogDirTests : IAsyncDisposable
{
    private const int Self = 1;
    private const int Follower = 2;

    private readonly ClusterState _state = new();
    private readonly LogManager _logs = new(
        Path.Combine(Path.GetTempPath(), $"surgewave-offline-rm-{Guid.NewGuid():N}"),
        new MemoryLogSegmentFactory());
    private readonly ReplicaManager _replicas;
    private readonly TopicPartition _tp = new() { Topic = "orders", Partition = 0 };

    public ReplicaManagerOfflineLogDirTests()
    {
        _replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, _state, _logs,
            new ClusteringConfig { BrokerId = Self, Host = "localhost", Port = 9092 },
            new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());
        _state.AssignReplicas(_tp, [Self, Follower]);
        _state.UpdateIsr(_tp, [Self, Follower]);
    }

    public async ValueTask DisposeAsync()
    {
        await _replicas.DisposeAsync();
        _logs.Dispose();
    }

    private void FailTheDisk() => _logs.LogDirectory.Fail("append to", _tp, new IOException("disk gone"));

    [Fact]
    public async Task ATransitionForAPartitionInAnOfflineDirectoryIsNotCarriedOut()
    {
        _state.ElectLeader(_tp, Follower);
        var epoch = _state.GetPartitionState(_tp)!.LeaderEpoch;
        FailTheDisk();

        await _replicas.BecomeLeaderAsync(_tp, epoch + 1, TestContext.Current.CancellationToken);

        Assert.False(_replicas.IsLeader(_tp));
        Assert.Equal(Follower, _state.GetPartitionState(_tp)!.LeaderBrokerId);
        Assert.Equal(epoch, _state.GetPartitionState(_tp)!.LeaderEpoch);

        await _replicas.BecomeFollowerAsync(_tp, Follower, epoch + 1, TestContext.Current.CancellationToken);

        Assert.Null(_replicas.GetReplica(_tp));
        Assert.Equal(epoch, _state.GetPartitionState(_tp)!.LeaderEpoch);
    }

    [Fact]
    public async Task ALeaderWithAnOnlineDirectoryExpiresASilentFollower()
    {
        // The control for the next test: with no lag allowed at all, a follower is silent at once.
        await LeadWithAFollowerThatReportedAsync();

        _replicas.CheckIsrForPartition(_tp);

        Assert.DoesNotContain(Follower, _state.GetIsrSnapshot(_tp));
    }

    [Fact]
    public async Task ALeaderWithAnOfflineDirectoryLeavesItsIsrToTheController()
    {
        await LeadWithAFollowerThatReportedAsync();
        FailTheDisk();

        _replicas.CheckIsrForPartition(_tp);

        Assert.Equal([Self, Follower], _state.GetIsrSnapshot(_tp));
    }

    private async Task LeadWithAFollowerThatReportedAsync()
    {
        _state.ElectLeader(_tp, Self);
        await _replicas.BecomeLeaderAsync(_tp, leaderEpoch: 1, TestContext.Current.CancellationToken);
        _replicas.UpdateFollowerFetchPosition(_tp, Follower, fetchOffset: 0);
        Assert.Contains(Follower, _state.GetIsrSnapshot(_tp));
        _replicas.ReplicaLagTimeMax = TimeSpan.FromTicks(-1);
    }
}
