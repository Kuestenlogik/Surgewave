using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The membership half of #162: a heartbeat's offline log directories are recorded for the broker's incarnation, and
/// the controller is told of each directory once — Kafka's broker repeats them in every heartbeat.
/// </summary>
public class ClusterMembershipOfflineLogDirTests
{
    private readonly ClusterMembershipService _service = new(
        new ClusterIdManager(new ClusteringConfig { BrokerId = 0 }, NullLogger<ClusterIdManager>.Instance),
        new ClusterState(),
        NullLogger<ClusterMembershipService>.Instance);

    private readonly List<(int BrokerId, IReadOnlyList<Guid> Dirs)> _reports = [];

    public ClusterMembershipOfflineLogDirTests()
    {
        _service.LogDirsOffline += (brokerId, dirs) => _reports.Add((brokerId, dirs));
    }

    private long Register(int brokerId, Guid incarnation, long index)
    {
        _service.ApplyReplicatedRegistration(brokerId, incarnation, index, "10.0.0.1", 9092, rack: null,
            InterBrokerProtocolFeature.Native, replicationPort: null);
        Assert.True(_service.TryGetBrokerEpoch(brokerId, out var epoch));
        return epoch;
    }

    private static BrokerHeartbeatInput Beat(int brokerId, long epoch, params Guid[] offline)
        => new(brokerId, epoch, CurrentMetadataOffset: epoch, WantFence: false, WantShutDown: false, OfflineLogDirs: offline);

    [Fact]
    public void AReportedDirectoryIsRecordedAndReportedOnce()
    {
        var epoch = Register(1, Guid.NewGuid(), index: 5);
        var dir = Guid.NewGuid();

        Assert.Equal(ClusterRpcStatus.None, _service.Heartbeat(Beat(1, epoch)).Status);
        Assert.False(_service.HasOfflineLogDirs(1));
        Assert.Empty(_reports);

        Assert.Equal(ClusterRpcStatus.None, _service.Heartbeat(Beat(1, epoch, dir)).Status);
        Assert.Equal(ClusterRpcStatus.None, _service.Heartbeat(Beat(1, epoch, dir)).Status);

        Assert.True(_service.HasOfflineLogDirs(1));
        var report = Assert.Single(_reports);
        Assert.Equal(1, report.BrokerId);
        Assert.Equal([dir], report.Dirs);
    }

    [Fact]
    public void ASecondDirectoryIsReportedOnItsOwn()
    {
        var epoch = Register(1, Guid.NewGuid(), index: 5);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();

        _service.Heartbeat(Beat(1, epoch, first));
        _service.Heartbeat(Beat(1, epoch, first, second));

        Assert.Equal(2, _reports.Count);
        Assert.Equal([second], _reports[1].Dirs);
    }

    [Fact]
    public void AStaleEpochHeartbeatRecordsNothing()
    {
        var epoch = Register(1, Guid.NewGuid(), index: 5);

        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, _service.Heartbeat(Beat(1, epoch + 1, Guid.NewGuid())).Status);

        Assert.False(_service.HasOfflineLogDirs(1));
        Assert.Empty(_reports);
    }

    [Fact]
    public void AReplayOfTheSameIncarnationKeepsTheDirectoryOffline()
    {
        var incarnation = Guid.NewGuid();
        var epoch = Register(1, incarnation, index: 5);
        _service.Heartbeat(Beat(1, epoch, Guid.NewGuid()));

        Register(1, incarnation, index: 9);

        Assert.True(_service.HasOfflineLogDirs(1));
    }

    [Fact]
    public void ARestartedBrokerStartsWithItsDirectoriesOnline()
    {
        var epoch = Register(1, Guid.NewGuid(), index: 5);
        _service.Heartbeat(Beat(1, epoch, Guid.NewGuid()));

        Register(1, Guid.NewGuid(), index: 9);

        Assert.False(_service.HasOfflineLogDirs(1));
    }
}
