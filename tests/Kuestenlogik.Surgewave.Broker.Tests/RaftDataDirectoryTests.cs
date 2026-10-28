using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The Raft metadata log follows the data directory unless set explicitly (#196). It defaulted to ./data/raft
/// regardless of DataDirectory, so a broker on a fresh data directory replayed an earlier run's metadata and
/// two brokers started from one working directory shared a log.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftDataDirectoryTests
{
    [Fact]
    public void UnsetTheRaftLogLivesInTheDataDirectory()
    {
        var broker = new BrokerConfig { DataDirectory = Path.Combine("brokers", "b1") };
        var clustering = new ClusteringConfig { DataDirectory = Path.Combine("brokers", "b2") };

        Assert.Equal(Path.Combine("brokers", "b1", "raft"), broker.ResolvedRaftDataDirectory);
        Assert.Equal(Path.Combine("brokers", "b2", "raft"), clustering.ResolvedRaftDataDirectory);
    }

    [Fact]
    public void SetExplicitlyItIsUsedAsIs()
    {
        var broker = new BrokerConfig { DataDirectory = "data", RaftDataDirectory = "elsewhere" };
        var clustering = new ClusteringConfig { DataDirectory = "data", RaftDataDirectory = "elsewhere" };

        Assert.Equal("elsewhere", broker.ResolvedRaftDataDirectory);
        Assert.Equal("elsewhere", clustering.ResolvedRaftDataDirectory);
    }
}
