using Kuestenlogik.Surgewave.Broker.Hosting;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The embedded Connect workers of a cluster's brokers do not form a group: only the one on the broker that leads the
/// stores' partitions has the stored connectors and pipelines (#282). A broker that runs the worker as one of several
/// says so at every start; a single broker, and a broker without the worker, have nothing to say.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class EmbeddedConnectInClusterTests
{
    [Theory]
    [InlineData("1@broker-1:10092,2@broker-2:10092,3@broker-3:10092", "")]
    [InlineData("", "1@broker-1:10092,2@broker-2:10092,3@broker-3:10092")]
    [InlineData("1@broker-1:10092, 2@broker-2:10092", "1@broker-1:10092")]
    public void TheWorkerOnABrokerOfSeveral_IsSaid(string clusterNodes, string voters)
    {
        var config = new BrokerConfig { ClusterNodes = clusterNodes, ControllerQuorumVoters = voters };
        config.Connect.Enabled = true;

        Assert.True(EmbeddedConnectInCluster.Applies(config));
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("1@broker-1:10092", "1@broker-1:10092")]
    [InlineData(" , ", "")]
    public void TheWorkerOnASingleBroker_IsNot(string clusterNodes, string voters)
    {
        var config = new BrokerConfig { ClusterNodes = clusterNodes, ControllerQuorumVoters = voters };
        config.Connect.Enabled = true;

        Assert.False(EmbeddedConnectInCluster.Applies(config));
    }

    [Fact]
    public void ABrokerOfSeveralWithoutTheWorker_IsNot()
    {
        var config = new BrokerConfig
        {
            ClusterNodes = "1@broker-1:10092,2@broker-2:10092,3@broker-3:10092",
            ControllerQuorumVoters = "1@broker-1:10092,2@broker-2:10092,3@broker-3:10092",
        };
        config.Connect.Enabled = false;

        Assert.False(EmbeddedConnectInCluster.Applies(config));
    }
}
