using System.Net;
using System.Net.NetworkInformation;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Runtime.Tests;

/// <summary>
/// The embedded broker as a test gets it without naming a host listens on loopback only, with both of its
/// listeners (#285): the client listener did, the replication listener a broker of a cluster has bound every
/// interface.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class EmbeddedBrokerListenerTests
{
    [Fact]
    public async Task ABrokerOfAClusterWithTheDefaultHostListensOnLoopbackOnly()
    {
        await using var broker = await SurgewaveRuntime.CreateBuilder()
            .WithCluster()
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync(TestContext.Current.CancellationToken);

        AssertListensOnLoopbackOnly(broker.Port, "client");
        AssertListensOnLoopbackOnly(broker.ReplicationPort, "replication");
    }

    [Fact]
    public async Task ABrokerOfAClusterOnLocalhostWithoutDualModeListensOnLoopbackOnly()
    {
        await using var broker = await SurgewaveRuntime.CreateBuilder()
            .WithHost("localhost")
            .WithCluster()
            .WithStorageEngine(StorageEngines.Memory)
            .Build()
            .StartAsync(TestContext.Current.CancellationToken);

        AssertListensOnLoopbackOnly(broker.Port, "client");
        AssertListensOnLoopbackOnly(broker.ReplicationPort, "replication");
    }

    /// <summary>
    /// The port is the broker's own while it runs, so the machine's listeners on it are the broker's.
    /// </summary>
    private static void AssertListensOnLoopbackOnly(int port, string listener)
    {
        var bound = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners()
            .Where(endpoint => endpoint.Port == port)
            .ToList();

        Assert.True(bound.Count > 0, $"The {listener} listener on port {port} is not listening.");
        Assert.All(bound, endpoint => Assert.True(IPAddress.IsLoopback(endpoint.Address),
            $"The {listener} listener is bound to {endpoint}, which is not loopback."));
    }
}
