using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// The replication listener binds what <see cref="ClusteringConfig.ReplicationBindAddress"/> names (#285). Unset it
/// is every interface, as before: the brokers of a cluster are on other hosts. A broker whose peers are all on its
/// own host — every broker a test starts — names loopback.
/// </summary>
public sealed class ReplicationBindAddressTests : IAsyncLifetime
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "surgewave-replication-bind-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Unset_TheListenerBindsEveryInterface()
    {
        var config = new ClusteringConfig { ReplicationPort = 10092 };

        Assert.Equal(new IPEndPoint(IPAddress.Any, 10092), config.ReplicationListenEndPoint());
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("::1")]
    [InlineData("10.1.2.3")]
    public void Set_TheListenerBindsThatAddress(string address)
    {
        var config = new ClusteringConfig { ReplicationPort = 10092, ReplicationBindAddress = address };

        Assert.Equal(new IPEndPoint(IPAddress.Parse(address), 10092), config.ReplicationListenEndPoint());
        Assert.DoesNotContain(config.Validate(), e => e.Contains(nameof(ClusteringConfig.ReplicationBindAddress), StringComparison.Ordinal));
    }

    [Fact]
    public void WhatIsNoAddress_IsRefused()
    {
        var config = new ClusteringConfig { ReplicationBindAddress = "eth0" };

        var error = Assert.Single(config.Validate(), e => e.Contains(nameof(ClusteringConfig.ReplicationBindAddress), StringComparison.Ordinal));
        Assert.Contains("eth0", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheStartedServerListensOnTheAddressNamed()
    {
        var tcp = new TcpPeerTransport();
        var state = new ClusterState();
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            Host = "127.0.0.1",
            Port = 9092,
            ReplicationPort = 0,
            ReplicationBindAddress = "127.0.0.1",
        };
        var logs = new LogManager(Path.Combine(_directory, "logs"), new MemoryLogSegmentFactory());
        await using var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, tcp);
        await using var server = new ReplicationServer(NullLogger<ReplicationServer>.Instance, state, logs, replicas, config, tcp);

        await server.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(IPAddress.Loopback, server.BoundEndPoint!.Address);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        try { if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true); }
        catch (IOException) { /* a leftover temp directory is harmless */ }
        return ValueTask.CompletedTask;
    }
}
