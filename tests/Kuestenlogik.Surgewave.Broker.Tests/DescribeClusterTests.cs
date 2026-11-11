using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// DescribeCluster reports the cluster as the broker knows it, and every client-facing answer the same cluster
/// id (#131, #203). It reported only the broker asked, and its cluster id fell back to "surgewave-{brokerId}" where
/// Metadata said "surgewave-cluster" — with KIP-1242 a client that sees two ids rebootstraps.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeClusterTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-describecluster-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly BrokerConfig _config = new() { BrokerId = 1, Host = "10.0.0.1", Port = 9092 };
    private readonly MetadataApiHandler _handler;

    public DescribeClusterTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _handler = new MetadataApiHandler(_config, _logManager, NullLogger<MetadataApiHandler>.Instance);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task ABrokerInAClusterReportsEveryBrokerItKnowsTheControllerAndWhichAreFenced()
    {
        var cluster = new ClusterState { ControllerId = 2 };
        cluster.AddBroker(new BrokerNode { BrokerId = 1, Host = "10.0.0.1", Port = 9092 });
        cluster.AddBroker(new BrokerNode { BrokerId = 2, Host = "10.0.0.2", Port = 9092, Rack = "r2" });
        cluster.AddBroker(new BrokerNode { BrokerId = 3, Host = "10.0.0.3", Port = 9092 });
        _handler.SetClusterState(cluster);
        _handler.SetBrokerFencing(id => id == 3);

        var response = await DescribeClusterAsync();

        Assert.Equal(2, response.ControllerId);
        Assert.Collection(response.Brokers,
            b => { Assert.Equal(1, b.BrokerId); Assert.False(b.IsFenced); },
            b => { Assert.Equal(2, b.BrokerId); Assert.Equal("10.0.0.2", b.Host); Assert.Equal("r2", b.Rack); Assert.False(b.IsFenced); },
            b => { Assert.Equal(3, b.BrokerId); Assert.True(b.IsFenced); });
    }

    [Fact]
    public async Task ABrokerOutsideAClusterReportsItselfAsTheController()
    {
        var response = await DescribeClusterAsync();

        var broker = Assert.Single(response.Brokers);
        Assert.Equal(1, broker.BrokerId);
        Assert.Equal(1, response.ControllerId);
    }

    [Theory]
    [InlineData("configured-id", null, "configured-id")]
    [InlineData(null, "AAAAAAAAQAGAAAAAAAAAAQ", "AAAAAAAAQAGAAAAAAAAAAQ")] // the id the cluster generated and shares (#203)
    public async Task DescribeClusterMetadataAndApiVersionsReportTheSameClusterId(string? configured, string? shared, string expected)
    {
        _config.ClusterId = configured;
        if (shared is not null)
            _config.UseClusterIdentity(() => shared);

        var (described, metadata) = await DescribeAndMetadataAsync();
        var apiVersions = await ApiVersionsV5Async(expected);

        Assert.Equal(expected, described.ClusterId);
        Assert.Equal(expected, metadata.ClusterId);
        Assert.Equal(ErrorCode.None, apiVersions.ErrorCode); // the id a client learned is the broker's own
    }

    /// <summary>
    /// A broker that has not joined its cluster yet has no id to report (#203) — no literal default stands in for one.
    /// Metadata says null, as Kafka's nullable field allows; a client pinned to an id is told to rebootstrap.
    /// </summary>
    [Fact]
    public async Task ABrokerWithoutAClusterIdReportsNoneAndAPinnedClientRebootstraps()
    {
        _config.UseClusterIdentity(() => null);

        var (described, metadata) = await DescribeAndMetadataAsync();
        var apiVersions = await ApiVersionsV5Async("AAAAAAAAQAGAAAAAAAAAAQ");

        Assert.Equal("", described.ClusterId);
        Assert.Null(metadata.ClusterId);
        Assert.Equal(ErrorCode.RebootstrapRequired, apiVersions.ErrorCode);
    }

    private async Task<(DescribeClusterResponse Described, MetadataResponse Metadata)> DescribeAndMetadataAsync()
    {
        var described = await DescribeClusterAsync();
        var metadata = Assert.IsType<MetadataResponse>(await _handler.HandleAsync(new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = 12,
            CorrelationId = 2,
            ClientId = "admin",
            Topics = [],
        }, Context(), CancellationToken.None));
        return (described, metadata);
    }

    private async Task<ApiVersionsResponse> ApiVersionsV5Async(string pinnedClusterId) =>
        Assert.IsType<ApiVersionsResponse>(await _handler.HandleAsync(new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = 5,
            CorrelationId = 3,
            ClientId = "admin",
            ClusterId = pinnedClusterId,
            NodeId = 1,
        }, Context(), CancellationToken.None));

    [Fact]
    public async Task FindCoordinatorNamesTheCoordinatorTheClusterAgreesOnOrSaysNoneIsAvailable()
    {
        // #204: the coordinator of a group is the leader of its __consumer_offsets partition, not the broker asked.
        var cluster = new ClusterState { LocalBrokerId = 1 };
        foreach (var id in new[] { 1, 2, 3 })
            cluster.AddBroker(new BrokerNode { BrokerId = id, Host = $"10.0.0.{id}", Port = 9092 });
        cluster.AddTopic(new Kuestenlogik.Surgewave.Core.Models.TopicMetadata
        {
            Name = Kuestenlogik.Surgewave.Coordination.Coordinators.CoordinatorPartitioner.GroupTopic,
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 3,
            Config = [],
            CreatedAt = DateTime.UtcNow,
        });
        var tp = new Kuestenlogik.Surgewave.Core.Models.TopicPartition
        {
            Topic = Kuestenlogik.Surgewave.Coordination.Coordinators.CoordinatorPartitioner.GroupTopic,
            Partition = 0,
        };
        cluster.AssignReplicas(tp, [1, 2, 3], 1);
        cluster.ElectLeader(tp, 3);
        _handler.SetCoordinatorLocator(new ClusterCoordinatorLocator(cluster));

        var found = await FindCoordinatorAsync(["orders-app"]);
        var coordinator = Assert.Single(found.Coordinators!);
        Assert.Equal(ErrorCode.None, coordinator.ErrorCode);
        Assert.Equal(3, coordinator.NodeId);
        Assert.Equal("10.0.0.3", coordinator.Host);

        cluster.UpdatePartitionState(tp, p => p.LeaderBrokerId = -1);
        var none = Assert.Single((await FindCoordinatorAsync(["orders-app"])).Coordinators!);
        Assert.Equal(ErrorCode.CoordinatorNotAvailable, none.ErrorCode);
    }

    private async Task<FindCoordinatorResponse> FindCoordinatorAsync(List<string> keys) =>
        Assert.IsType<FindCoordinatorResponse>(await _handler.HandleAsync(new FindCoordinatorRequest
        {
            ApiKey = ApiKey.FindCoordinator,
            ApiVersion = 4,
            CorrelationId = 9,
            ClientId = "admin",
            KeyType = 0,
            CoordinatorKeys = keys,
        }, Context(), CancellationToken.None));

    private async Task<DescribeClusterResponse> DescribeClusterAsync() =>
        Assert.IsType<DescribeClusterResponse>(await _handler.HandleAsync(new DescribeClusterRequest
        {
            ApiKey = ApiKey.DescribeCluster,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "admin",
        }, Context(), CancellationToken.None));

    private static RequestContext Context() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "admin",
    };
}
