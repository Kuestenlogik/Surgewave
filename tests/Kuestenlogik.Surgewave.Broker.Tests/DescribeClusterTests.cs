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
/// id (#131). It reported only the broker asked, and its cluster id fell back to "surgewave-{brokerId}" where
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
    [InlineData(null, null, BrokerConfig.DefaultClusterId)]
    [InlineData("configured-id", null, "configured-id")]
    [InlineData(null, "generated-id", "generated-id")]
    public async Task DescribeClusterMetadataAndApiVersionsReportTheSameClusterId(string? configured, string? resolved, string expected)
    {
        _config.ClusterId = configured;
        if (resolved is not null)
            _config.UseResolvedClusterId(resolved);

        var described = await DescribeClusterAsync();
        var metadata = Assert.IsType<MetadataResponse>(await _handler.HandleAsync(new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = 12,
            CorrelationId = 2,
            ClientId = "admin",
            Topics = [],
        }, Context(), CancellationToken.None));
        var apiVersions = Assert.IsType<ApiVersionsResponse>(await _handler.HandleAsync(new ApiVersionsRequest
        {
            ApiKey = ApiKey.ApiVersions,
            ApiVersion = 5,
            CorrelationId = 3,
            ClientId = "admin",
            ClusterId = expected,
            NodeId = 1,
        }, Context(), CancellationToken.None));

        Assert.Equal(expected, described.ClusterId);
        Assert.Equal(expected, metadata.ClusterId);
        Assert.Equal(ErrorCode.None, apiVersions.ErrorCode); // the id a client learned is the broker's own
    }

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
