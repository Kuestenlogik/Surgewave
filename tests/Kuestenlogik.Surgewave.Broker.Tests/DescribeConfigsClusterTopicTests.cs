using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
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
/// DescribeConfigs for a topic the cluster has and this broker hosts no replica of (#240): a Kafka admin client asks
/// any broker, and <c>kafka-topics --describe</c> failed with "Topic does not exist" when it asked one outside the
/// topic's replica assignment.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeConfigsClusterTopicTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-describe-configs-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly ClusterState _clusterState = new();
    private readonly ConfigApiHandler _handler;

    public DescribeConfigsClusterTopicTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var brokerConfig = new BrokerConfig { DataDirectory = _dataDir };
        _handler = new ConfigApiHandler(
            brokerConfig, new DynamicBrokerConfig(brokerConfig, NullLogger<DynamicBrokerConfig>.Instance), _logManager);
        _handler.SetClusterState(_clusterState);
    }

    public void Dispose()
    {
        _logManager.Dispose();
        Directory.Delete(_dataDir, recursive: true);
    }

    [Fact]
    public async Task ATopicOfTheClusterIsDescribedByABrokerThatHostsNoReplicaOfIt()
    {
        _clusterState.AddTopic(new TopicMetadata
        {
            Name = "orders",
            TopicId = Guid.NewGuid(),
            PartitionCount = 2,
            ReplicationFactor = 2,
            Config = new Dictionary<string, string> { ["retention.ms"] = "60000" },
            CreatedAt = DateTime.UtcNow,
        });
        Assert.Null(_logManager.GetTopicMetadata("orders"));

        var result = Assert.Single((await DescribeAsync("orders")).Results);

        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal("60000", Assert.Single(result.Configs, c => c.Name == "retention.ms").Value);
    }

    [Fact]
    public async Task ATopicTheClusterDoesNotHaveIsUnknown()
    {
        var result = Assert.Single((await DescribeAsync("missing")).Results);

        Assert.Equal(ErrorCode.UnknownTopicOrPartition, result.ErrorCode);
    }

    private async Task<DescribeConfigsResponse> DescribeAsync(string topic)
    {
        var request = new DescribeConfigsRequest
        {
            ApiKey = ApiKey.DescribeConfigs,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "adminclient-1",
            Resources = [new DescribeConfigsRequest.ConfigResource { ResourceType = ConfigResourceType.Topic, ResourceName = topic }],
        };
        var context = new RequestContext { ConnectionState = new ConnectionState("127.0.0.1"), ClientId = request.ClientId };
        return (DescribeConfigsResponse)await _handler.HandleAsync(request, context, TestContext.Current.CancellationToken);
    }
}
