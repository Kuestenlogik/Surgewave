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
/// AlterConfigs and IncrementalAlterConfigs for a topic the cluster has and this broker hosts no replica of: a Kafka
/// admin client asks any broker, and the change failed with "Topic does not exist" when it asked one outside the
/// topic's replica assignment — in a cluster of three with one replica, two times out of three.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AlterConfigsClusterTopicTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-alter-configs-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly ClusterState _clusterState = new();
    private readonly ChangesTaken _controller = new();
    private readonly ConfigApiHandler _handler;

    public AlterConfigsClusterTopicTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _logManager.RouteTopicConfigChanges(_controller);
        var brokerConfig = new BrokerConfig { DataDirectory = _dataDir };
        _handler = new ConfigApiHandler(
            brokerConfig, new DynamicBrokerConfig(brokerConfig, NullLogger<DynamicBrokerConfig>.Instance), _logManager);
        _handler.SetClusterState(_clusterState);

        _clusterState.AddTopic(new TopicMetadata
        {
            Name = "orders",
            TopicId = Guid.NewGuid(),
            PartitionCount = 1,
            ReplicationFactor = 1,
            Config = new Dictionary<string, string> { ["retention.ms"] = "60000" },
            CreatedAt = DateTime.UtcNow,
        });
    }

    public void Dispose()
    {
        _logManager.Dispose();
        Directory.Delete(_dataDir, recursive: true);
    }

    [Fact]
    public async Task AnIncrementalChangeIsTakenByABrokerThatHostsNoReplicaOfTheTopic()
    {
        Assert.Null(_logManager.GetTopicMetadata("orders"));

        var result = Assert.Single((await IncrementalAlterAsync("orders")).Responses);

        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal("987654321", Assert.Single(_controller.Taken)["retention.ms"]);
    }

    [Fact]
    public async Task AChangeIsTakenByABrokerThatHostsNoReplicaOfTheTopic()
    {
        Assert.Null(_logManager.GetTopicMetadata("orders"));

        var result = Assert.Single((await AlterAsync("orders")).Results);

        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal("987654321", Assert.Single(_controller.Taken)["retention.ms"]);
    }

    [Fact]
    public async Task ATopicTheClusterDoesNotHaveIsUnknown()
    {
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, Assert.Single((await IncrementalAlterAsync("missing")).Responses).ErrorCode);
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, Assert.Single((await AlterAsync("missing")).Results).ErrorCode);
        Assert.Empty(_controller.Taken);
    }

    private async Task<IncrementalAlterConfigsResponse> IncrementalAlterAsync(string topic)
    {
        var request = new IncrementalAlterConfigsRequest
        {
            ApiKey = ApiKey.IncrementalAlterConfigs,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "adminclient-1",
            Resources =
            [
                new IncrementalAlterConfigsRequest.AlterConfigsResource
                {
                    ResourceType = (sbyte)ConfigResourceType.Topic,
                    ResourceName = topic,
                    Configs = [new IncrementalAlterConfigsRequest.AlterableConfig { Name = "retention.ms", ConfigOperation = 0, Value = "987654321" }],
                },
            ],
        };
        return (IncrementalAlterConfigsResponse)await _handler.HandleAsync(request, Context(request), TestContext.Current.CancellationToken);
    }

    private async Task<AlterConfigsResponse> AlterAsync(string topic)
    {
        var request = new AlterConfigsRequest
        {
            ApiKey = ApiKey.AlterConfigs,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "adminclient-1",
            Resources =
            [
                new AlterConfigsRequest.AlterConfigResource
                {
                    ResourceType = ConfigResourceType.Topic,
                    ResourceName = topic,
                    Configs = [new AlterConfigsRequest.AlterConfigEntry { Name = "retention.ms", Value = "987654321" }],
                },
            ],
        };
        return (AlterConfigsResponse)await _handler.HandleAsync(request, Context(request), TestContext.Current.CancellationToken);
    }

    private static RequestContext Context(KafkaRequest request) =>
        new() { ConnectionState = new ConnectionState("127.0.0.1"), ClientId = request.ClientId };

    // The way into the metadata log, as the log manager of a broker in a cluster has one: it takes every change.
    private sealed class ChangesTaken : ITopicConfigWriter
    {
        public List<IReadOnlyDictionary<string, string>> Taken { get; } = [];

        public Task<bool> ChangeTopicConfigAsync(
            string topicName, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken cancellationToken)
        {
            Taken.Add(set);
            return Task.FromResult(true);
        }
    }
}
