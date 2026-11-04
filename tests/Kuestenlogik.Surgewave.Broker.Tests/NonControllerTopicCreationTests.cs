using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
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
/// A broker in a cluster that is not the controller never creates a topic for itself alone (#207): CreateTopics and a
/// Metadata request that auto-creates hand the creation to the cluster's topic creator — which asks the controller —
/// and the client is answered with a retriable error until the topic reaches this broker through the metadata log.
/// They used to create the topic on this broker only and answer success.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class NonControllerTopicCreationTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-noncontroller-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly QuotaManager _quotaManager;
    private readonly BrokerConfig _config = new() { BrokerId = 2, AutoCreateTopics = true, DefaultNumPartitions = 2 };
    private readonly RecordingCreator _controller = new();

    public NonControllerTopicCreationTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _quotaManager = new QuotaManager(_config.Quotas, NullLogger<QuotaManager>.Instance);
    }

    public void Dispose()
    {
        _quotaManager.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task CreateTopicsAsksTheControllerAndCreatesNothingHere()
    {
        var handler = new TopicAdminHandler(_config, _logManager, _quotaManager, auditLogger: null, NullLogger<TopicAdminHandler>.Instance);
        handler.SetClusterTopicCreator(_controller);

        var response = await CreateAsync(handler, "orders");

        Assert.Equal(ErrorCode.None, Assert.Single(response.Topics).ErrorCode);
        Assert.Equal("orders", Assert.Single(_controller.Asked));
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task ACreationTheControllerDidNotDoIsRetriable()
    {
        _controller.Succeeds = false;
        var handler = new TopicAdminHandler(_config, _logManager, _quotaManager, auditLogger: null, NullLogger<TopicAdminHandler>.Instance);
        handler.SetClusterTopicCreator(_controller);

        var response = await CreateAsync(handler, "orders");

        Assert.Equal(ErrorCode.NotController, Assert.Single(response.Topics).ErrorCode);
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task AutoCreationAsksTheControllerAndAnswersLeaderNotAvailableUntilTheTopicArrives()
    {
        var handler = new MetadataApiHandler(_config, _logManager, NullLogger<MetadataApiHandler>.Instance);
        handler.SetClusterState(new ClusterState());
        handler.SetClusterTopicCreator(_controller);

        var response = Assert.IsType<MetadataResponse>(await handler.HandleAsync(new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "producer",
            Topics = [new MetadataRequest.MetadataRequestTopic { Name = "orders" }],
            AllowAutoTopicCreation = true,
        }, Context(), CancellationToken.None));

        Assert.Equal(ErrorCode.LeaderNotAvailable, Assert.Single(response.Topics).ErrorCode);
        Assert.Equal("orders", Assert.Single(_controller.Asked));
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    private static async Task<CreateTopicsResponse> CreateAsync(TopicAdminHandler handler, string topic) =>
        Assert.IsType<CreateTopicsResponse>(await handler.HandleAsync(new CreateTopicsRequest
        {
            ApiKey = ApiKey.CreateTopics,
            ApiVersion = 7,
            CorrelationId = 1,
            ClientId = "admin",
            TimeoutMs = 30_000,
            Topics = [new CreateTopicsRequest.TopicToCreate { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }],
        }, Context(), CancellationToken.None));

    private static RequestContext Context() => new() { ConnectionState = new ConnectionState("test-host"), ClientId = "test" };

    // The cluster's topic creator on a broker that is not the controller: it asks the controller.
    private sealed class RecordingCreator : IClusterTopicCreator
    {
        public bool Succeeds { get; set; } = true;

        public List<string> Asked { get; } = [];

        public bool IsController => false;

        public Task<bool> CreateTopicAsync(string topic, int partitionCount, short replicationFactor, Dictionary<string, string>? config, CancellationToken ct)
        {
            Asked.Add(topic);
            return Task.FromResult(Succeeds);
        }
    }
}
