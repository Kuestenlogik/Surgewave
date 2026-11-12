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
/// Metadata request that auto-creates go through the log manager, which hands the creation to the controller, and
/// complete once the topic has reached this broker through the metadata log (#214); a controller that does not take it
/// is a retriable error. They used to create the topic on this broker only and answer success.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class NonControllerTopicCreationTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-noncontroller-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logManager;
    private readonly QuotaManager _quotaManager;
    private readonly BrokerConfig _config = new() { BrokerId = 2, AutoCreateTopics = true, DefaultNumPartitions = 2 };
    private readonly ClusterState _clusterState = new();
    private readonly RecordingCreator _controller;

    public NonControllerTopicCreationTests()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _quotaManager = new QuotaManager(_config.Quotas, NullLogger<QuotaManager>.Instance);
        _controller = new RecordingCreator(_clusterState);
        // A cluster whose controller is broker 1 and has this broker registered.
        _clusterState.AddBroker(new Kuestenlogik.Surgewave.Clustering.Cluster.BrokerNode { BrokerId = _config.BrokerId, Host = "localhost", Port = 9092 });
        _clusterState.ControllerId = 1;
        var topics = new MetadataLogTopics(_controller, _controller, _clusterState, _logManager, _config.BrokerId);
        _logManager.RouteTopicCreation(topics);
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

        var response = await CreateAsync(handler, "orders");

        Assert.Equal(ErrorCode.NotController, Assert.Single(response.Topics).ErrorCode);
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task AutoCreationAsksTheControllerAndAnswersWithTheTopicOnceItArrived()
    {
        var response = await AutoCreateAsync("orders");

        // The topic as the cluster has it; its replicas are on another broker, so nothing was created here.
        var topic = Assert.Single(response.Topics);
        Assert.Equal(ErrorCode.None, topic.ErrorCode);
        Assert.Equal(2, topic.Partitions.Count);
        Assert.Equal("orders", Assert.Single(_controller.Asked));
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    /// <summary>
    /// A topic the cluster has and this broker hosts no replica of is not a missing one (#281, the Kafka wire's side
    /// of what #268 found in the native protocol): Metadata answers it from the cluster's state and asks nobody to
    /// create it, which the metadata log would refuse.
    /// </summary>
    [Fact]
    public async Task ATopicOfTheClusterThisBrokerHostsNoReplicaOfIsAnsweredAndNotCreatedAgain()
    {
        await _controller.CreateTopicAsync("orders", 2, 1, config: null, replicaAssignment: null, CancellationToken.None);
        _controller.Asked.Clear();
        Assert.Null(_logManager.GetTopicMetadata("orders"));

        var response = await AutoCreateAsync("orders");

        var topic = Assert.Single(response.Topics);
        Assert.Equal(ErrorCode.None, topic.ErrorCode);
        Assert.Equal(2, topic.Partitions.Count);
        Assert.Empty(_controller.Asked);
    }

    [Fact]
    public async Task AnAutoCreationTheControllerDidNotDoAnswersLeaderNotAvailable()
    {
        _controller.Succeeds = false;

        var response = await AutoCreateAsync("orders");

        Assert.Equal(ErrorCode.LeaderNotAvailable, Assert.Single(response.Topics).ErrorCode);
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task MetadataNamesThisBrokerAsControllerWhileNoneIsElected()
    {
        // A broker that has just started, before its first election (#235): told -1, an admin client waited for a
        // controller until its CreateTopics timed out. This broker takes the request and hands it on once there is one;
        // a topic to auto-create is not created before then, and the client is told to ask again.
        _clusterState.ControllerId = -1;

        var response = await AutoCreateAsync("orders");

        Assert.Equal(_config.BrokerId, response.ControllerId);
        Assert.Equal(ErrorCode.LeaderNotAvailable, Assert.Single(response.Topics).ErrorCode);
        Assert.Empty(_controller.Asked);
    }

    private async Task<MetadataResponse> AutoCreateAsync(string topic)
    {
        var handler = new MetadataApiHandler(_config, _logManager, NullLogger<MetadataApiHandler>.Instance);
        handler.SetClusterState(_clusterState);
        return Assert.IsType<MetadataResponse>(await handler.HandleAsync(new MetadataRequest
        {
            ApiKey = ApiKey.Metadata,
            ApiVersion = 9,
            CorrelationId = 1,
            ClientId = "producer",
            Topics = [new MetadataRequest.MetadataRequestTopic { Name = topic }],
            AllowAutoTopicCreation = true,
        }, Context(), CancellationToken.None));
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

    // The cluster's topic creator on a broker that is not the controller: it asks the controller, whose metadata log
    // then has the topic, its replicas on broker 1 and not this one.
    private sealed class RecordingCreator(ClusterState clusterState) : IClusterTopicCreator, IClusterTopicConfigWriter
    {
        public bool Succeeds { get; set; } = true;

        public List<string> Asked { get; } = [];

        public bool IsController => false;

        public Task<bool> CreateTopicAsync(
            string topic, int partitionCount, short replicationFactor, Dictionary<string, string>? config,
            IReadOnlyList<IReadOnlyList<int>>? replicaAssignment, CancellationToken ct)
        {
            Asked.Add(topic);
            if (Succeeds)
            {
                clusterState.AddTopic(new Kuestenlogik.Surgewave.Core.Models.TopicMetadata
                {
                    Name = topic, TopicId = Guid.NewGuid(), PartitionCount = partitionCount, ReplicationFactor = replicationFactor,
                    Config = config ?? [], CreatedAt = DateTime.UtcNow,
                });
                for (var p = 0; p < partitionCount; p++)
                    clusterState.AssignReplicas(new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = topic, Partition = p }, [1]);
            }
            return Task.FromResult(Succeeds);
        }

        public Task<bool> ChangeTopicConfigAsync(
            string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct) =>
            Task.FromResult(false);
    }
}
