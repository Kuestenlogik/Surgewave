using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Once a broker is in a cluster, every topic it creates goes through the controller into the metadata log (#214),
/// whatever the entry point, and the creation completes when the topic has reached this broker.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataLogTopicsTests : IDisposable
{
    private const int BrokerId = 1;
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-topiccreator-" + Guid.NewGuid().ToString("N"));
    private readonly ClusterState _clusterState = new();
    private readonly LogManager _logManager;
    private readonly FakeController _controller = new();
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public MetadataLogTopicsTests()
    {
        _logManager = new LogManager(_dataDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        var topics = new MetadataLogTopics(_controller, _controller, _clusterState, _logManager, BrokerId);
        _logManager.RouteTopicCreation(topics);
        _logManager.RouteTopicConfigChanges(topics);
    }

    // A cluster with a controller that has this broker registered.
    private void JoinTheCluster()
    {
        _clusterState.AddBroker(new BrokerNode { BrokerId = BrokerId, Host = "localhost", Port = 9092 });
        _clusterState.ControllerId = 2;
    }

    [Fact]
    public async Task ACreationBeforeTheFirstElectionWaitsForTheControllerAndThisBrokersRegistration()
    {
        var creating = _logManager.CreateTopicAsync("orders", 1, 1, cancellationToken: _ct);

        // A broker that has just started (#235): no controller elected yet, then one that has not registered it.
        Assert.False(_controller.FirstAsk.Task.IsCompleted);
        _clusterState.ControllerId = 2;
        Assert.False(_controller.FirstAsk.Task.IsCompleted);
        _clusterState.AddBroker(new BrokerNode { BrokerId = BrokerId, Host = "localhost", Port = 9092 });

        await _controller.FirstAsk.Task.WaitAsync(TimeSpan.FromSeconds(10), _ct);
        Apply("orders", 1, replicas: [2]);
        Assert.Equal("orders", (await creating).Name);
    }


    public void Dispose()
    {
        JoinTheCluster();
        _logManager.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    [Fact]
    public async Task ATopicIsCreatedByTheControllerNotInTheLogManagerAlone()
    {
        JoinTheCluster();
        var creating = _logManager.CreateTopicAsync("orders", 1, 1, new() { ["retention.ms"] = "1000" }, _ct);

        var asked = Assert.Single(_controller.Asked);
        Assert.Equal(("orders", 1, (short)1, "1000"), (asked.Topic, asked.Partitions, asked.ReplicationFactor, asked.Config!["retention.ms"]));
        Assert.Null(_logManager.GetTopicMetadata("orders"));
        Assert.False(creating.IsCompleted);

        Apply("orders", 1, replicas: [BrokerId]);
        var topic = await creating;
        Assert.Equal("1000", topic.Config["retention.ms"]);
    }

    [Fact]
    public async Task ACreationCompletesOnceThisBrokersReplicasAreLocal()
    {
        JoinTheCluster();
        var creating = _logManager.CreateTopicAsync("orders", 2, 1, cancellationToken: _ct);

        // The metadata log has the topic, but this broker's replica is not in its log manager yet.
        _clusterState.AddTopic(Topic("orders", 2));
        _clusterState.AssignReplicas(Partition("orders", 0), [2]);
        _clusterState.AssignReplicas(Partition("orders", 1), [BrokerId]);
        Assert.False(creating.IsCompleted);

        _logManager.EnsureTopicMetadata("orders", Guid.NewGuid(), 2, 1);
        Assert.Equal(2, (await creating).PartitionCount);
    }

    [Fact]
    public async Task ATopicOnlyOtherBrokersHostIsThereOnceItsPartitionsAreAssigned()
    {
        JoinTheCluster();
        var creating = _logManager.CreateTopicAsync("orders", 1, 1, cancellationToken: _ct);

        _clusterState.AddTopic(Topic("orders", 1));
        Assert.False(creating.IsCompleted);
        _clusterState.AssignReplicas(Partition("orders", 0), [2]);

        Assert.Equal("orders", (await creating).Name);
        Assert.Null(_logManager.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task ATopicTheMetadataLogHasExistsAlready()
    {
        JoinTheCluster();
        Apply("orders", 1, replicas: [2]);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _logManager.CreateTopicAsync("orders", 1, cancellationToken: _ct));
        Assert.Contains("already exists", refused.Message, StringComparison.Ordinal);
        Assert.Empty(_controller.Asked);
    }

    [Fact]
    public async Task ATopicNoControllerTookIsRetriable()
    {
        JoinTheCluster();
        _controller.Takes = false;

        var refused = await Assert.ThrowsAsync<TopicNotCreatedException>(
            () => _logManager.CreateTopicAsync("orders", 1, cancellationToken: _ct));
        Assert.Equal("orders", refused.TopicName);
    }

    [Fact]
    public async Task AConfigChangeGoesToTheControllerAndCompletesOnceThisBrokerHasIt()
    {
        JoinTheCluster();
        Apply("orders", 1, replicas: [BrokerId]);

        var changing = _logManager.UpdateTopicConfigAsync(
            "orders", new() { ["retention.ms"] = "5000", ["segment.bytes"] = "" }, cancellationToken: _ct);

        // Not changed here alone (#119): the controller has it, and this broker's copy waits for the metadata log.
        var changed = Assert.Single(_controller.Changed);
        Assert.Equal("5000", changed.Set["retention.ms"]);
        Assert.Equal(["segment.bytes"], changed.Removed);
        Assert.False(_logManager.GetTopicMetadata("orders")!.Config.ContainsKey("retention.ms"));
        Assert.False(changing.IsCompleted);

        _clusterState.ChangeTopicConfig("orders", changed.Set, changed.Removed);
        Assert.False(changing.IsCompleted);
        _logManager.ApplyTopicConfig("orders", _clusterState.GetTopic("orders")!.Config);

        Assert.True(await changing);
        Assert.Equal("5000", _logManager.GetTopicMetadata("orders")!.Config["retention.ms"]);
    }

    [Fact]
    public async Task AConfigChangeOfATopicTheMetadataLogDoesNotHaveIsRefused()
    {
        JoinTheCluster();
        Assert.False(await _logManager.UpdateTopicConfigAsync("unknown", new() { ["retention.ms"] = "5000" }, cancellationToken: _ct));
        Assert.Empty(_controller.Changed);
    }

    [Fact]
    public async Task AConfigChangeNoControllerTookIsRetriable()
    {
        JoinTheCluster();
        Apply("orders", 1, replicas: [BrokerId]);
        _controller.Takes = false;

        var refused = await Assert.ThrowsAsync<TopicConfigNotChangedException>(
            () => _logManager.UpdateTopicConfigAsync("orders", new() { ["retention.ms"] = "5000" }, cancellationToken: _ct));
        Assert.Equal("orders", refused.TopicName);
    }

    [Fact]
    public void TheMetadataLogsConfigReplacesWhatThisBrokerHad()
    {
        JoinTheCluster();
        Apply("orders", 1, replicas: [BrokerId]);
        _clusterState.ChangeTopicConfig("orders", new Dictionary<string, string> { ["retention.ms"] = "5000" }, []);

        Assert.True(_logManager.ApplyTopicConfig("orders", _clusterState.GetTopic("orders")!.Config));
        _clusterState.ChangeTopicConfig("orders", new Dictionary<string, string>(), ["retention.ms"]);
        Assert.True(_logManager.ApplyTopicConfig("orders", _clusterState.GetTopic("orders")!.Config));

        Assert.False(_logManager.GetTopicMetadata("orders")!.Config.ContainsKey("retention.ms"));
    }

    // The metadata log's TopicCreated, as this broker applies it.
    private void Apply(string topic, int partitions, List<int> replicas)
    {
        _clusterState.AddTopic(Topic(topic, partitions));
        for (var p = 0; p < partitions; p++)
            _clusterState.AssignReplicas(Partition(topic, p), replicas);
        if (replicas.Contains(BrokerId))
            _logManager.EnsureTopicMetadata(topic, _clusterState.GetTopic(topic)!.TopicId, partitions, 1, _clusterState.GetTopic(topic)!.Config);
    }

    private TopicMetadata Topic(string name, int partitions) => new()
    {
        Name = name,
        TopicId = Guid.NewGuid(),
        PartitionCount = partitions,
        ReplicationFactor = 1,
        Config = _controller.Asked.LastOrDefault(a => a.Topic == name).Config ?? [],
        CreatedAt = DateTime.UtcNow,
    };

    private static TopicPartition Partition(string topic, int partition) => new() { Topic = topic, Partition = partition };

    private sealed class FakeController : IClusterTopicCreator, IClusterTopicConfigWriter
    {
        public List<(string Topic, IReadOnlyDictionary<string, string> Set, IReadOnlyList<string> Removed)> Changed { get; } = [];

        public Task<bool> ChangeTopicConfigAsync(
            string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct)
        {
            Changed.Add((topic, set, removed));
            return Task.FromResult(Takes);
        }

        public List<(string Topic, int Partitions, short ReplicationFactor, Dictionary<string, string>? Config)> Asked { get; } = [];

        public bool Takes { get; set; } = true;

        /// <summary>Completes at the first creation the controller is asked for.</summary>
        public TaskCompletionSource FirstAsk { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool IsController => true;

        public Task<bool> CreateTopicAsync(
            string topic, int partitionCount, short replicationFactor, Dictionary<string, string>? config, CancellationToken ct)
        {
            Asked.Add((topic, partitionCount, replicationFactor, config));
            FirstAsk.TrySetResult();
            return Task.FromResult(Takes);
        }
    }
}
