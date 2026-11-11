using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
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
/// A CreateTopics request that names each partition's replicas gets that placement (#202), checked as Kafka's controller
/// checks it, with Kafka's error codes and messages. The assignment used to be counted for the partition limit and then
/// dropped: the topic got the broker's own placement, and the client no error.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CreateTopicsReplicaAssignmentTests : IDisposable
{
    private const int ThisBroker = 2;

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), "surgewave-replica-assignment-" + Guid.NewGuid().ToString("N"));
    private readonly BrokerConfig _config = new() { BrokerId = ThisBroker, DefaultNumPartitions = 3, DefaultReplicationFactor = 1 };
    private readonly QuotaManager _quotaManager;
    private readonly ClusterState _clusterState = new();
    private readonly PlacingController _controller;
    private readonly LogManager _clusterLogs;
    private readonly TopicAdminHandler _clusterHandler;
    private readonly LogManager _standaloneLogs;
    private readonly TopicAdminHandler _standaloneHandler;

    public CreateTopicsReplicaAssignmentTests()
    {
        Directory.CreateDirectory(_dataDir);
        _quotaManager = new QuotaManager(_config.Quotas, NullLogger<QuotaManager>.Instance);

        // A broker of a three-broker cluster whose controller is broker 1.
        foreach (var id in new[] { 1, 2, 3 })
            _clusterState.AddBroker(new BrokerNode { BrokerId = id, Host = "localhost", Port = 9090 + id });
        _clusterState.ControllerId = 1;
        _clusterLogs = new LogManager(Path.Combine(_dataDir, "cluster"), new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _controller = new PlacingController(_clusterState, _clusterLogs);
        var topics = new MetadataLogTopics(_controller, _controller, _clusterState, _clusterLogs, ThisBroker);
        _clusterLogs.RouteTopicCreation(topics);
        _clusterHandler = new TopicAdminHandler(_config, _clusterLogs, _quotaManager, auditLogger: null, NullLogger<TopicAdminHandler>.Instance);

        // A broker on its own: its only broker is itself.
        _standaloneLogs = new LogManager(Path.Combine(_dataDir, "standalone"), new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _standaloneHandler = new TopicAdminHandler(_config, _standaloneLogs, _quotaManager, auditLogger: null, NullLogger<TopicAdminHandler>.Instance);
    }

    public void Dispose()
    {
        _quotaManager.Dispose();
        _clusterLogs.Dispose();
        _standaloneLogs.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheTopicGetsTheReplicasTheClientChoseInItsOrder()
    {
        // kafka-topics --create --replica-assignment 3:1,1:2 — the first replica of each is the preferred leader.
        var result = await CreateAsync(_clusterHandler, Assigned("orders", [3, 1], [1, 2]));

        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal((2, (short)2), (result.NumPartitions, result.ReplicationFactor));
        Assert.Equal([3, 1], _clusterState.ReplicasOf(Partition("orders", 0)));
        Assert.Equal([1, 2], _clusterState.ReplicasOf(Partition("orders", 1)));
    }

    [Fact]
    public async Task ABrokerOnItsOwnCreatesATopicAssignedToItself()
    {
        var result = await CreateAsync(_standaloneHandler, Assigned("orders", [ThisBroker], [ThisBroker]));

        Assert.Equal(ErrorCode.None, result.ErrorCode);
        Assert.Equal(2, _standaloneLogs.GetTopicMetadata("orders")!.PartitionCount);
    }

    [Theory]
    [InlineData(-1, 2, "A manual partition assignment was specified, but replication factor was not set to -1.")]
    [InlineData(2, -1, "A manual partition assignment was specified, but numPartitions was not set to -1.")]
    [InlineData(1, 1, "A manual partition assignment was specified, but replication factor was not set to -1.")]
    public async Task AnAssignmentBesideAPartitionCountOrReplicationFactorIsAnInvalidRequest(
        int numPartitions, short replicationFactor, string message)
    {
        var topic = Assigned("orders", [1, 2]);
        var result = await CreateAsync(_clusterHandler, new CreateTopicsRequest.TopicToCreate
        {
            Name = topic.Name,
            NumPartitions = numPartitions,
            ReplicationFactor = replicationFactor,
            Assignments = topic.Assignments,
        });

        AssertRefused(result, ErrorCode.InvalidRequest, message);
    }

    [Fact]
    public async Task APartitionAssignedTwiceIsRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", (0, [1]), (0, [2])));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment, "Found multiple manual partition assignments for partition 0");
    }

    [Fact]
    public async Task APartitionWithoutReplicasIsRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", [1], []));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment, "The manual partition assignment includes an empty replica list.");
    }

    [Fact]
    public async Task ABrokerTwiceInOnePartitionIsRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", [3, 1, 3]));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment, "The manual partition assignment includes the broker 3 more than once.");
    }

    [Fact]
    public async Task PartitionsWithDifferentReplicationFactorsAreRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", [1, 2], [3]));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment,
            "The manual partition assignment includes a partition with 1 replica(s), but this is not consistent with previous " +
            "partitions, which have 2 replica(s).");
    }

    [Fact]
    public async Task PartitionsThatAreNotNumberedFromZeroOnAreRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", (0, [1]), (2, [2])));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment, "partitions should be a consecutive 0-based integer sequence");
    }

    [Fact]
    public async Task ABrokerTheClusterDoesNotHaveIsRefused()
    {
        var result = await CreateAsync(_clusterHandler, Assigned("orders", [1, 2], [7, 1]));

        AssertRefused(result, ErrorCode.InvalidReplicaAssignment, "The manual partition assignment includes broker 7, but no such broker is registered.");
    }

    [Fact]
    public async Task ABrokerOnItsOwnRefusesAnyOtherBroker()
    {
        var result = await CreateAsync(_standaloneHandler, Assigned("orders", [ThisBroker], [1]));

        Assert.Equal(ErrorCode.InvalidReplicaAssignment, result.ErrorCode);
        Assert.Equal("The manual partition assignment includes broker 1, but no such broker is registered.", result.ErrorMessage);
        Assert.Null(_standaloneLogs.GetTopicMetadata("orders"));
    }

    [Fact]
    public async Task ValidateOnlyChecksTheAssignmentAndCreatesNothing()
    {
        var refused = await CreateAsync(_clusterHandler, validateOnly: true, Assigned("bad", [1, 1]));
        var accepted = await CreateAsync(_clusterHandler, validateOnly: true, Assigned("good", [1, 2], [2, 3], [3, 1]));

        Assert.Equal(ErrorCode.InvalidReplicaAssignment, refused.ErrorCode);
        Assert.Equal(ErrorCode.None, accepted.ErrorCode);
        Assert.Equal((3, (short)2), (accepted.NumPartitions, accepted.ReplicationFactor));
        Assert.Empty(_controller.Asked);
    }

    private void AssertRefused(CreateTopicsResponse.TopicResult result, ErrorCode errorCode, string message)
    {
        Assert.Equal(errorCode, result.ErrorCode);
        Assert.Equal(message, result.ErrorMessage);
        Assert.Empty(_controller.Asked);
        Assert.Null(_clusterState.GetTopic("orders"));
    }

    private static TopicPartition Partition(string topic, int partition) => new() { Topic = topic, Partition = partition };

    // As an admin client sends it: NumPartitions and ReplicationFactor -1, partitions numbered in order.
    private static CreateTopicsRequest.TopicToCreate Assigned(string name, params List<int>[] replicas) =>
        Assigned(name, [.. replicas.Select((r, p) => (p, r))]);

    private static CreateTopicsRequest.TopicToCreate Assigned(string name, params (int Partition, List<int> Replicas)[] assignments) => new()
    {
        Name = name,
        NumPartitions = -1,
        ReplicationFactor = -1,
        Assignments = [.. assignments.Select(a => new CreateTopicsRequest.ReplicaAssignment { Partition = a.Partition, BrokerIds = a.Replicas })],
    };

    private static Task<CreateTopicsResponse.TopicResult> CreateAsync(TopicAdminHandler handler, CreateTopicsRequest.TopicToCreate topic) =>
        CreateAsync(handler, validateOnly: false, topic);

    private static async Task<CreateTopicsResponse.TopicResult> CreateAsync(
        TopicAdminHandler handler, bool validateOnly, CreateTopicsRequest.TopicToCreate topic)
    {
        var response = Assert.IsType<CreateTopicsResponse>(await handler.HandleAsync(new CreateTopicsRequest
        {
            ApiKey = ApiKey.CreateTopics,
            ApiVersion = 7,
            CorrelationId = 1,
            ClientId = "admin",
            TimeoutMs = 30_000,
            ValidateOnly = validateOnly,
            Topics = [topic],
        }, new RequestContext { ConnectionState = new ConnectionState("test-host"), ClientId = "admin" }, TestContext.Current.CancellationToken));
        return Assert.Single(response.Topics);
    }

    // The controller as the metadata log applies its decision: the topic with the replicas it was given, or its own
    // placement — every partition on broker 1 — without them; this broker hosts its replicas locally.
    private sealed class PlacingController(ClusterState clusterState, LogManager localLogs) : IClusterTopicCreator, IClusterTopicConfigWriter
    {
        public List<string> Asked { get; } = [];

        public bool IsController => false;

        public Task<bool> CreateTopicAsync(
            string topic, int partitionCount, short replicationFactor, Dictionary<string, string>? config,
            IReadOnlyList<IReadOnlyList<int>>? replicaAssignment, CancellationToken ct)
        {
            Asked.Add(topic);
            var topicId = Guid.NewGuid();
            clusterState.AddTopic(new TopicMetadata
            {
                Name = topic, TopicId = topicId, PartitionCount = partitionCount, ReplicationFactor = replicationFactor,
                Config = config ?? [], CreatedAt = DateTime.UtcNow,
            });
            var hostsAReplica = false;
            for (var p = 0; p < partitionCount; p++)
            {
                List<int> replicas = replicaAssignment is not null ? [.. replicaAssignment[p]] : [1];
                clusterState.AssignReplicas(Partition(topic, p), replicas);
                hostsAReplica |= replicas.Contains(ThisBroker);
            }
            if (hostsAReplica)
                localLogs.EnsureTopicMetadata(topic, topicId, partitionCount, replicationFactor);
            return Task.FromResult(true);
        }

        public Task<bool> ChangeTopicConfigAsync(
            string topic, IReadOnlyDictionary<string, string> set, IReadOnlyList<string> removed, CancellationToken ct) =>
            Task.FromResult(false);
    }
}
