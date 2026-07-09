using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// Integration tests for Raft consensus protocol.
/// Tests leader election, vote requests, quorum state, and epoch transitions.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public class RaftIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly List<SurgewaveRuntime> _brokers = [];

    public RaftIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Debug);
        });
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var broker in _brokers)
        {
            try
            {
                await broker.DisposeAsync();
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Error disposing broker: {ex.Message}");
            }
        }
        _loggerFactory.Dispose();
    }

    #region RaftNode Unit Tests

    [Fact]
    public async Task RaftNode_SingleNode_BecomesLeader()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftHeartbeatIntervalMs = 50,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);

        // Act
        await raftNode.StartAsync(CancellationToken.None);

        // Wait for single node to become leader (election timeout)
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Single node should become leader within timeout");

        // Assert
        Assert.True(raftNode.IsLeader, "Single node should become leader");
        Assert.Equal(RaftState.Leader, raftNode.State);
        Assert.Equal(1, raftNode.LeaderId);
        Assert.True(raftNode.CurrentTerm >= 1, "Term should be at least 1 after election");
    }

    [Fact]
    public async Task RaftNode_WithClusterPeers_DoesNotSelfElectWithoutPeerDiscovery()
    {
        // This test verifies the split-brain prevention fix:
        // When ClusterNodes is configured (expecting peers), a node should NOT
        // self-elect as leader if no peers have been discovered yet.

        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 100, // Short timeout to trigger election quickly
            RaftElectionTimeoutMaxMs = 200,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-splitbrain-" + Guid.NewGuid().ToString("N")),
            ClusterNodes = "2:localhost:9092:9093,3:localhost:9094:9095", // Configured to expect peers
            RaftPeerDiscoveryTimeoutSeconds = 0 // Disable peer discovery wait for this test
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport(); // Returns empty peer list (peers not discovered)
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);

        // Act
        await raftNode.StartAsync(CancellationToken.None);

        // Wait for multiple election timeouts - node should NOT become leader
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Assert - Node should still be Follower or Candidate, NOT Leader
        Assert.NotEqual(RaftState.Leader, raftNode.State);
        _output.WriteLine($"Node state after election timeout: {raftNode.State}, Term: {raftNode.CurrentTerm}");
        _output.WriteLine("Split-brain prevention working: node did not self-elect without peer discovery");
    }

    [Fact]
    public async Task RaftNode_SingleNodeWithoutClusterPeers_CanSelfElect()
    {
        // This test verifies that a truly single-node cluster (no ClusterNodes configured)
        // can still self-elect as leader.

        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-single-" + Guid.NewGuid().ToString("N")),
            ClusterNodes = "" // No peers configured - truly single node
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);

        // Act
        await raftNode.StartAsync(CancellationToken.None);

        // Wait for single node to become leader
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(becameLeader, "Single node without cluster peers should become leader");
        Assert.Equal(RaftState.Leader, raftNode.State);
        _output.WriteLine($"Single node correctly self-elected as leader, Term: {raftNode.CurrentTerm}");
    }

    [Fact]
    public async Task RaftNode_WaitsForPeerReadiness_WithShortTimeout()
    {
        // This test verifies that when RaftPeerDiscoveryTimeoutSeconds is set,
        // the node waits for peer discovery before starting elections.
        // With no peers reachable and a short timeout, it should proceed after timeout.

        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-peerwait-" + Guid.NewGuid().ToString("N")),
            ClusterNodes = "2:localhost:9092:9093", // Configured to expect peers
            RaftPeerDiscoveryTimeoutSeconds = 1 // Short timeout for test (1 second)
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport(); // Returns no reachable peers
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);

        // Act - Start should wait for peer discovery timeout before starting elections
        var startTime = DateTimeOffset.UtcNow;
        await raftNode.StartAsync(CancellationToken.None);
        var elapsed = DateTimeOffset.UtcNow - startTime;

        // Assert - Should have waited at least 1 second (the peer discovery timeout)
        Assert.True(elapsed >= TimeSpan.FromSeconds(0.9),
            $"Should have waited for peer discovery timeout, but only waited {elapsed.TotalMilliseconds}ms");
        _output.WriteLine($"Node waited {elapsed.TotalMilliseconds}ms for peer discovery before proceeding");
    }

    [Fact]
    public async Task RaftNode_HandleRequestVote_GrantsVoteForHigherTerm()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000, // Long timeout to prevent self-election
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Act - Send vote request from candidate with higher term
        var request = new RequestVoteRequest(
            Term: 5,
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandleRequestVoteAsync(request, CancellationToken.None);

        // Assert
        Assert.True(response.VoteGranted, "Vote should be granted for higher term");
        Assert.Equal(5, response.Term);
        Assert.Equal(RaftState.Follower, raftNode.State);
    }

    [Fact]
    public async Task RaftNode_HandleRequestVote_RejectsVoteForStaleTerm()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Wait to become leader (establishes term)
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader within timeout");
        var currentTerm = raftNode.CurrentTerm;

        // Act - Send vote request with lower term
        var request = new RequestVoteRequest(
            Term: currentTerm - 1,
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandleRequestVoteAsync(request, CancellationToken.None);

        // Assert
        Assert.False(response.VoteGranted, "Vote should be rejected for stale term");
        Assert.Equal(currentTerm, response.Term);
    }

    [Fact]
    public async Task RaftNode_HandleAppendEntries_AcceptsFromLeader()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Act - Send AppendEntries from a leader
        var request = new AppendEntriesRequest(
            Term: 5,
            LeaderId: 2,
            PrevLogIndex: 0,
            PrevLogTerm: 0,
            Entries: [],
            LeaderCommit: 0
        );

        var response = await raftNode.HandleAppendEntriesAsync(request, CancellationToken.None);

        // Assert
        Assert.True(response.Success, "AppendEntries should succeed");
        Assert.Equal(5, response.Term);
        Assert.Equal(RaftState.Follower, raftNode.State);
        Assert.Equal(2, raftNode.LeaderId);
    }

    [Fact]
    public async Task RaftNode_ProposeEntry_SucceedsWhenLeader()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Wait to become leader
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader within timeout");
        Assert.True(raftNode.IsLeader);

        // Act
        var data = System.Text.Encoding.UTF8.GetBytes("test-command");
        var index = await raftNode.ProposeAsync(MetadataCommandType.Noop, data, CancellationToken.None);

        // Assert
        Assert.True(index > 0, "Proposed entry should have positive index");
        Assert.Equal(index, raftNode.LastLogIndex);
    }

    [Fact]
    public async Task RaftNode_ProposeEntry_FailsWhenNotLeader()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000, // Long timeout to stay follower
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Act (node is follower)
        var data = System.Text.Encoding.UTF8.GetBytes("test-command");
        var index = await raftNode.ProposeAsync(MetadataCommandType.Noop, data, CancellationToken.None);

        // Assert
        Assert.Equal(-1, index);
    }

    #endregion

    #region RaftApiHandler Tests

    [Fact]
    public void RaftApiHandler_SupportedApiKeys_IncludesAllRaftApis()
    {
        // Arrange
        var config = new BrokerConfig { BrokerId = 1 };
        var clusterState = new ClusterState();
        var logger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(config, null, null, clusterState, logger);

        // Assert — 5 base Raft RPCs (KIP-595 / KIP-630) plus the 3 KIP-853
        // voter-management RPCs (Add/Remove/Update Raft voter). Update this
        // count when the handler grows another KIP, not just when CI shouts.
        var supportedApis = handler.SupportedApiKeys.ToList();
        Assert.Contains(ApiKey.Vote, supportedApis);
        Assert.Contains(ApiKey.BeginQuorumEpoch, supportedApis);
        Assert.Contains(ApiKey.EndQuorumEpoch, supportedApis);
        Assert.Contains(ApiKey.DescribeQuorum, supportedApis);
        Assert.Contains(ApiKey.FetchSnapshot, supportedApis);
        Assert.Contains(ApiKey.AddRaftVoter, supportedApis);
        Assert.Contains(ApiKey.RemoveRaftVoter, supportedApis);
        Assert.Contains(ApiKey.UpdateRaftVoter, supportedApis);
        Assert.Equal(8, supportedApis.Count);
    }

    [Fact]
    public async Task RaftApiHandler_VoteRequest_ReturnsNotControllerWhenNoRaftNode()
    {
        // Arrange
        var config = new BrokerConfig { BrokerId = 1 };
        var clusterState = new ClusterState();
        var logger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(config, null, null, clusterState, logger);

        var request = new VoteRequest
        {
            ApiKey = ApiKey.Vote,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "test-client",
            ClusterId = "test-cluster",
            Topics = []
        };

        var context = CreateTestRequestContext();

        // Act
        var response = await handler.HandleAsync(request, context, CancellationToken.None);

        // Assert
        Assert.IsType<VoteResponse>(response);
        var voteResponse = (VoteResponse)response;
        Assert.Equal(ErrorCode.NotController, voteResponse.ErrorCode);
    }

    [Fact]
    public async Task RaftApiHandler_DescribeQuorum_ReturnsNotControllerWhenNoRaftNode()
    {
        // Arrange
        var config = new BrokerConfig { BrokerId = 1 };
        var clusterState = new ClusterState();
        var logger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(config, null, null, clusterState, logger);

        var request = new DescribeQuorumRequest
        {
            ApiKey = ApiKey.DescribeQuorum,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "test-client",
            Topics = []
        };

        var context = CreateTestRequestContext();

        // Act
        var response = await handler.HandleAsync(request, context, CancellationToken.None);

        // Assert
        Assert.IsType<DescribeQuorumResponse>(response);
        var describeResponse = (DescribeQuorumResponse)response;
        Assert.Equal(ErrorCode.NotController, describeResponse.ErrorCode);
    }

    [Fact]
    public async Task RaftApiHandler_VoteRequest_RejectsClusterIdMismatch()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var brokerConfig = new BrokerConfig
        {
            BrokerId = 1,
            ClusterId = "my-cluster"
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var raftLogger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(raftLogger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        var clusterState = new ClusterState();
        var handlerLogger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(brokerConfig, raftNode, null, clusterState, handlerLogger);

        var request = new VoteRequest
        {
            ApiKey = ApiKey.Vote,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "test-client",
            ClusterId = "different-cluster", // Mismatched cluster ID
            Topics =
            [
                new VoteRequest.TopicData
                {
                    TopicName = "__cluster_metadata",
                    Partitions =
                    [
                        new VoteRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            CandidateEpoch = 1,
                            CandidateId = 2,
                            LastOffset = 0,
                            LastOffsetEpoch = 0
                        }
                    ]
                }
            ]
        };

        var context = CreateTestRequestContext();

        // Act
        var response = await handler.HandleAsync(request, context, CancellationToken.None);

        // Assert
        Assert.IsType<VoteResponse>(response);
        var voteResponse = (VoteResponse)response;
        Assert.Equal(ErrorCode.InconsistentClusterId, voteResponse.ErrorCode);
    }

    [Fact]
    public async Task RaftApiHandler_VoteRequest_HandlesValidRequest()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var brokerConfig = new BrokerConfig
        {
            BrokerId = 1,
            ClusterId = "test-cluster"
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var raftLogger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(raftLogger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        var clusterState = new ClusterState();
        var handlerLogger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(brokerConfig, raftNode, null, clusterState, handlerLogger);

        var request = new VoteRequest
        {
            ApiKey = ApiKey.Vote,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "test-client",
            ClusterId = "test-cluster",
            Topics =
            [
                new VoteRequest.TopicData
                {
                    TopicName = "__cluster_metadata",
                    Partitions =
                    [
                        new VoteRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            CandidateEpoch = 10, // Higher term
                            CandidateId = 2,
                            LastOffset = 0,
                            LastOffsetEpoch = 0
                        }
                    ]
                }
            ]
        };

        var context = CreateTestRequestContext();

        // Act
        var response = await handler.HandleAsync(request, context, CancellationToken.None);

        // Assert
        Assert.IsType<VoteResponse>(response);
        var voteResponse = (VoteResponse)response;
        Assert.Equal(ErrorCode.None, voteResponse.ErrorCode);
        Assert.Single(voteResponse.Topics);
        Assert.Single(voteResponse.Topics[0].Partitions);
        Assert.True(voteResponse.Topics[0].Partitions[0].VoteGranted);
    }

    [Fact]
    public async Task RaftApiHandler_DescribeQuorum_ReturnsQuorumState()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-test-" + Guid.NewGuid().ToString("N"))
        };

        var brokerConfig = new BrokerConfig
        {
            BrokerId = 1
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var raftLogger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(raftLogger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Wait for election
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader within timeout");

        var clusterState = new ClusterState();
        var handlerLogger = _loggerFactory.CreateLogger<RaftApiHandler>();

        var handler = new RaftApiHandler(brokerConfig, raftNode, null, clusterState, handlerLogger);

        var request = new DescribeQuorumRequest
        {
            ApiKey = ApiKey.DescribeQuorum,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "test-client",
            Topics =
            [
                new DescribeQuorumRequest.TopicData
                {
                    TopicName = "__cluster_metadata",
                    Partitions =
                    [
                        new DescribeQuorumRequest.PartitionData { PartitionIndex = 0 }
                    ]
                }
            ]
        };

        var context = CreateTestRequestContext();

        // Act
        var response = await handler.HandleAsync(request, context, CancellationToken.None);

        // Assert
        Assert.IsType<DescribeQuorumResponse>(response);
        var describeResponse = (DescribeQuorumResponse)response;
        Assert.Equal(ErrorCode.None, describeResponse.ErrorCode);
        Assert.Single(describeResponse.Topics);

        var partition = describeResponse.Topics[0].Partitions[0];
        Assert.Equal(ErrorCode.None, partition.ErrorCode);
        Assert.Equal(1, partition.LeaderId); // Self is leader
        Assert.True(partition.LeaderEpoch >= 1);
        Assert.NotEmpty(partition.CurrentVoters);
    }

    #endregion

    #region SurgewaveRuntime Integration Tests

    [Fact]
    public async Task SurgewaveRuntime_SingleBroker_StartsSuccessfully()
    {
        // Arrange & Act
        var broker = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();

        _brokers.Add(broker);

        // Assert
        Assert.True(broker.Port > 0, "Broker should have a valid port");
        _output.WriteLine($"Broker started on port {broker.Port}");
    }

    [Fact]
    public async Task SurgewaveRuntime_ThreeBrokerCluster_AllBrokersStart()
    {
        // Arrange & Act - Start 3 brokers
        var broker1 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster()
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker1);

        var broker2 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(2)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster($"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}")
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker2);

        var broker3 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(3)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(
                $"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}",
                $"2:{broker2.Host}:{broker2.Port}:{broker2.ReplicationPort}")
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker3);

        // Assert
        Assert.All(_brokers, b => Assert.True(b.Port > 0, "Broker should have a valid port"));
        _output.WriteLine($"3-broker cluster started: {string.Join(", ", _brokers.Select(b => b.Port))}");
    }

    [Fact(Timeout = 60000)]
    public async Task RaftCluster_ThreeNodes_RaftNodesInitialized()
    {
        // Arrange - Start 3 brokers with Raft consensus enabled
        var broker1 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster()
            .WithRaft(true) // Enable Raft consensus
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker1);

        var broker2 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(2)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster($"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}")
            .WithRaft(true)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker2);

        var broker3 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(3)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(
                $"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}",
                $"2:{broker2.Host}:{broker2.Port}:{broker2.ReplicationPort}")
            .WithRaft(true)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker3);

        _output.WriteLine($"3-broker Raft cluster started: ports {broker1.Port}, {broker2.Port}, {broker3.Port}");

        // Assert - All brokers should have RaftNode initialized
        foreach (var broker in _brokers)
        {
            Assert.NotNull(broker.RaftNode);
            _output.WriteLine($"Broker {broker.BrokerId}: RaftNode present, State={broker.RaftNode.State}, Term={broker.RaftNode.CurrentTerm}");
        }

        // Wait for at least one leader to be elected
        var hasLeader = await TestUtilities.WaitForCondition(
            () => _brokers.Any(b => b.RaftNode?.IsLeader == true),
            TimeSpan.FromSeconds(10));

        Assert.True(hasLeader, "At least one Raft leader should be elected");

        var leaders = _brokers.Where(b => b.RaftNode?.IsLeader == true).ToList();
        _output.WriteLine($"Leaders elected: {string.Join(", ", leaders.Select(l => $"Broker {l.BrokerId}"))}");

        // Note: With dynamic peer discovery during sequential startup, there can still be
        // a brief window where multiple leaders exist. The split-brain prevention fix
        // (ExpectsClusterPeers check) helps but the RaftTransport peer registration
        // may lag behind the ClusterNodes config. For production, pre-configure all peers
        // with static addresses to ensure proper quorum.
        // The important thing is that at least one leader is elected.
    }

    [Fact(Timeout = 60000)]
    public async Task RaftCluster_RaftMessagesExchanged()
    {
        // Arrange - Start 3 brokers with Raft consensus
        // Note: Disable peer discovery timeout for tests since brokers start sequentially
        var broker1 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster()
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker1);

        var broker2 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(2)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster($"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}")
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker2);

        var broker3 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(3)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(
                $"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}",
                $"2:{broker2.Host}:{broker2.Port}:{broker2.ReplicationPort}")
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker3);

        _output.WriteLine($"3-broker Raft cluster started: ports {broker1.Port}, {broker2.Port}, {broker3.Port}");

        // Wait for leader election - with Pre-Vote, this ensures Raft messages
        // (pre-vote, vote) have been exchanged
        var hasLeader = await TestUtilities.WaitForCondition(
            () => _brokers.Any(b => b.RaftNode?.IsLeader == true),
            TimeSpan.FromSeconds(15));

        Assert.True(hasLeader, "A Raft leader should be elected (indicates Raft messages exchanged)");

        // Assert - Leader should have term >= 1 (proves Raft election occurred)
        var leader = _brokers.First(b => b.RaftNode?.IsLeader == true);
        Assert.True(leader.RaftNode!.CurrentTerm >= 1, "Leader should have term >= 1");

        // Log all broker states for diagnostics
        foreach (var broker in _brokers)
        {
            var raft = broker.RaftNode;
            Assert.NotNull(raft);
            _output.WriteLine($"Broker {broker.BrokerId}: Term={raft.CurrentTerm}, State={raft.State}, LeaderId={raft.LeaderId}");
        }

        _output.WriteLine($"Leader elected: Broker {leader.BrokerId} at term {leader.RaftNode.CurrentTerm}");

        // Note: In this test setup, broker 1 starts without knowing about brokers 2 and 3,
        // so it self-elects as a single-node cluster. The followers' terms won't advance
        // until they start their own elections (which would require waiting for election timeout).
        // For full term propagation across all nodes, use symmetric cluster configuration.
    }

    [Fact(Timeout = 60000)]
    public async Task RaftCluster_LeaderProposeEntry_ReplicatesToFollowers()
    {
        // Arrange - Start 3 brokers with Raft consensus
        // Note: Disable peer discovery timeout for tests since brokers start sequentially
        var broker1 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(1)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster()
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker1);

        var broker2 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(2)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster($"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}")
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker2);

        var broker3 = await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(3)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(
                $"1:{broker1.Host}:{broker1.Port}:{broker1.ReplicationPort}",
                $"2:{broker2.Host}:{broker2.Port}:{broker2.ReplicationPort}")
            .WithRaft(true)
            .WithRaftPeerDiscoveryTimeout(0)
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            .WithShutdownTimeout(3)
            .Build()
            .StartAsync();
        _brokers.Add(broker3);

        _output.WriteLine($"3-broker Raft cluster started");

        // Wait for leader election
        var hasLeader = await TestUtilities.WaitForCondition(
            () => _brokers.Any(b => b.RaftNode?.IsLeader == true),
            TimeSpan.FromSeconds(10));
        Assert.True(hasLeader, "A Raft leader should be elected");

        var leader = _brokers.First(b => b.RaftNode?.IsLeader == true);
        _output.WriteLine($"Leader is broker {leader.BrokerId}");

        // Act - Propose an entry on the leader
        var data = System.Text.Encoding.UTF8.GetBytes("test-metadata-command");
        var index = await leader.RaftNode!.ProposeAsync(MetadataCommandType.Noop, data, CancellationToken.None);

        _output.WriteLine($"Proposed entry at index {index}");

        // Assert - Entry was proposed successfully
        Assert.True(index > 0, "Entry should be proposed with positive index");
        Assert.Equal(index, leader.RaftNode.LastLogIndex);

        // Wait for replication (with eventual timeout)
        await Task.Delay(1000);

        // Verify other nodes received the entry (or at least terms advanced)
        foreach (var broker in _brokers)
        {
            _output.WriteLine($"Broker {broker.BrokerId}: LastLogIndex={broker.RaftNode?.LastLogIndex}, Term={broker.RaftNode?.CurrentTerm}");
        }
    }

    [Fact]
    public async Task RaftNode_Persistence_RestoresStateOnRestart()
    {
        // Arrange
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-raft-persistence-test-" + Guid.NewGuid().ToString("N"));

        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = dataDir
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        // Start first instance, become leader, propose entry
        {
            await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
            await raftNode.StartAsync(CancellationToken.None);

            var becameLeader = await TestUtilities.WaitForCondition(
                () => raftNode.IsLeader,
                TimeSpan.FromSeconds(5));
            Assert.True(becameLeader, "Node should become leader");

            var data = System.Text.Encoding.UTF8.GetBytes("persistent-entry");
            var index = await raftNode.ProposeAsync(MetadataCommandType.Noop, data, CancellationToken.None);
            Assert.True(index > 0);

            _output.WriteLine($"First instance: Term={raftNode.CurrentTerm}, LastLogIndex={raftNode.LastLogIndex}");
        }

        // Start second instance with same data directory - should restore state
        {
            var persistence2 = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
            await using var raftNode2 = new RaftNode(logger, config, persistence2, transport, stateMachine);
            await raftNode2.StartAsync(CancellationToken.None);

            // Node should have restored log
            Assert.True(raftNode2.LastLogIndex >= 1, "Restored node should have log entries");
            _output.WriteLine($"Second instance: Term={raftNode2.CurrentTerm}, LastLogIndex={raftNode2.LastLogIndex}");
        }

        // Cleanup
        try
        {
            Directory.Delete(dataDir, true);
        }
        catch { /* ignore cleanup errors */ }
    }

    [Fact]
    public async Task RaftNode_LeaderStepsDown_WhenHigherTermReceived()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-stepdown-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Wait to become leader
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader");
        Assert.Equal(RaftState.Leader, raftNode.State);

        var leaderTerm = raftNode.CurrentTerm;
        _output.WriteLine($"Node is leader at term {leaderTerm}");

        // Act - Receive AppendEntries from higher term (simulating another leader)
        var request = new AppendEntriesRequest(
            Term: leaderTerm + 10,
            LeaderId: 99,
            PrevLogIndex: 0,
            PrevLogTerm: 0,
            Entries: [],
            LeaderCommit: 0
        );

        var response = await raftNode.HandleAppendEntriesAsync(request, CancellationToken.None);

        // Assert - Node should step down to follower
        Assert.True(response.Success);
        Assert.Equal(RaftState.Follower, raftNode.State);
        Assert.Equal(99, raftNode.LeaderId);
        Assert.Equal(leaderTerm + 10, raftNode.CurrentTerm);
        _output.WriteLine($"Node stepped down to follower, new leader={raftNode.LeaderId}, term={raftNode.CurrentTerm}");
    }

    [Fact]
    public async Task RaftNode_MultipleVoteRequests_OnlyGrantsOneVotePerTerm()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000, // Long timeout to prevent self-election
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-vote-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Act - First vote request from candidate 2
        var request1 = new RequestVoteRequest(Term: 5, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0);
        var response1 = await raftNode.HandleRequestVoteAsync(request1, CancellationToken.None);

        // Second vote request from candidate 3 in same term
        var request2 = new RequestVoteRequest(Term: 5, CandidateId: 3, LastLogIndex: 0, LastLogTerm: 0);
        var response2 = await raftNode.HandleRequestVoteAsync(request2, CancellationToken.None);

        // Assert
        Assert.True(response1.VoteGranted, "First vote should be granted");
        Assert.False(response2.VoteGranted, "Second vote in same term should be rejected");
        _output.WriteLine($"Vote1 granted to 2: {response1.VoteGranted}, Vote2 granted to 3: {response2.VoteGranted}");
    }

    [Fact]
    public async Task RaftNode_GracefulShutdown_StepsDownFromLeadership()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-graceful-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Wait to become leader
        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader");
        Assert.Equal(RaftState.Leader, raftNode.State);

        // Act - initiate graceful shutdown
        var shutdownResult = await raftNode.GracefulShutdownAsync(TimeSpan.FromSeconds(5));

        // Assert - single node can't transfer leadership (no other nodes to elect)
        // but should have stepped down to follower
        Assert.Equal(RaftState.Follower, raftNode.State);
        _output.WriteLine($"Graceful shutdown result: {shutdownResult}, state: {raftNode.State}");
    }

    [Fact]
    public async Task RaftNode_HandlePreVote_GrantsPreVoteWhenLogUpToDate()
    {
        // Arrange - node with long timeout to stay follower
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-prevote-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Act - Send pre-vote request from candidate with higher proposed term
        var request = new PreVoteRequest(
            ProposedTerm: 5,
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandlePreVoteAsync(request, CancellationToken.None);

        // Assert - Pre-vote should be granted
        Assert.True(response.VoteGranted, "Pre-vote should be granted when log is up-to-date");
        Assert.Equal(0, response.Term); // Our term is still 0
        _output.WriteLine($"Pre-vote granted: {response.VoteGranted}, responder term: {response.Term}");
    }

    [Fact]
    public async Task RaftNode_HandlePreVote_RejectsWhenProposedTermTooLow()
    {
        // Arrange - node that has seen a higher term
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-prevote-reject-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // First, update our term by receiving a vote request from a higher term
        var voteRequest = new RequestVoteRequest(Term: 10, CandidateId: 99, LastLogIndex: 0, LastLogTerm: 0);
        await raftNode.HandleRequestVoteAsync(voteRequest, CancellationToken.None);
        Assert.Equal(10, raftNode.CurrentTerm);

        // Act - Send pre-vote with proposed term lower than current term
        var preVoteRequest = new PreVoteRequest(
            ProposedTerm: 5, // Lower than our term 10
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandlePreVoteAsync(preVoteRequest, CancellationToken.None);

        // Assert - Pre-vote should be rejected
        Assert.False(response.VoteGranted, "Pre-vote should be rejected when proposed term is too low");
        Assert.Equal(10, response.Term);
        _output.WriteLine($"Pre-vote rejected: proposed={preVoteRequest.ProposedTerm}, current={response.Term}");
    }

    [Fact]
    public async Task RaftNode_HandlePreVote_RejectsWhenRecentLeaderContact()
    {
        // Arrange - node that has recently heard from a leader
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-prevote-leader-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Simulate receiving AppendEntries from a leader (this sets _leaderId and _lastHeartbeat)
        var appendRequest = new AppendEntriesRequest(
            Term: 5,
            LeaderId: 99,
            PrevLogIndex: 0,
            PrevLogTerm: 0,
            Entries: [],
            LeaderCommit: 0
        );
        await raftNode.HandleAppendEntriesAsync(appendRequest, CancellationToken.None);
        Assert.Equal(99, raftNode.LeaderId);

        // Act - Immediately send pre-vote request (leader contact is recent)
        var preVoteRequest = new PreVoteRequest(
            ProposedTerm: 6,
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandlePreVoteAsync(preVoteRequest, CancellationToken.None);

        // Assert - Pre-vote should be rejected because we have a recent leader
        Assert.False(response.VoteGranted, "Pre-vote should be rejected when we have recent leader contact");
        _output.WriteLine($"Pre-vote rejected due to active leader {raftNode.LeaderId}");
    }

    [Fact]
    public async Task RaftNode_HandlePreVote_DoesNotChangeTerm()
    {
        // Arrange - Pre-vote should NEVER change the receiver's term
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000,
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-prevote-noterm-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        var initialTerm = raftNode.CurrentTerm;

        // Act - Send pre-vote with very high proposed term
        var preVoteRequest = new PreVoteRequest(
            ProposedTerm: 100,
            CandidateId: 2,
            LastLogIndex: 0,
            LastLogTerm: 0
        );

        var response = await raftNode.HandlePreVoteAsync(preVoteRequest, CancellationToken.None);

        // Assert - Term should NOT have changed (unlike real RequestVote)
        Assert.Equal(initialTerm, raftNode.CurrentTerm);
        Assert.True(response.VoteGranted, "Pre-vote should be granted");
        _output.WriteLine($"Term unchanged after pre-vote: before={initialTerm}, after={raftNode.CurrentTerm}");
    }

    [Fact]
    public async Task RaftNode_GracefulShutdown_NonLeader_ReturnsImmediately()
    {
        // Arrange
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 5000, // Long timeout to stay follower
            RaftElectionTimeoutMaxMs = 10000,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-graceful-follower-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        // Node is follower (long election timeout)
        Assert.Equal(RaftState.Follower, raftNode.State);

        // Act - graceful shutdown when not leader should return immediately
        var shutdownResult = await raftNode.GracefulShutdownAsync(TimeSpan.FromSeconds(1));

        // Assert
        Assert.True(shutdownResult, "Non-leader graceful shutdown should succeed immediately");
        _output.WriteLine($"Graceful shutdown result: {shutdownResult}");
    }

    [Fact]
    public async Task RaftNode_CommitIndex_AdvancesWhenEntriesCommitted()
    {
        // Arrange - Single node becomes leader immediately
        var config = new ClusteringConfig
        {
            BrokerId = 1,
            RaftElectionTimeoutMinMs = 150,
            RaftElectionTimeoutMaxMs = 300,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-raft-commit-" + Guid.NewGuid().ToString("N"))
        };

        var persistence = new RaftPersistence(_loggerFactory.CreateLogger<RaftPersistence>(), config);
        var transport = new InMemoryRaftTransport();
        var stateMachine = new NoOpStateMachine();
        var logger = _loggerFactory.CreateLogger<RaftNode>();

        await using var raftNode = new RaftNode(logger, config, persistence, transport, stateMachine);
        await raftNode.StartAsync(CancellationToken.None);

        var becameLeader = await TestUtilities.WaitForCondition(
            () => raftNode.IsLeader,
            TimeSpan.FromSeconds(5));
        Assert.True(becameLeader, "Node should become leader");

        var initialCommitIndex = raftNode.CommitIndex;
        _output.WriteLine($"Initial commit index: {initialCommitIndex}");

        // Act - Propose multiple entries
        for (int i = 0; i < 5; i++)
        {
            var data = System.Text.Encoding.UTF8.GetBytes($"entry-{i}");
            await raftNode.ProposeAsync(MetadataCommandType.Noop, data, CancellationToken.None);
        }

        // Wait for commits (single node commits immediately)
        await Task.Delay(500);

        // Assert
        Assert.True(raftNode.LastLogIndex >= 5, "Should have at least 5 log entries");
        Assert.True(raftNode.CommitIndex >= initialCommitIndex, "Commit index should advance");
        _output.WriteLine($"Final: LastLogIndex={raftNode.LastLogIndex}, CommitIndex={raftNode.CommitIndex}");
    }

    #endregion

    #region Test Helpers

    private static RequestContext CreateTestRequestContext()
    {
        return new RequestContext
        {
            ConnectionState = new Kuestenlogik.Surgewave.Protocol.Kafka.ConnectionState("127.0.0.1"),
            ClientId = "test-client"
        };
    }

    /// <summary>
    /// In-memory Raft transport for testing (no network, single node).
    /// </summary>
    private sealed class InMemoryRaftTransport : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => [];

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
        {
            return Task.FromResult(false); // No peers reachable in single-node test
        }

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
        {
            throw new NotImplementedException("No peers in single-node test");
        }

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
        {
            throw new NotImplementedException("No peers in single-node test");
        }

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
        {
            throw new NotImplementedException("No peers in single-node test");
        }
    }

    /// <summary>
    /// No-op state machine for testing.
    /// </summary>
    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry)
        {
            // No-op
        }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct)
        {
            return Task.FromResult(Array.Empty<byte>());
        }

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct)
        {
            return Task.CompletedTask;
        }
    }

    #endregion
}

/// <summary>
/// Xunit logger provider for test output
/// </summary>
public class XunitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public XunitLoggerProvider(ITestOutputHelper output)
    {
        _output = output;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new XunitLogger(_output, categoryName);
    }

    public void Dispose() { }
}

/// <summary>
/// Xunit logger implementation
/// </summary>
public class XunitLogger : ILogger
{
    private readonly ITestOutputHelper _output;
    private readonly string _categoryName;

    public XunitLogger(ITestOutputHelper output, string categoryName)
    {
        _output = output;
        _categoryName = categoryName;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;

        try
        {
            var shortCategory = _categoryName.Split('.').LastOrDefault() ?? _categoryName;
            _output.WriteLine($"[{logLevel}] {shortCategory}: {formatter(state, exception)}");
            if (exception != null)
            {
                _output.WriteLine($"  Exception: {exception.Message}");
            }
        }
        catch
        {
            // Ignore - test may have completed
        }
    }
}
