using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// DescribeQuorum has to report the quorum as it actually is, now that not every node in it
/// votes (#167).
/// </summary>
/// <remarks>
/// The handler used to answer from cluster state alone — every known broker was a voter and
/// the observer list was always empty, which was true only for as long as those were the same
/// set. Reporting an observer as a voter overstates how many nodes must agree, and that number
/// is the one thing an operator reads this API for.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class DescribeQuorumObserverTests
{
    private const string MetadataTopic = "__cluster_metadata";

    [Fact]
    public async Task ABrokerOutsideTheQuorumIsReportedAsAnObserver()
    {
        // Voters 1 and 2, with 3 along for the log only.
        var partition = await DescribeAsync(localBrokerId: 1, brokers: [1, 2, 3], voters: [1, 2]);

        Assert.Equal([1, 2], partition.CurrentVoters.Select(v => v.ReplicaId).Order());
        Assert.Equal([3], partition.Observers.Select(o => o.ReplicaId));
    }

    [Fact]
    public async Task ANodeThatDoesNotVoteReportsItselfAsAnObserver()
    {
        // Answered by broker 3, which is the one outside the quorum. The node has to classify
        // itself by the same rule it applies to everyone else — the old code added self to the
        // voters unconditionally.
        var partition = await DescribeAsync(localBrokerId: 3, brokers: [1, 2, 3], voters: [1, 2]);

        Assert.DoesNotContain(3, partition.CurrentVoters.Select(v => v.ReplicaId));
        Assert.Contains(3, partition.Observers.Select(o => o.ReplicaId));
    }

    [Fact]
    public async Task InCombinedModeEveryBrokerIsStillAVoter()
    {
        // The shape a single broker and an embedded host run in: nothing about the answer
        // changes, and the observer list stays empty.
        var partition = await DescribeAsync(localBrokerId: 1, brokers: [1, 2, 3], voters: [1, 2, 3]);

        Assert.Equal([1, 2, 3], partition.CurrentVoters.Select(v => v.ReplicaId).Order());
        Assert.Empty(partition.Observers);
    }

    private static async Task<DescribeQuorumResponse.PartitionData> DescribeAsync(
        int localBrokerId, int[] brokers, int[] voters)
    {
        var config = Substitute.For<IBrokerConfigView>();
        config.BrokerId.Returns(localBrokerId);

        var clusterState = new ClusterState();
        foreach (var brokerId in brokers)
        {
            clusterState.AddBroker(new BrokerNode { BrokerId = brokerId, Host = "localhost", Port = 9092 + brokerId });
        }

        var clusteringConfig = new ClusteringConfig
        {
            BrokerId = localBrokerId,
            RaftDataDirectory = Path.Combine(Path.GetTempPath(), "surgewave-describequorum-" + Guid.NewGuid().ToString("N")),
        };
        var persistence = new RaftPersistence(NullLogger<RaftPersistence>.Instance, clusteringConfig);

        // Never started: DescribeQuorum only reads state, and an election would make the
        // answer depend on timing.
        var raftNode = new RaftNode(
            NullLogger<RaftNode>.Instance,
            clusteringConfig,
            persistence,
            new IdleTransport(brokers.Where(id => id != localBrokerId).ToArray()),
            new NoOpStateMachine(),
            new FixedVoterSet(voters));

        var handler = new RaftApiHandler(
            config, raftNode, persistence, clusterState, NullLogger<RaftApiHandler>.Instance);

        var request = new DescribeQuorumRequest
        {
            ApiKey = ApiKey.DescribeQuorum,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = "quorum-admin",
            Topics =
            [
                new DescribeQuorumRequest.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions = [new DescribeQuorumRequest.PartitionData { PartitionIndex = 0 }],
                },
            ],
        };

        var context = new RequestContext
        {
            ConnectionState = new ConnectionState("127.0.0.1"),
            ClientId = "quorum-admin",
        };

        var response = Assert.IsType<DescribeQuorumResponse>(
            await handler.HandleAsync(request, context, CancellationToken.None));

        return Assert.Single(Assert.Single(response.Topics).Partitions);
    }

    private sealed class FixedVoterSet(IReadOnlyList<int> voterIds) : IRaftVoterSet
    {
        public IReadOnlyList<int> VoterIds { get; } = voterIds;

        public int Majority => (VoterIds.Count / 2) + 1;
    }

    private sealed class IdleTransport(IReadOnlyList<int> peers) : IRaftTransport
    {
        public IReadOnlyList<int> GetPeerIds() => peers;

        public Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct) => Task.FromResult(false);

        public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
            => throw new NotSupportedException();
    }

    private sealed class NoOpStateMachine : IRaftStateMachine
    {
        public void Apply(RaftLogEntry entry) { }

        public Task<byte[]> CreateSnapshotAsync(CancellationToken ct) => Task.FromResult(Array.Empty<byte>());

        public Task RestoreFromSnapshotAsync(byte[] snapshot, CancellationToken ct) => Task.CompletedTask;
    }
}
