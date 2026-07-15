using System.Text.Json;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #72 Inc5 — Raft-mode broker registration. <see cref="MetadataStateMachine.Apply"/> of a
/// BrokerRegistered entry rebuilds the membership store with the broker epoch = committed log INDEX
/// (durable, replicated, strictly monotone across failover — KRaft parity), merges cluster state via
/// UpdateBroker (never an AddBroker-clobber), is idempotent on re-apply, and stays backward compatible
/// with pre-Inc5 command JSON (the raft-log.bin frame is unchanged; new fields default).
/// </summary>
public class MetadataStateMachineBrokerRegistrationTests
{
    private static (MetadataStateMachine Sm, ClusterState State, ClusterMembershipService Membership) NewStateMachine()
    {
        var config = new ClusteringConfig { BrokerId = 0 };
        var state = new ClusterState();
        var idManager = new ClusterIdManager(config, NullLogger<ClusterIdManager>.Instance);
        var membership = new ClusterMembershipService(idManager, state, NullLogger<ClusterMembershipService>.Instance);
        var sm = new MetadataStateMachine(NullLogger<MetadataStateMachine>.Instance, state, membership);
        return (sm, state, membership);
    }

    private static RaftLogEntry Entry(long index, BrokerRegisteredCommand cmd) => new()
    {
        Term = 1,
        Index = index,
        CommandType = MetadataCommandType.BrokerRegistered,
        Data = JsonSerializer.SerializeToUtf8Bytes(cmd, ClusteringJsonContext.Default.BrokerRegisteredCommand),
    };

    // A heartbeat carrying `epoch` is accepted (None) only if the store recorded exactly that epoch;
    // any other value fences (StaleBrokerEpoch). This is how we assert the stored epoch value.
    private static ClusterRpcStatus HeartbeatStatus(ClusterMembershipService m, int brokerId, long epoch)
        => m.Heartbeat(new BrokerHeartbeatInput(brokerId, epoch, 0, false, false)).Status;

    [Fact]
    public void ApplyBrokerRegistered_UsesCommittedLogIndexAsEpoch()
    {
        var (sm, state, membership) = NewStateMachine();

        sm.Apply(Entry(42, new BrokerRegisteredCommand(
            1, "h", 9092, "rack-1", Guid.NewGuid(), InterBrokerProtocolFeature.Native, 10999)));

        var node = state.GetBroker(1)!;
        Assert.Equal("h", node.Host);
        Assert.Equal(9092, node.Port);
        Assert.Equal(10999, node.ReplicationPort);
        Assert.Equal(InterBrokerProtocolFeature.Native, node.InterBrokerProtocol);

        // The epoch is the committed log INDEX: a heartbeat at 42 is accepted, at 41 fenced.
        Assert.Equal(ClusterRpcStatus.None, HeartbeatStatus(membership, 1, 42));
        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, HeartbeatStatus(membership, 1, 41));
    }

    [Fact]
    public void ApplyBrokerRegistered_IsIdempotentOnReplay()
    {
        var (sm, _, membership) = NewStateMachine();
        var incarnation = Guid.NewGuid();
        var entry = Entry(7, new BrokerRegisteredCommand(1, "h", 9092, null, incarnation, InterBrokerProtocolFeature.Native, 0));

        sm.Apply(entry);
        sm.Apply(entry); // replay the SAME committed entry — must keep the same epoch

        Assert.Equal(ClusterRpcStatus.None, HeartbeatStatus(membership, 1, 7));
    }

    [Fact]
    public void ApplyBrokerRegistered_ReRegistrationAtHigherIndex_AdvancesEpoch()
    {
        var (sm, _, membership) = NewStateMachine();

        sm.Apply(Entry(7, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));
        // Same broker re-registers with a NEW incarnation, committed at a higher index.
        sm.Apply(Entry(19, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));

        Assert.Equal(ClusterRpcStatus.None, HeartbeatStatus(membership, 1, 19));
        Assert.Equal(ClusterRpcStatus.StaleBrokerEpoch, HeartbeatStatus(membership, 1, 7)); // the old epoch is fenced
    }

    [Fact]
    public void ApplyBrokerRegistered_ReRegistration_StartsFenced_EvenIfPriorWasUnfenced()
    {
        var (sm, _, membership) = NewStateMachine();

        sm.Apply(Entry(5, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));
        // A caught-up (offset >= 0), non-fence-requesting heartbeat unfences the broker.
        Assert.False(membership.Heartbeat(new BrokerHeartbeatInput(1, 5, 0, false, false)).IsFenced);

        // A re-registration under a NEW incarnation (higher committed index) must start fenced again —
        // it is a fresh join that has not yet caught up, not an idempotent replay of the same entry.
        sm.Apply(Entry(9, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));
        Assert.True(membership.IsBrokerFenced(1));
    }

    [Fact]
    public void ApplyBrokerRegistered_SetsFinalizedInterBrokerProtocol()
    {
        var (sm, state, _) = NewStateMachine();

        sm.Apply(Entry(1, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));
        sm.Apply(Entry(2, new BrokerRegisteredCommand(2, "h", 9093, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));

        Assert.Equal(InterBrokerProtocolFeature.Native, state.FinalizedInterBrokerProtocol);
    }

    [Fact]
    public void ApplyBrokerRegistered_MergePreservesDiscoveredReplicationPort()
    {
        var (sm, state, _) = NewStateMachine();

        // First entry advertises a replication port; a later entry for the same broker omits it (0).
        sm.Apply(Entry(1, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 10999)));
        sm.Apply(Entry(2, new BrokerRegisteredCommand(1, "h", 9092, null, Guid.NewGuid(), InterBrokerProtocolFeature.Native, 0)));

        // UpdateBroker-merge keeps the discovered replication port instead of clobbering it to the default.
        Assert.Equal(10999, state.GetBroker(1)!.ReplicationPort);
    }

    [Fact]
    public void ApplyBrokerRegistered_PreInc5Json_ReplaysWithSafeDefaults()
    {
        var (sm, state, membership) = NewStateMachine();

        // A pre-Inc5 log entry: the command JSON carries only the original four fields (camelCase, the
        // context's naming policy). The raft-log.bin frame is unchanged, so it must still deserialize —
        // with IncarnationId=empty, level=0 (Kafka wire) and replicationPort=0.
        var preInc5Json = "{\"brokerId\":5,\"host\":\"h\",\"port\":9097}"u8.ToArray();
        sm.Apply(new RaftLogEntry
        {
            Term = 1,
            Index = 3,
            CommandType = MetadataCommandType.BrokerRegistered,
            Data = preInc5Json,
        });

        var node = state.GetBroker(5)!;
        Assert.Equal(9097, node.Port);
        Assert.Equal(InterBrokerProtocolFeature.KafkaWire, node.InterBrokerProtocol); // absent level = 0
        Assert.Equal(ClusterRpcStatus.None, HeartbeatStatus(membership, 5, 3)); // epoch = index still works
    }
}
