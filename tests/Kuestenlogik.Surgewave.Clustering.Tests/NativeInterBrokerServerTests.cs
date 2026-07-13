using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc4/Inc5 — coverage for the native inter-broker receive server: decode → dispatch → encode
/// for the wired controller-plane ops (LeaderAndIsr / UpdateMetadata / StopReplica / AlterPartition),
/// the controller-epoch fence, the error shape for opcodes that are in-band but not yet wired, and an
/// end-to-end round trip over a real TCP loopback stream.
/// </summary>
public class NativeInterBrokerServerTests
{
    private sealed record ServerFixture(
        NativeInterBrokerServer Server,
        ClusterState State,
        ReplicaManager Replicas,
        StubIsrApplier IsrApplier);

    private static ServerFixture NewServer(int localBrokerId = 0)
    {
        var state = new ClusterState();
        var config = new ClusteringConfig
        {
            BrokerId = localBrokerId,
            Host = "localhost",
            Port = 9092 + localBrokerId,
            RebalanceCheckIntervalSeconds = 5,
        };
        var logs = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(
            NullLogger<ReplicaManager>.Instance, state, logs, config, new TcpPeerTransport());
        var isrApplier = new StubIsrApplier();
        var service = new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance,
            state, replicas, logs, localBrokerId, isrApplier);
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance, service);
        return new(server, state, replicas, isrApplier);
    }

    internal sealed class StubIsrApplier : IIsrUpdateApplier
    {
        public bool IsController { get; set; }
        public PartitionState? Result { get; set; }
        public (TopicPartition Tp, int LeaderId, int LeaderEpoch, IReadOnlyList<int> NewIsr)? LastApply { get; private set; }

        public Task<PartitionState?> ApplyIsrUpdateAsync(
            TopicPartition tp, int leaderId, int leaderEpoch, IReadOnlyList<int> newIsr, CancellationToken ct = default)
        {
            LastApply = (tp, leaderId, leaderEpoch, newIsr);
            return Task.FromResult(Result);
        }
    }

    private static PartitionStatesPayload PartitionStates(
        TopicPartition tp, int leader, int leaderEpoch, List<int> replicas, List<int> isr,
        int controllerId = 1, int controllerEpoch = 0, IReadOnlyList<LiveBrokerSpec>? liveBrokers = null)
        => new(controllerId, controllerEpoch, liveBrokers ?? [],
            [(tp, new PartitionState { TopicPartition = tp, LeaderBrokerId = leader, LeaderEpoch = leaderEpoch, Replicas = replicas, Isr = isr })]);

    private static (SurgewaveOpCode Opcode, ClusterRpcStatus Status) DecodeStatusFrame(byte[] frame)
    {
        // Skip the [int32 size] prefix, then read [uint16 opcode][int16 status].
        var reader = new SurgewavePayloadReader(frame.AsSpan(4));
        var opcode = (SurgewaveOpCode)reader.ReadUInt16();
        return (opcode, InterBrokerStatusPayload.Read(ref reader).Status);
    }

    private static async ValueTask<(SurgewaveOpCode Opcode, ClusterRpcStatus Status)> ProcessAsync<TPayload>(
        NativeInterBrokerServer server, SurgewaveOpCode opcode, TPayload payload)
        where TPayload : Protocol.Native.Serialization.ISerializablePayload<TPayload>
    {
        var bytes = InterBrokerFrameCodec.EncodePayload(payload);
        var response = await server.ProcessAsync(opcode, bytes, CancellationToken.None);
        return DecodeStatusFrame(response);
    }

    // ── UpdateMetadata ───────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateMetadata_AppliesToClusterStateAndAcks()
    {
        var fx = NewServer();
        var tp = new TopicPartition { Topic = "orders", Partition = 2 };

        var (opcode, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(tp, leader: 4, leaderEpoch: 9, replicas: [4, 5, 6], isr: [4, 5]));

        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, opcode);
        Assert.Equal(ClusterRpcStatus.None, status);

        var applied = fx.State.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(4, applied!.LeaderBrokerId);
        Assert.Equal(9, applied.LeaderEpoch);
        Assert.Equal(new[] { 4, 5, 6 }, applied.Replicas);
        Assert.Equal(new[] { 4, 5 }, applied.Isr);
    }

    [Fact]
    public async Task UpdateMetadata_AdvancesControllerIdAndEpoch()
    {
        var fx = NewServer();
        var tp = new TopicPartition { Topic = "t", Partition = 0 };

        await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(tp, 1, 1, [1], [1], controllerId: 7, controllerEpoch: 12));

        Assert.Equal(7, fx.State.ControllerId);
        Assert.Equal(12, fx.State.ControllerEpoch);
    }

    [Fact]
    public async Task UpdateMetadata_StaleControllerEpoch_RejectsWithoutApplying()
    {
        var fx = NewServer();
        fx.State.ControllerEpoch = 5;
        fx.State.ControllerId = 9;
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };

        // A delayed push from a demoted controller (epoch 4 < current 5) must not regress metadata.
        var (opcode, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(tp, leader: 4, leaderEpoch: 9, replicas: [4], isr: [4], controllerId: 4, controllerEpoch: 4));

        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, opcode);
        Assert.Equal(ClusterRpcStatus.StaleControllerEpoch, status);
        Assert.Null(fx.State.GetPartitionState(tp)); // nothing applied
        Assert.Equal(5, fx.State.ControllerEpoch);   // epoch not regressed
        Assert.Equal(9, fx.State.ControllerId);
    }

    [Fact]
    public async Task UpdateMetadata_WithBogusBrokerCount_RejectsCleanlyWithoutHugeAllocation()
    {
        var fx = NewServer();

        // Hostile payload: valid controller id/epoch, then int32 broker count = 0x40000000 with no
        // entries following. The decoder must reject it (bounds guard) before pre-allocating,
        // degrading to a clean Error frame. Layout: controllerId(4) epoch(4) brokerCount(4).
        byte[] bogus = [0, 0, 0, 1, 0, 0, 0, 1, 0x40, 0x00, 0x00, 0x00];
        var response = await fx.Server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, bogus, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.Unknown, status);
    }

    [Fact]
    public async Task UpdateMetadata_WithBogusEntryCount_RejectsCleanlyWithoutHugeAllocation()
    {
        var fx = NewServer();

        // Hostile payload: valid controller id/epoch, zero brokers, then int32 entry count =
        // 0x40000000 with no entries following (~17 GB pre-allocation without the guard).
        // Layout: controllerId(4) epoch(4) brokerCount=0(4) entryCount(4).
        byte[] bogus = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0x40, 0x00, 0x00, 0x00];
        var response = await fx.Server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, bogus, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.Unknown, status);
    }

    [Fact]
    public async Task LeaderAndIsr_WithLiveBrokers_LearnsUnknownBrokerAndConvergesKnownLevel()
    {
        var fx = NewServer(localBrokerId: 3);
        // Broker 5 is already known from config discovery — its endpoint must NOT be clobbered,
        // but its advertised protocol level must converge.
        fx.State.AddBroker(new BrokerNode { BrokerId = 5, Host = "cfg-host", Port = 9097, ReplicationPort = 12345 });
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };

        var (_, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerLeaderAndIsr,
            PartitionStates(tp, leader: 9, leaderEpoch: 1, replicas: [9, 3], isr: [9], liveBrokers:
            [
                new LiveBrokerSpec(BrokerId: 9, Host: "10.0.0.9", Port: 9092, ReplicationPort: 10999, InterBrokerProtocol: 1, Rack: null),
                new LiveBrokerSpec(BrokerId: 5, Host: "push-host", Port: 1, ReplicationPort: 2, InterBrokerProtocol: 1, Rack: null),
            ]));

        Assert.Equal(ClusterRpcStatus.None, status);

        // Unknown leader 9 was learned with its full inter-broker identity — the follower can fetch.
        var learned = fx.State.GetBroker(9);
        Assert.NotNull(learned);
        Assert.Equal("10.0.0.9", learned!.Host);
        Assert.Equal(10999, learned.ReplicationPort);
        Assert.Equal((short)1, learned.InterBrokerProtocol);

        // Known broker 5 kept its discovered endpoint but converged its level.
        var known = fx.State.GetBroker(5);
        Assert.NotNull(known);
        Assert.Equal("cfg-host", known!.Host);
        Assert.Equal(12345, known.ReplicationPort);
        Assert.Equal((short)1, known.InterBrokerProtocol);
    }

    [Fact]
    public async Task UpdateMetadata_DelayedLowerLeaderEpoch_IsSkippedPerPartitionWithoutRegressing()
    {
        var fx = NewServer();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };

        // Fresh push: same controller epoch, partition at leader epoch 9, leader 4.
        var (_, s1) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(tp, leader: 4, leaderEpoch: 9, replicas: [4], isr: [4], controllerEpoch: 3));
        Assert.Equal(ClusterRpcStatus.None, s1);

        // Delayed/reordered push: SAME controller epoch but an OLDER partition leader epoch (2) and a
        // stale leader — the push is accepted at the controller level, but the partition entry is
        // skipped by the per-partition guard, so the fresh leader is not regressed.
        var (_, s2) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(tp, leader: 1, leaderEpoch: 2, replicas: [1], isr: [1], controllerEpoch: 3));
        Assert.Equal(ClusterRpcStatus.None, s2); // push accepted; the stale entry is silently skipped

        var applied = fx.State.GetPartitionState(tp);
        Assert.Equal(4, applied!.LeaderBrokerId); // still the fresh push's leader
        Assert.Equal(9, applied.LeaderEpoch);
    }

    [Fact]
    public async Task UpdateMetadata_DisjointPartitions_BothApply()
    {
        // The regression guard: two same-epoch pushes for DIFFERENT partitions must both apply — a
        // coarse per-push version fence would have dropped the second.
        var fx = NewServer();
        var a = new TopicPartition { Topic = "orders", Partition = 0 };
        var b = new TopicPartition { Topic = "orders", Partition = 1 };

        await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(a, leader: 4, leaderEpoch: 9, replicas: [4], isr: [4], controllerEpoch: 3));
        await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerUpdateMetadata,
            PartitionStates(b, leader: 5, leaderEpoch: 2, replicas: [5], isr: [5], controllerEpoch: 3));

        Assert.Equal(4, fx.State.GetPartitionState(a)!.LeaderBrokerId);
        Assert.Equal(5, fx.State.GetPartitionState(b)!.LeaderBrokerId); // NOT dropped despite lower epoch
    }

    // ── LeaderAndIsr ─────────────────────────────────────────────────────────

    [Fact]
    public async Task LeaderAndIsr_LocalBrokerIsLeader_BecomesLeader()
    {
        var fx = NewServer(localBrokerId: 3);
        var tp = new TopicPartition { Topic = "orders", Partition = 1 };

        var (opcode, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerLeaderAndIsr,
            PartitionStates(tp, leader: 3, leaderEpoch: 2, replicas: [3, 1], isr: [3]));

        Assert.Equal(SurgewaveOpCode.InterBrokerLeaderAndIsr, opcode);
        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.True(fx.Replicas.IsLeader(tp));

        var applied = fx.State.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(3, applied!.LeaderBrokerId);
        Assert.Equal(2, applied.LeaderEpoch);
    }

    [Fact]
    public async Task LeaderAndIsr_StaleControllerEpoch_RejectsWithoutTransition()
    {
        var fx = NewServer(localBrokerId: 3);
        fx.State.ControllerEpoch = 10;
        var tp = new TopicPartition { Topic = "orders", Partition = 1 };

        var (_, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerLeaderAndIsr,
            PartitionStates(tp, leader: 3, leaderEpoch: 2, replicas: [3, 1], isr: [3], controllerEpoch: 9));

        Assert.Equal(ClusterRpcStatus.StaleControllerEpoch, status);
        Assert.False(fx.Replicas.IsLeader(tp));
        Assert.Null(fx.State.GetPartitionState(tp));
    }

    // ── StopReplica ──────────────────────────────────────────────────────────

    [Fact]
    public async Task StopReplica_DeleteOnTargetBroker_RemovesPartitionState()
    {
        var fx = NewServer(localBrokerId: 3);
        var tp = new TopicPartition { Topic = "orders", Partition = 1 };

        await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerLeaderAndIsr,
            PartitionStates(tp, leader: 3, leaderEpoch: 2, replicas: [3], isr: [3]));
        Assert.True(fx.Replicas.IsLeader(tp));

        var (opcode, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerStopReplica,
            new StopReplicaPayload(ControllerId: 1, ControllerEpoch: 0, BrokerId: 3, [(tp, 2, true)]));

        Assert.Equal(SurgewaveOpCode.InterBrokerStopReplica, opcode);
        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.False(fx.Replicas.IsLeader(tp));
        Assert.Null(fx.State.GetPartitionState(tp));
    }

    [Fact]
    public async Task StopReplica_AddressedToOtherBroker_RefusedWithoutStopping()
    {
        var fx = NewServer(localBrokerId: 3);
        var tp = new TopicPartition { Topic = "orders", Partition = 1 };

        await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerLeaderAndIsr,
            PartitionStates(tp, leader: 3, leaderEpoch: 2, replicas: [3], isr: [3]));

        // A stop can delete data, so a frame addressed to broker 4 must be refused by broker 3.
        var (_, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerStopReplica,
            new StopReplicaPayload(ControllerId: 1, ControllerEpoch: 0, BrokerId: 4, [(tp, 2, true)]));

        Assert.Equal(ClusterRpcStatus.ReplicaNotAvailable, status);
        Assert.True(fx.Replicas.IsLeader(tp));
        Assert.NotNull(fx.State.GetPartitionState(tp));
    }

    [Fact]
    public async Task StopReplica_WithBogusCount_RejectsCleanlyWithoutHugeAllocation()
    {
        var fx = NewServer();

        // Hostile payload: valid controllerId(4) epoch(4) brokerId(4), then int32 partition count =
        // 0x40000000 with no entries following — must hit the bounds guard.
        byte[] bogus = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 0x40, 0x00, 0x00, 0x00];
        var response = await fx.Server.ProcessAsync(SurgewaveOpCode.InterBrokerStopReplica, bogus, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.Unknown, status);
    }

    // ── AlterPartition (reverse ISR, #69) ────────────────────────────────────

    [Fact]
    public async Task AlterPartition_AsController_AppliesViaIsrApplier()
    {
        var fx = NewServer();
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        fx.IsrApplier.IsController = true;
        fx.IsrApplier.Result = new PartitionState { TopicPartition = tp, LeaderBrokerId = 2, LeaderEpoch = 6, Isr = [2, 1] };

        var (opcode, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerAlterPartition,
            new AlterPartitionPayload(LeaderId: 2, LeaderEpoch: 6, tp, NewIsr: [2, 1]));

        Assert.Equal(SurgewaveOpCode.InterBrokerAlterPartition, opcode);
        Assert.Equal(ClusterRpcStatus.None, status);
        Assert.Equal((tp, 2, 6), (fx.IsrApplier.LastApply!.Value.Tp, fx.IsrApplier.LastApply.Value.LeaderId, fx.IsrApplier.LastApply.Value.LeaderEpoch));
        Assert.Equal([2, 1], fx.IsrApplier.LastApply.Value.NewIsr);
    }

    [Fact]
    public async Task AlterPartition_NotController_RepliesNotController()
    {
        var fx = NewServer();
        fx.IsrApplier.IsController = false;

        var (_, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerAlterPartition,
            new AlterPartitionPayload(1, 1, new TopicPartition { Topic = "t", Partition = 0 }, [1]));

        Assert.Equal(ClusterRpcStatus.NotController, status);
        Assert.Null(fx.IsrApplier.LastApply);
    }

    [Fact]
    public async Task AlterPartition_UnknownPartition_RepliesUnknownTopicOrPartition()
    {
        var fx = NewServer();
        fx.IsrApplier.IsController = true;
        fx.IsrApplier.Result = null; // controller doesn't track this partition

        var (_, status) = await ProcessAsync(fx.Server, SurgewaveOpCode.InterBrokerAlterPartition,
            new AlterPartitionPayload(1, 1, new TopicPartition { Topic = "ghost", Partition = 0 }, [1]));

        Assert.Equal(ClusterRpcStatus.UnknownTopicOrPartition, status);
    }

    // ── Dispatch edges ───────────────────────────────────────────────────────

    [Fact]
    public async Task InBandButUnwiredOpcode_RepliesErrorUnsupportedVersion()
    {
        var fx = NewServer();

        // Registration is in the native band but has no handler yet (Inc6).
        var response = await fx.Server.ProcessAsync(SurgewaveOpCode.InterBrokerRegistration, ReadOnlyMemory<byte>.Empty, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.UnsupportedVersion, status);
    }

    [Fact]
    public async Task WiredOpcodeWithoutService_RepliesNotController()
    {
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance, service: null);

        var payload = InterBrokerFrameCodec.EncodePayload(
            PartitionStates(new TopicPartition { Topic = "t", Partition = 0 }, 1, 1, [1], [1]));
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, payload, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.NotController, status);
    }

    // ── End-to-end loopback ──────────────────────────────────────────────────

    [Fact]
    public async Task Loopback_UpdateMetadataOverTcp_AppliesAndAcks()
    {
        var fx = NewServer();

        var transport = new TcpPeerTransport();
        await using var listener = transport.CreateListener(new IPEndPoint(IPAddress.Loopback, 0));
        await listener.StartAsync();
        var port = listener.LocalEndPoint.Port;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var acceptTask = listener.AcceptAsync(cts.Token);
        await using var clientConn = await transport.ConnectAsync("127.0.0.1", port, cts.Token);
        await using var serverConn = await acceptTask;

        // Server side: accept one inbound stream and handle a single RPC.
        var serverTask = Task.Run(async () =>
        {
            await using var lease = await serverConn.AcceptInboundStreamAsync(cts.Token);
            await fx.Server.HandleSingleAsync(lease.Stream, cts.Token);
        }, cts.Token);

        // Client side: write the request frame, then read the response frame.
        var tp = new TopicPartition { Topic = "orders", Partition = 7 };
        var frame = InterBrokerFrameCodec.EncodeFrame(
            SurgewaveOpCode.InterBrokerUpdateMetadata, PartitionStates(tp, leader: 3, leaderEpoch: 11, replicas: [3, 1], isr: [3]));

        await using var clientLease = await clientConn.AcquireStreamAsync(cts.Token);
        await clientLease.Stream.WriteAsync(frame, cts.Token);
        await clientLease.Stream.FlushAsync(cts.Token);

        var response = await InterBrokerFrameCodec.ReadFrameAsync(clientLease.Stream, cts.Token);
        await serverTask;

        Assert.NotNull(response);
        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, response!.Value.Opcode);
        var reader = new SurgewavePayloadReader(response.Value.Payload.Span);
        Assert.Equal(ClusterRpcStatus.None, InterBrokerStatusPayload.Read(ref reader).Status);

        var applied = fx.State.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(3, applied!.LeaderBrokerId);
        Assert.Equal(11, applied.LeaderEpoch);
    }
}
