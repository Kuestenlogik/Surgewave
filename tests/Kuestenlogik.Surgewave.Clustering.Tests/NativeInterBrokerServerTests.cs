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
/// for the wired ops, the error shape for opcodes that are in-band but not wired, and an end-to-end
/// round trip over a real TCP loopback stream. The controller-push ops (LeaderAndIsr /
/// UpdateMetadata / StopReplica) went with the push path itself (#163 step 3) — the controller now
/// replicates its decisions through the Raft log, so the only inbound control-plane RPC left here is
/// the reverse AlterPartition report.
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

        // LeaderAndIsr is in the native band and deliberately has no handler: it was a controller
        // push, and the pushes are gone (#163 step 3). ControlledShutdown used to stand here and no
        // longer can — it is wired now (#180).
        var response = await fx.Server.ProcessAsync(SurgewaveOpCode.InterBrokerLeaderAndIsr, ReadOnlyMemory<byte>.Empty, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.UnsupportedVersion, status);
    }

    [Fact]
    public async Task WiredOpcodeWithoutService_RepliesNotController()
    {
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance, service: null);

        var payload = InterBrokerFrameCodec.EncodePayload(
            new AlterPartitionPayload(1, 1, new TopicPartition { Topic = "t", Partition = 0 }, [1]));
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerAlterPartition, payload, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.NotController, status);
    }

    // ── End-to-end loopback ──────────────────────────────────────────────────

    [Fact]
    public async Task Loopback_AlterPartitionOverTcp_AppliesAndAcks()
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
        fx.IsrApplier.IsController = true;
        fx.IsrApplier.Result = new PartitionState { TopicPartition = tp, LeaderBrokerId = 3, LeaderEpoch = 11, Isr = [3, 1] };
        var frame = InterBrokerFrameCodec.EncodeFrame(
            SurgewaveOpCode.InterBrokerAlterPartition, new AlterPartitionPayload(3, 11, tp, [3, 1]));

        await using var clientLease = await clientConn.AcquireStreamAsync(cts.Token);
        await clientLease.Stream.WriteAsync(frame, cts.Token);
        await clientLease.Stream.FlushAsync(cts.Token);

        var response = await InterBrokerFrameCodec.ReadFrameAsync(clientLease.Stream, cts.Token);
        await serverTask;

        Assert.NotNull(response);
        Assert.Equal(SurgewaveOpCode.InterBrokerAlterPartition, response!.Value.Opcode);
        var reader = new SurgewavePayloadReader(response.Value.Payload.Span);
        Assert.Equal(ClusterRpcStatus.None, InterBrokerStatusPayload.Read(ref reader).Status);

        var applied = fx.IsrApplier.LastApply;
        Assert.NotNull(applied);
        Assert.Equal((tp, 3, 11), (applied!.Value.Tp, applied.Value.LeaderId, applied.Value.LeaderEpoch));
        Assert.Equal([3, 1], applied.Value.NewIsr);
    }
}
