using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc4 — coverage for the native inter-broker receive server: decode → dispatch → encode for the
/// real UpdateMetadata op (applied to <see cref="ClusterState"/>), the error shape for opcodes that are
/// in-band but not yet wired, and an end-to-end round trip over a real TCP loopback stream.
/// </summary>
public class NativeInterBrokerServerTests
{
    private static NativeInterBrokerServer NewServer(ClusterState clusterState)
        => new(NullLogger<NativeInterBrokerServer>.Instance, new ClusterStateInterBrokerService(clusterState));

    private static PartitionStatesPayload UpdateMetadata(TopicPartition tp, int leader, int leaderEpoch, List<int> replicas, List<int> isr)
        => new([(tp, new PartitionState { TopicPartition = tp, LeaderBrokerId = leader, LeaderEpoch = leaderEpoch, Replicas = replicas, Isr = isr })]);

    private static (SurgewaveOpCode Opcode, ClusterRpcStatus Status) DecodeStatusFrame(byte[] frame)
    {
        // Skip the [int32 size] prefix, then read [uint16 opcode][int16 status].
        var reader = new SurgewavePayloadReader(frame.AsSpan(4));
        var opcode = (SurgewaveOpCode)reader.ReadUInt16();
        return (opcode, InterBrokerStatusPayload.Read(ref reader).Status);
    }

    [Fact]
    public async Task ProcessAsync_UpdateMetadata_AppliesToClusterStateAndAcks()
    {
        var clusterState = new ClusterState();
        var server = NewServer(clusterState);
        var tp = new TopicPartition { Topic = "orders", Partition = 2 };

        var payload = InterBrokerFrameCodec.EncodePayload(UpdateMetadata(tp, leader: 4, leaderEpoch: 9, replicas: [4, 5, 6], isr: [4, 5]));
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, payload, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, opcode);
        Assert.Equal(ClusterRpcStatus.None, status);

        var applied = clusterState.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(4, applied!.LeaderBrokerId);
        Assert.Equal(9, applied.LeaderEpoch);
        Assert.Equal(new[] { 4, 5, 6 }, applied.Replicas);
        Assert.Equal(new[] { 4, 5 }, applied.Isr);
    }

    [Fact]
    public async Task ProcessAsync_InBandButUnwiredOpcode_RepliesErrorUnsupportedVersion()
    {
        var server = NewServer(new ClusterState());

        // StopReplica is in the native band but has no handler yet (Inc7).
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerStopReplica, ReadOnlyMemory<byte>.Empty, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.UnsupportedVersion, status);
    }

    [Fact]
    public async Task ProcessAsync_UpdateMetadataWithBogusCount_RejectsCleanlyWithoutHugeAllocation()
    {
        var server = NewServer(new ClusterState());

        // Hostile payload: int32 entry count = 0x40000000 with no entries following. The decoder must
        // reject it (bounds guard) before pre-allocating ~17 GB, degrading to a clean Error frame.
        byte[] bogus = [0x40, 0x00, 0x00, 0x00];
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, bogus, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.Unknown, status);
    }

    [Fact]
    public async Task ProcessAsync_UpdateMetadataWithoutService_RepliesNotController()
    {
        var server = new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance, service: null);

        var payload = InterBrokerFrameCodec.EncodePayload(
            UpdateMetadata(new TopicPartition { Topic = "t", Partition = 0 }, 1, 1, [1], [1]));
        var response = await server.ProcessAsync(SurgewaveOpCode.InterBrokerUpdateMetadata, payload, CancellationToken.None);

        var (opcode, status) = DecodeStatusFrame(response);
        Assert.Equal(SurgewaveOpCode.Error, opcode);
        Assert.Equal(ClusterRpcStatus.NotController, status);
    }

    [Fact]
    public async Task Loopback_UpdateMetadataOverTcp_AppliesAndAcks()
    {
        var clusterState = new ClusterState();
        var server = NewServer(clusterState);

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
            await server.HandleSingleAsync(lease.Stream, cts.Token);
        }, cts.Token);

        // Client side: write the request frame, then read the response frame.
        var tp = new TopicPartition { Topic = "orders", Partition = 7 };
        var frame = InterBrokerFrameCodec.EncodeFrame(
            SurgewaveOpCode.InterBrokerUpdateMetadata, UpdateMetadata(tp, leader: 3, leaderEpoch: 11, replicas: [3, 1], isr: [3]));

        await using var clientLease = await clientConn.AcquireStreamAsync(cts.Token);
        await clientLease.Stream.WriteAsync(frame, cts.Token);
        await clientLease.Stream.FlushAsync(cts.Token);

        var response = await InterBrokerFrameCodec.ReadFrameAsync(clientLease.Stream, cts.Token);
        await serverTask;

        Assert.NotNull(response);
        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, response!.Value.Opcode);
        var reader = new SurgewavePayloadReader(response.Value.Payload.Span);
        Assert.Equal(ClusterRpcStatus.None, InterBrokerStatusPayload.Read(ref reader).Status);

        var applied = clusterState.GetPartitionState(tp);
        Assert.NotNull(applied);
        Assert.Equal(3, applied!.LeaderBrokerId);
        Assert.Equal(11, applied.LeaderEpoch);
    }
}
