using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// #60 Inc4 — coverage for the native SRWV inter-broker frame codec
/// (<c>[int32 size][uint16 opcode][payload]</c>) and the native-band opcode split that lets the
/// receiver share the ReplicationPort with Family-B replication traffic.
/// </summary>
public class InterBrokerFrameCodecTests
{
    private static PartitionStatesPayload SamplePartitionStates()
    {
        var tp = new TopicPartition { Topic = "orders", Partition = 0 };
        return new PartitionStatesPayload(ControllerId: 1, ControllerEpoch: 3, LiveBrokers: [],
        [
            (tp, new PartitionState { TopicPartition = tp, LeaderBrokerId = 1, LeaderEpoch = 5, Replicas = [1, 2, 3], Isr = [1, 2] }),
        ]);
    }

    [Fact]
    public async Task EncodeFrame_ReadFrameAsync_RoundTripsOpcodeAndPayload()
    {
        var payload = SamplePartitionStates();
        var frame = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerUpdateMetadata, payload);

        using var stream = new MemoryStream(frame);
        var read = await InterBrokerFrameCodec.ReadFrameAsync(stream, CancellationToken.None);

        Assert.NotNull(read);
        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, read!.Value.Opcode);

        var reader = new SurgewavePayloadReader(read.Value.Payload.Span);
        var decoded = PartitionStatesPayload.Read(ref reader);
        Assert.Single(decoded.Entries);
        Assert.Equal("orders", decoded.Entries[0].Tp.Topic);
        Assert.Equal(1, decoded.Entries[0].State.LeaderBrokerId);
        Assert.Equal([1, 2], decoded.Entries[0].State.Isr);
    }

    [Fact]
    public async Task ReadFrameAsync_EmptyStream_ReturnsNull()
    {
        using var stream = new MemoryStream([]);
        Assert.Null(await InterBrokerFrameCodec.ReadFrameAsync(stream, CancellationToken.None));
    }

    [Fact]
    public async Task ReadFrameAsync_TwoFramesBackToBack_ReadsBothThenEof()
    {
        var f1 = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerUpdateMetadata, new InterBrokerStatusPayload(ClusterRpcStatus.None));
        var f2 = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.Error, new InterBrokerStatusPayload(ClusterRpcStatus.NotController));
        using var stream = new MemoryStream([.. f1, .. f2]);

        var r1 = await InterBrokerFrameCodec.ReadFrameAsync(stream, CancellationToken.None);
        var r2 = await InterBrokerFrameCodec.ReadFrameAsync(stream, CancellationToken.None);
        var r3 = await InterBrokerFrameCodec.ReadFrameAsync(stream, CancellationToken.None);

        Assert.Equal(SurgewaveOpCode.InterBrokerUpdateMetadata, r1!.Value.Opcode);
        Assert.Equal(SurgewaveOpCode.Error, r2!.Value.Opcode);
        Assert.Null(r3);
    }

    [Fact]
    public void IsNativeOpcode_SeparatesFamilyBFromNativeBand()
    {
        // Family-B replication api keys sit below the native band.
        Assert.False(NativeInterBrokerServer.IsNativeOpcode(1));    // Fetch
        Assert.False(NativeInterBrokerServer.IsNativeOpcode(100));  // Heartbeat
        Assert.False(NativeInterBrokerServer.IsNativeOpcode(104));  // Raft PreVote

        // Native inter-broker/Raft SRWV opcodes are in-band.
        Assert.True(NativeInterBrokerServer.IsNativeOpcode((ushort)SurgewaveOpCode.InterBrokerUpdateMetadata));
        Assert.True(NativeInterBrokerServer.IsNativeOpcode((ushort)SurgewaveOpCode.InterBrokerRegistration));
        Assert.True(NativeInterBrokerServer.IsNativeOpcode((ushort)SurgewaveOpCode.RaftAppendEntries));
    }
}
