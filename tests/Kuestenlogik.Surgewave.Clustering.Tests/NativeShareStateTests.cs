using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

/// <summary>
/// A partition leader asks another broker's share coordinator about a share partition's state over the inter-broker
/// port (#228): the request reaches the coordinator's store as sent, and its answer — status, state epoch, the state
/// with its batches — comes back whole.
/// </summary>
public sealed class NativeShareStateTests
{
    private static readonly ShareStateKey Key = new("share-g", Guid.NewGuid(), 3);

    [Fact]
    public async Task AWrittenStateIsReadBackWhole()
    {
        var store = new RecordingPersister();
        var server = Server(store);
        var state = new ShareStateSnapshot(4, 9, 100,
            [new ShareStateBatch(100, 104, ShareRecordState.Acknowledged, 1), new ShareStateBatch(105, 105, ShareRecordState.Archived, 5)]);

        var written = await SendAsync(server, new ShareStatePayload(ShareStateOperation.Write, Key, -1, state));
        var read = await SendAsync(server, new ShareStatePayload(ShareStateOperation.Read, Key, -1, null));

        Assert.Equal(ShareStateStatus.Ok, written.Status);
        Assert.Equal(Key, store.LastKey);
        Assert.Equal(ShareStateStatus.Ok, read.Status);
        Assert.Equal(state.StateEpoch, read.State!.StateEpoch);
        Assert.Equal((state.LeaderEpoch, state.StartOffset), (read.State.LeaderEpoch, read.State.StartOffset));
        Assert.Equal(state.Batches, read.State.Batches);
    }

    [Fact]
    public async Task AnInitializationAnswersTheNewStateEpoch()
    {
        var store = new RecordingPersister();

        var initialized = await SendAsync(Server(store), new ShareStatePayload(ShareStateOperation.Initialize, Key, 42, null));

        Assert.Equal((ShareStateStatus.Ok, 1), (initialized.Status, initialized.StateEpoch));
        Assert.Equal(42, store.State!.StartOffset);
    }

    [Fact]
    public async Task ABrokerWithoutAShareCoordinatorIsNotTheCoordinator()
    {
        var answer = await SendAsync(Server(null), new ShareStatePayload(ShareStateOperation.Read, Key, -1, null));

        Assert.Equal(ShareStateStatus.NotCoordinator, answer.Status);
    }

    private static NativeInterBrokerServer Server(IShareStatePersister? shareState)
    {
        var state = new ClusterState();
        var config = new ClusteringConfig { BrokerId = 0, Host = "localhost", Port = 9092, RebalanceCheckIntervalSeconds = 5 };
        var logs = new LogManager(Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}"), new MemoryLogSegmentFactory());
        var replicas = new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, logs, config, new TcpPeerTransport());
        var service = new ClusterStateInterBrokerService(
            NullLogger<ClusterStateInterBrokerService>.Instance, state, replicas, logs, 0,
            new NativeInterBrokerServerTests.StubIsrApplier(), shareState: shareState);
        return new NativeInterBrokerServer(NullLogger<NativeInterBrokerServer>.Instance, service);
    }

    private static async Task<ShareStateResponsePayload> SendAsync(NativeInterBrokerServer server, ShareStatePayload request)
    {
        var frame = await server.ProcessAsync(SurgewaveOpCode.InterBrokerShareState, InterBrokerFrameCodec.EncodePayload(request), CancellationToken.None);
        // Skip the [int32 size] prefix, then read [uint16 opcode] and the answer.
        var reader = new SurgewavePayloadReader(frame.AsSpan(4));
        Assert.Equal(SurgewaveOpCode.InterBrokerShareState, (SurgewaveOpCode)reader.ReadUInt16());
        return ShareStateResponsePayload.Read(ref reader);
    }

    // One key's state, as a share coordinator would keep it.
    private sealed class RecordingPersister : IShareStatePersister
    {
        public ShareStateKey? LastKey { get; private set; }
        public ShareStateSnapshot? State { get; private set; }

        public Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken) =>
            Task.FromResult(State is null ? (ShareStateStatus.NotInitialized, (ShareStateSnapshot?)null) : (ShareStateStatus.Ok, State));

        public Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken)
        {
            (LastKey, State) = (key, state);
            return Task.FromResult(ShareStateStatus.Ok);
        }

        public Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken)
        {
            (LastKey, State) = (key, new ShareStateSnapshot((State?.StateEpoch ?? 0) + 1, -1, startOffset, []));
            return Task.FromResult((ShareStateStatus.Ok, State.StateEpoch));
        }

        public Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken)
        {
            (LastKey, State) = (key, null);
            return Task.FromResult(ShareStateStatus.Ok);
        }
    }
}
