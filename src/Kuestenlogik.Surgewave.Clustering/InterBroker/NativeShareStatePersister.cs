using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// Asks a share partition's share coordinator — the leader of the key's partition of <c>__share_group_state</c> — on
/// another broker about the share partition's state, over the inter-broker channel (#228): Kafka's
/// Read/Write/Initialize/DeleteShareGroupState between brokers.
/// </summary>
public sealed partial class NativeShareStatePersister : IShareStatePersister
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusterCoordinatorLocator _locator;
    private readonly int _localBrokerId;
    private readonly ILogger<NativeShareStatePersister> _logger;

    public NativeShareStatePersister(ConnectionPool connectionPool, ClusterState clusterState, int localBrokerId, ILogger<NativeShareStatePersister> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _locator = new ClusterCoordinatorLocator(clusterState);
        _localBrokerId = localBrokerId;
        _logger = logger;
    }

    public async Task<(ShareStateStatus Status, ShareStateSnapshot? State)> ReadAsync(ShareStateKey key, CancellationToken cancellationToken)
    {
        var response = await SendAsync(new ShareStatePayload(ShareStateOperation.Read, key, -1, null), cancellationToken).ConfigureAwait(false);
        return (response.Status, response.State);
    }

    public async Task<ShareStateStatus> WriteAsync(ShareStateKey key, ShareStateSnapshot state, CancellationToken cancellationToken) =>
        (await SendAsync(new ShareStatePayload(ShareStateOperation.Write, key, -1, state), cancellationToken).ConfigureAwait(false)).Status;

    public async Task<(ShareStateStatus Status, int StateEpoch)> InitializeAsync(ShareStateKey key, long startOffset, CancellationToken cancellationToken)
    {
        var response = await SendAsync(new ShareStatePayload(ShareStateOperation.Initialize, key, startOffset, null), cancellationToken).ConfigureAwait(false);
        return (response.Status, response.StateEpoch);
    }

    public async Task<ShareStateStatus> DeleteAsync(ShareStateKey key, CancellationToken cancellationToken) =>
        (await SendAsync(new ShareStatePayload(ShareStateOperation.Delete, key, -1, null), cancellationToken).ConfigureAwait(false)).Status;

    private async Task<ShareStateResponsePayload> SendAsync(ShareStatePayload request, CancellationToken cancellationToken)
    {
        var coordinator = _locator.Locate(CoordinatorKind.Share, request.Key.CoordinatorKey);
        if (coordinator.Status != CoordinatorLookupStatus.Ok || _clusterState.GetBroker(coordinator.BrokerId) is not { } broker)
            return Unavailable;
        if (coordinator.BrokerId == _localBrokerId)
            return new ShareStateResponsePayload(ShareStateStatus.NotCoordinator, -1, null); // named here, not served here yet

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerShareState, request);
            var connection = await _connectionPool.GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);
            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);
                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading a share-state answer");
                exchangeComplete = true;

                if (response.Opcode != SurgewaveOpCode.InterBrokerShareState)
                {
                    LogRefused(request.Key.GroupId, coordinator.BrokerId, response.Opcode);
                    return Unavailable;
                }
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                return ShareStateResponsePayload.Read(ref reader);
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogSendFailed(request.Key.GroupId, coordinator.BrokerId, ex);
            return Unavailable;
        }
    }

    private static ShareStateResponsePayload Unavailable => new(ShareStateStatus.CoordinatorNotAvailable, -1, null);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Share coordinator {BrokerId} answered a state request of group {GroupId} with {Opcode}")]
    private partial void LogRefused(string groupId, int brokerId, SurgewaveOpCode opcode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not ask share coordinator {BrokerId} about a share partition of group {GroupId}")]
    private partial void LogSendFailed(string groupId, int brokerId, Exception ex);
}
