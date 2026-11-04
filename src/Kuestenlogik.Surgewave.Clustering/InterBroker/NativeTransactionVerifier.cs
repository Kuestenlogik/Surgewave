using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// Asks a transactional id's coordinator — the leader of its partition of <c>__transaction_state</c> — about a
/// partition of the transaction, over the inter-broker channel (#197). The partition leader that appends a
/// transactional batch is mostly not the coordinator: Kafka's leaders send AddPartitionsToTxn v4+ for this.
/// </summary>
public sealed partial class NativeTransactionVerifier : ITransactionPartitionVerifier
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly ClusterCoordinatorLocator _locator;
    private readonly int _localBrokerId;
    private readonly ILogger<NativeTransactionVerifier> _logger;

    public NativeTransactionVerifier(
        ConnectionPool connectionPool, ClusterState clusterState, int localBrokerId, ILogger<NativeTransactionVerifier> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _locator = new ClusterCoordinatorLocator(clusterState);
        _localBrokerId = localBrokerId;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<TxnErrorStatus> VerifyPartitionAsync(
        string transactionalId, long producerId, short producerEpoch, TopicPartition partition, TransactionVersion version,
        CancellationToken cancellationToken)
    {
        var coordinator = _locator.Locate(CoordinatorKind.Transaction, transactionalId);
        if (coordinator.Status != CoordinatorLookupStatus.Ok)
            return TxnErrorStatus.CoordinatorNotAvailable;
        if (coordinator.BrokerId == _localBrokerId)
            return TxnErrorStatus.NotCoordinator; // the cluster names this broker, whose coordinator did not answer for it yet

        var broker = _clusterState.GetBroker(coordinator.BrokerId);
        if (broker is null)
            return TxnErrorStatus.CoordinatorNotAvailable;

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(
                SurgewaveOpCode.InterBrokerVerifyTxnPartition,
                new VerifyTxnPartitionPayload(transactionalId, producerId, producerEpoch, partition, version));
            var connection = await _connectionPool.GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading a transaction verification");
                exchangeComplete = true;

                if (response.Opcode != SurgewaveOpCode.InterBrokerVerifyTxnPartition)
                {
                    LogRefused(transactionalId, coordinator.BrokerId, response.Opcode);
                    return TxnErrorStatus.CoordinatorNotAvailable;
                }
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                return VerifyTxnPartitionResponsePayload.Read(ref reader).Status;
            }
            finally
            {
                if (exchangeComplete) connection.Return(); else connection.Discard();
            }
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogSendFailed(transactionalId, coordinator.BrokerId, ex);
            return TxnErrorStatus.CoordinatorNotAvailable;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Coordinator {BrokerId} answered the verification of {TransactionalId} with {Opcode}")]
    private partial void LogRefused(string transactionalId, int brokerId, SurgewaveOpCode opcode);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not ask coordinator {BrokerId} to verify {TransactionalId}")]
    private partial void LogSendFailed(string transactionalId, int brokerId, Exception ex);
}
