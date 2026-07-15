using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc7 — the native SRWV transaction-marker replicator: the protocol-neutral
/// <see cref="ITransactionMarkerReplicator"/> counterpart of the Kafka-wire
/// <c>TransactionMarkerReplicator</c>. It sends a commit/abort marker to the LEADER of each involved
/// partition (excluding self, whose own-led markers the coordinator wrote locally) via a
/// <see cref="SurgewaveOpCode.InterBrokerWriteTxnMarkers"/> frame to the leader's <b>ReplicationPort</b>,
/// where the <see cref="NativeInterBrokerServer"/> appends it. Followers receive the marker through
/// normal fetch replication of the leader's log. Lives in <c>Clustering</c> with no Protocol.Kafka edge.
/// <para>
/// This is NOT identical to the Kafka-wire replicator's send-to-all-replicas fan-out: that one blasts
/// the marker at every replica and relies on each non-leader rejecting with NotLeaderForPartition, so
/// it reports a self-led-with-follower partition as a failure and retries transient errors. The native
/// path sends only to the current leader (the correct target) and is single-shot — retry, if wanted,
/// is the coordinator's job. Known gap (parity with the dormant coordinator's best-effort contract): a
/// partition with no elected leader (LeaderBrokerId &lt; 0) is skipped rather than retried until a
/// leader appears.
/// </para>
/// </summary>
public sealed partial class NativeTransactionMarkerReplicator : ITransactionMarkerReplicator
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly ConnectionPool _connectionPool;
    private readonly ClusterState _clusterState;
    private readonly int _localBrokerId;
    private readonly ILogger<NativeTransactionMarkerReplicator> _logger;

    public NativeTransactionMarkerReplicator(
        ConnectionPool connectionPool,
        ClusterState clusterState,
        int localBrokerId,
        ILogger<NativeTransactionMarkerReplicator> logger)
    {
        _connectionPool = connectionPool;
        _clusterState = clusterState;
        _localBrokerId = localBrokerId;
        _logger = logger;
    }

    public async Task<MarkerReplicationResult> ReplicateMarkersAsync(
        string transactionalId,
        long producerId,
        short producerEpoch,
        IReadOnlyList<TopicPartition> partitions,
        bool commit,
        int coordinatorEpoch,
        CancellationToken cancellationToken)
    {
        var result = new MarkerReplicationResult();
        if (partitions.Count == 0)
        {
            result.IsSuccess = true;
            return result;
        }

        // Classify EVERY involved partition so a no-leader / unknown skip is visible in the result
        // (#72 Inc6), and group the remote-led ones by leader for sending. The leader appends the
        // marker to its log; followers receive it via normal fetch replication, so sending only to
        // leaders avoids both wasted sends and the double-write a direct follower append would cause.
        // (See the class summary for how this deliberately differs from the Kafka send-to-all fan-out.)
        var byLeader = new Dictionary<int, List<TopicPartition>>();
        foreach (var tp in partitions)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state is null)
            {
                result.PartitionOutcomes[tp] = MarkerPartitionOutcome.SkippedUnknownPartition;
                continue;
            }
            var leader = state.LeaderBrokerId;
            if (leader < 0)
            {
                result.PartitionOutcomes[tp] = MarkerPartitionOutcome.SkippedNoLeader;
                continue;
            }
            if (leader == _localBrokerId) // the coordinator wrote its own-led markers locally already
            {
                result.PartitionOutcomes[tp] = MarkerPartitionOutcome.LocalLeader;
                continue;
            }
            if (!byLeader.TryGetValue(leader, out var list))
            {
                list = [];
                byLeader[leader] = list;
            }
            list.Add(tp);
        }

        // Log-only min.insync.replicas assessment, shared verbatim with the Kafka-wire replicator.
        AssessMinIsr(transactionalId, partitions);

        if (byLeader.Count == 0)
        {
            // Nothing to send remotely (all local-led or skipped). Preserve the prior success contract;
            // the per-partition outcomes above make any skip visible to the coordinator (Inc7).
            result.IsSuccess = true;
            return result;
        }

        var tasks = byLeader.Select(kvp => SendMarkersAsync(
            kvp.Key, transactionalId, producerId, producerEpoch, kvp.Value, commit, coordinatorEpoch, cancellationToken));
        var outcomes = await Task.WhenAll(tasks).ConfigureAwait(false);

        foreach (var (brokerId, ok, error) in outcomes)
        {
            var partitionOutcome = ok ? MarkerPartitionOutcome.Replicated : MarkerPartitionOutcome.Failed;
            foreach (var tp in byLeader[brokerId])
                result.PartitionOutcomes[tp] = partitionOutcome;

            if (ok)
                result.SuccessfulBrokers.Add(brokerId);
            else
                result.FailedBrokers[brokerId] = error ?? "unknown error";
        }

        // Match the Kafka-wire replicator's requirement: succeed once at least one follower acked (or
        // there were no failures). A full min.insync.replicas GATE is a later, sign-off-gated increment.
        result.IsSuccess = result.SuccessfulBrokers.Count > 0 || result.FailedBrokers.Count == 0;
        if (result.IsSuccess)
            LogReplicated(transactionalId, commit ? "COMMIT" : "ABORT", result.SuccessfulBrokers.Count);
        else
            LogReplicationFailed(transactionalId, commit ? "COMMIT" : "ABORT", result.FailedBrokers.Count);
        return result;
    }

    // #72 Inc6 — log-only: report the involved partitions that are under min.insync.replicas, using the
    // one shared assessment so the native and Kafka-wire transports log an identical view.
    private void AssessMinIsr(string transactionalId, IReadOnlyList<TopicPartition> partitions)
    {
        var triples = new List<(TopicPartition, int, int)>(partitions.Count);
        foreach (var tp in partitions)
        {
            var state = _clusterState.GetPartitionState(tp);
            if (state is not null)
                triples.Add((tp, state.Isr.Count, state.MinInSyncReplicas));
        }

        var under = MarkerReplicationAssessment.UnderMinIsr(triples);
        if (under.Count > 0)
            LogUnderMinIsr(transactionalId, under.Count, string.Join(", ", under.Select(tp => $"{tp.Topic}-{tp.Partition}")));
    }

    private async Task<(int BrokerId, bool Ok, string? Error)> SendMarkersAsync(
        int brokerId, string transactionalId, long producerId, short producerEpoch,
        List<TopicPartition> partitions, bool commit, int coordinatorEpoch, CancellationToken ct)
    {
        var broker = _clusterState.GetBroker(brokerId);
        if (broker is null)
        {
            LogBrokerNotFound(brokerId);
            return (brokerId, false, "broker not found");
        }

        var payload = new WriteTxnMarkersRequestPayload(
            transactionalId, producerId, producerEpoch, partitions, commit, coordinatorEpoch);

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(RequestTimeout);
            var token = timeoutCts.Token;

            var frame = InterBrokerFrameCodec.EncodeFrame(SurgewaveOpCode.InterBrokerWriteTxnMarkers, payload);
            var connection = await _connectionPool.GetConnectionAsync(broker.Host, broker.ReplicationPort, token).ConfigureAwait(false);

            var exchangeComplete = false;
            try
            {
                await connection.Stream.WriteAsync(frame, token).ConfigureAwait(false);
                await connection.Stream.FlushAsync(token).ConfigureAwait(false);

                var response = await InterBrokerFrameCodec.ReadFrameAsync(connection.Stream, token).ConfigureAwait(false)
                    ?? throw new EndOfStreamException("Connection closed while reading WriteTxnMarkers response");

                if (response.Opcode != SurgewaveOpCode.InterBrokerWriteTxnMarkers && response.Opcode != SurgewaveOpCode.Error)
                {
                    LogResponseMismatch(response.Opcode, brokerId);
                    return (brokerId, false, "opcode mismatch");
                }

                exchangeComplete = true;
                var reader = new SurgewavePayloadReader(response.Payload.Span);
                var status = WriteTxnMarkersResponsePayload.Read(ref reader).Status;
                if (status != ClusterRpcStatus.None)
                {
                    LogMarkerRejected(brokerId, status);
                    return (brokerId, false, status.ToString());
                }
                return (brokerId, true, null);
            }
            finally
            {
                if (exchangeComplete)
                    connection.Return();
                else
                    connection.Discard();
            }
        }
        catch (Exception ex)
        {
            LogSendFailed(brokerId, ex);
            return (brokerId, false, ex.Message);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} not found in cluster state")]
    private partial void LogBrokerNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction {TransactionalId}: {MarkerType} markers replicated natively to {BrokerCount} brokers")]
    private partial void LogReplicated(string transactionalId, string markerType, int brokerCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transaction {TransactionalId}: {Count} involved partition(s) below min.insync.replicas when writing markers: {Partitions}")]
    private partial void LogUnderMinIsr(string transactionalId, int count, string partitions);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transaction {TransactionalId}: failed to replicate {MarkerType} markers natively to {FailedCount} brokers")]
    private partial void LogReplicationFailed(string transactionalId, string markerType, int failedCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native WriteTxnMarkers rejected by broker {BrokerId}: {Status}")]
    private partial void LogMarkerRejected(int brokerId, ClusterRpcStatus status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send native WriteTxnMarkers to broker {BrokerId}")]
    private partial void LogSendFailed(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native WriteTxnMarkers answered with mismatched opcode {ResponseOpcode} by broker {BrokerId} — discarding poisoned connection")]
    private partial void LogResponseMismatch(SurgewaveOpCode responseOpcode, int brokerId);
}
