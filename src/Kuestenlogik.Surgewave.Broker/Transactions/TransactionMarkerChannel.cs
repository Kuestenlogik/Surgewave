using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Writes the commit or abort markers of a transaction to its partitions, each on the partition's leader (#197): this
/// broker appends to the partitions it leads, the replicator hands the rest to their leaders. A marker counts once
/// the partition's in-sync replicas have it, since the transaction completes once every marker is written.
/// </summary>
/// <remarks>
/// Without a replicator — a broker on its own — every partition is this broker's. The coordinator used to write every
/// marker into its own log, whether it led the partition or not, and send them on besides; a partition led elsewhere
/// then had a marker in a follower's log that its leader never wrote.
/// </remarks>
internal sealed class TransactionMarkerChannel
{
    private static readonly TimeSpan CommitTimeout = TimeSpan.FromSeconds(5);

    private readonly LogManager _logManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly ILogger _logger;

    public TransactionMarkerChannel(LogManager logManager, TransactionIndex transactionIndex, ILogger logger)
    {
        _logManager = logManager;
        _transactionIndex = transactionIndex;
        _logger = logger;
    }

    /// <summary>Hands markers for partitions other brokers lead to those leaders; null on a broker on its own.</summary>
    public ITransactionMarkerReplicator? Replicator { get; set; }

    /// <summary>Says when a marker this broker appended is on the in-sync replicas; null when nobody replicates.</summary>
    public IPartitionCommitGate? CommitGate { get; set; }

    /// <summary>
    /// Writes the markers to <paramref name="partitions"/> and returns those that have it now; a partition missing
    /// from the answer is to be tried again.
    /// </summary>
    public async Task<IReadOnlyCollection<TopicPartition>> WriteAsync(
        TransactionMetadata txn, IReadOnlyCollection<TopicPartition> partitions, bool commit, int coordinatorEpoch,
        CancellationToken cancellationToken)
    {
        var written = new HashSet<TopicPartition>();
        List<TopicPartition> local;
        if (Replicator is null)
        {
            local = [.. partitions];
        }
        else
        {
            MarkerReplicationResult result;
            try
            {
                result = await Replicator.ReplicateMarkersAsync(
                    txn.TransactionalId, txn.ProducerId, txn.ProducerEpoch, [.. partitions], commit, coordinatorEpoch,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Transaction {TransactionalId}: the markers could not be handed to the partitions' leaders", txn.TransactionalId);
                result = new MarkerReplicationResult();
            }

            local = [];
            foreach (var partition in partitions)
            {
                switch (result.PartitionOutcomes.GetValueOrDefault(partition, MarkerPartitionOutcome.Failed))
                {
                    // A partition the cluster state does not know is this broker's: one on its own has no partition
                    // states at all, and in a cluster the append fails for a partition this broker does not host.
                    case MarkerPartitionOutcome.LocalLeader or MarkerPartitionOutcome.SkippedUnknownPartition:
                        local.Add(partition);
                        break;
                    case MarkerPartitionOutcome.Replicated:
                        written.Add(partition);
                        break;
                }
            }
        }

        foreach (var partition in local)
        {
            if (await WriteLocalAsync(txn, partition, commit, cancellationToken).ConfigureAwait(false))
                written.Add(partition);
        }
        return written;
    }

    private async Task<bool> WriteLocalAsync(TransactionMetadata txn, TopicPartition partition, bool commit, CancellationToken cancellationToken)
    {
        var controlRecordType = commit ? KafkaConstants.ControlRecordType.Commit : KafkaConstants.ControlRecordType.Abort;
        long offset;
        try
        {
            var marker = ControlBatchBuilder.BuildTransactionMarker(txn.ProducerId, txn.ProducerEpoch, controlRecordType);
            offset = await _logManager.AppendBatchAsync(partition, marker, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to write the transaction marker for {Topic}-{Partition}", partition.Topic, partition.Partition);
            return false;
        }

        if (CommitGate is not null
            && !await CommitGate.WaitForDurableCommitAsync(partition, offset + 1, CommitTimeout, cancellationToken).ConfigureAwait(false))
        {
            _logger.LogWarning("The transaction marker for {Topic}-{Partition} was not replicated in time", partition.Topic, partition.Partition);
            return false;
        }

        if (commit)
            _transactionIndex.CommitTransaction(txn.ProducerId, [partition], offset);
        else
            _transactionIndex.AbortTransaction(txn.ProducerId, [partition], offset);

        _logger.LogDebug(
            "Wrote {MarkerType} marker for {Topic}-{Partition}, ProducerId={ProducerId}, Offset={Offset}",
            commit ? "COMMIT" : "ABORT", partition.Topic, partition.Partition, txn.ProducerId, offset);
        return true;
    }
}
