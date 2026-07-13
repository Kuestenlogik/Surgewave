using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Writes transaction markers (commit/abort) to partition logs.
/// </summary>
internal sealed class TransactionMarkerWriter
{
    private readonly LogManager _logManager;
    private readonly ILogger _logger;

    public TransactionMarkerWriter(LogManager logManager, ILogger logger)
    {
        _logManager = logManager;
        _logger = logger;
    }

    /// <summary>
    /// Writes transaction markers (commit/abort) to all partitions in the transaction.
    /// Returns the last marker offset.
    /// </summary>
    public async Task<long> WriteMarkersAsync(
        TransactionMetadata txnMetadata,
        bool commit,
        CancellationToken cancellationToken)
    {
        var controlRecordType = commit
            ? KafkaConstants.ControlRecordType.Commit
            : KafkaConstants.ControlRecordType.Abort;

        long lastOffset = 0;

        foreach (var partition in txnMetadata.Partitions)
        {
            try
            {
                var markerBatch = Kuestenlogik.Surgewave.Core.Storage.ControlBatchBuilder.BuildTransactionMarker(
                    txnMetadata.ProducerId,
                    txnMetadata.ProducerEpoch,
                    controlRecordType);

                var offset = await _logManager.AppendBatchAsync(partition, markerBatch, cancellationToken);
                lastOffset = Math.Max(lastOffset, offset);

                _logger.LogDebug(
                    "Wrote {MarkerType} marker for {Topic}-{Partition}, ProducerId={ProducerId}, Offset={Offset}",
                    commit ? "COMMIT" : "ABORT",
                    partition.Topic,
                    partition.Partition,
                    txnMetadata.ProducerId,
                    offset);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to write transaction marker for {Topic}-{Partition}",
                    partition.Topic,
                    partition.Partition);
            }
        }

        return lastOffset;
    }
}
