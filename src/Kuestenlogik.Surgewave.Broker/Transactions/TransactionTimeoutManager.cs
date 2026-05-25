using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Manages transaction timeout checking and cleanup.
/// </summary>
internal sealed class TransactionTimeoutManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, TransactionMetadata> _transactionsByTxnId;
    private readonly TransactionIndex _transactionIndex;
    private readonly TransactionMarkerWriter _markerWriter;
    private readonly ILogger _logger;
    private readonly Timer _timeoutCheckTimer;
    private readonly CancellationTokenSource _shutdownCts = new();
    private const int TimeoutCheckIntervalMs = 5000; // Check every 5 seconds

    public TransactionTimeoutManager(
        ConcurrentDictionary<string, TransactionMetadata> transactionsByTxnId,
        TransactionIndex transactionIndex,
        TransactionMarkerWriter markerWriter,
        ILogger logger)
    {
        _transactionsByTxnId = transactionsByTxnId;
        _transactionIndex = transactionIndex;
        _markerWriter = markerWriter;
        _logger = logger;

        // Start background timer for transaction timeout cleanup
        _timeoutCheckTimer = new Timer(
            callback: _ => _ = CheckTransactionTimeoutsAsync(),
            state: null,
            dueTime: TimeSpan.FromMilliseconds(TimeoutCheckIntervalMs),
            period: TimeSpan.FromMilliseconds(TimeoutCheckIntervalMs));
    }

    /// <summary>
    /// Checks for timed-out transactions and aborts them.
    /// </summary>
    private async Task CheckTransactionTimeoutsAsync()
    {
        try
        {
            var timedOutTransactions = _transactionsByTxnId.Values
                .Where(t => t.IsTimedOut)
                .ToList();

            foreach (var txn in timedOutTransactions)
            {
                try
                {
                    _logger.LogWarning(
                        "Transaction {TransactionalId} timed out after {TimeoutMs}ms, aborting",
                        txn.TransactionalId,
                        txn.TransactionTimeoutMs);

                    // Write abort markers
                    if (txn.Partitions.Count > 0)
                    {
                        var abortOffset = await _markerWriter.WriteMarkersAsync(txn, commit: false, _shutdownCts.Token);

                        _transactionIndex.AbortTransaction(
                            txn.ProducerId,
                            txn.Partitions,
                            abortOffset);
                    }

                    // Mark as dead (timed out)
                    txn.State = TransactionState.Dead;
                    txn.Partitions.Clear();
                    txn.ConsumerGroups.Clear();
                    txn.PendingOffsets.Clear();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to abort timed-out transaction {TransactionalId}",
                        txn.TransactionalId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking transaction timeouts");
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdownCts.CancelAsync();
        await _timeoutCheckTimer.DisposeAsync();
        _shutdownCts.Dispose();
    }
}
