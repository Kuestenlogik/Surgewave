using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Handles loading and persisting transaction state to durable storage.
/// </summary>
internal sealed class TransactionStatePersistence
{
    private readonly TransactionStateStore _stateStore;
    private readonly ProducerStateManager _producerStateManager;
    private readonly ILogger _logger;

    public TransactionStatePersistence(
        TransactionStateStore stateStore,
        ProducerStateManager producerStateManager,
        ILogger logger)
    {
        _stateStore = stateStore;
        _producerStateManager = producerStateManager;
        _logger = logger;
    }

    /// <summary>
    /// Loads persisted transaction state and recovers in-progress transactions.
    /// </summary>
    public void LoadPersistedState(ConcurrentDictionary<string, TransactionMetadata> transactionsByTxnId)
    {
        var persistedStates = _stateStore.GetAllTransactionStates();

        foreach (var persisted in persistedStates)
        {
            // Parse the state
            if (!Enum.TryParse<TransactionState>(persisted.State, out var state))
            {
                _logger.LogWarning(
                    "Unknown transaction state {State} for {TransactionalId}, skipping",
                    persisted.State,
                    persisted.TransactionalId);
                continue;
            }

            // Create transaction metadata
            var txnMetadata = new TransactionMetadata
            {
                TransactionalId = persisted.TransactionalId,
                ProducerId = persisted.ProducerId,
                ProducerEpoch = persisted.ProducerEpoch,
                State = state,
                TransactionTimeoutMs = persisted.TransactionTimeoutMs
            };

            // Restore partitions
            foreach (var p in persisted.Partitions)
            {
                txnMetadata.Partitions.Add(new TopicPartition { Topic = p.Topic, Partition = p.Partition });
            }

            // Restore consumer groups
            foreach (var g in persisted.ConsumerGroups)
            {
                txnMetadata.ConsumerGroups.Add(g);
            }

            // Restore pending offsets
            foreach (var o in persisted.PendingOffsets)
            {
                txnMetadata.PendingOffsets.Add(new PendingTxnOffset
                {
                    GroupId = o.GroupId,
                    Topic = o.Topic,
                    Partition = o.Partition,
                    Offset = o.Offset,
                    Metadata = o.Metadata
                });
            }

            // Register with ProducerStateManager
            _producerStateManager.RegisterProducerId(txnMetadata.ProducerId, txnMetadata.ProducerEpoch);

            // For in-progress transactions, we need to abort them since they may be incomplete
            if (state == TransactionState.Ongoing ||
                state == TransactionState.PrepareCommit ||
                state == TransactionState.PrepareAbort)
            {
                _logger.LogWarning(
                    "Found in-progress transaction {TransactionalId} during recovery, will abort. State={State}, Partitions={PartitionCount}",
                    txnMetadata.TransactionalId,
                    state,
                    txnMetadata.Partitions.Count);

                // Mark for abort during next timeout check
                txnMetadata.State = TransactionState.PrepareAbort;
                txnMetadata.LastActivityTime = DateTimeOffset.MinValue; // Force timeout
            }

            transactionsByTxnId[persisted.TransactionalId] = txnMetadata;

            _logger.LogDebug(
                "Restored transaction state for {TransactionalId}: ProducerId={ProducerId}, Epoch={Epoch}, State={State}",
                txnMetadata.TransactionalId,
                txnMetadata.ProducerId,
                txnMetadata.ProducerEpoch,
                txnMetadata.State);
        }

        if (persistedStates.Count > 0)
        {
            _logger.LogInformation("Loaded {Count} persisted transaction states", persistedStates.Count);
        }
    }

    /// <summary>
    /// Persists transaction state to durable storage.
    /// </summary>
    public void PersistState(TransactionMetadata txnMetadata)
    {
        _stateStore.SaveTransactionState(
            txnMetadata.TransactionalId,
            txnMetadata.ProducerId,
            txnMetadata.ProducerEpoch,
            txnMetadata.State,
            txnMetadata.TransactionTimeoutMs,
            txnMetadata.Partitions,
            txnMetadata.ConsumerGroups,
            txnMetadata.PendingOffsets.Select(p => new PendingOffsetEntry
            {
                GroupId = p.GroupId,
                Topic = p.Topic,
                Partition = p.Partition,
                Offset = p.Offset,
                Metadata = p.Metadata
            }));
    }
}
