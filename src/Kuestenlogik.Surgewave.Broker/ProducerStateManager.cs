using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Manages producer state for idempotent and transactional producers.
/// Tracks producer IDs, epochs, and sequence numbers to ensure exactly-once delivery.
/// </summary>
public sealed class ProducerStateManager
{
    private readonly ConcurrentDictionary<long, ProducerState> _producers = new();
    private long _nextProducerId = 1;
    private readonly Lock _idLock = new();

    /// <summary>
    /// Snapshot of one active producer's state for a single partition. Surface
    /// type for the KIP-664 <c>DescribeProducers</c> wire RPC; the values map
    /// 1:1 to the response fields the admin client expects.
    /// </summary>
    public sealed record ActiveProducerInfo(
        long ProducerId,
        short Epoch,
        int LastSequence,
        bool HasOngoingTransaction);

    /// <summary>
    /// Returns every producer that has written to <paramref name="partition"/>
    /// or registered the partition as part of an in-flight transaction. Used
    /// by the <c>DescribeProducers</c> admin RPC (KIP-664). Producers that
    /// never touched the partition are omitted — the broker would otherwise
    /// have to ship its entire producer table for every probe.
    /// </summary>
    public IReadOnlyList<ActiveProducerInfo> GetActiveProducersForPartition(TopicPartition partition)
    {
        var result = new List<ActiveProducerInfo>();
        foreach (var ps in _producers.Values)
        {
            var lastSeq = ps.GetLastSequence(partition);
            var hasTxn = ps.TransactionPartitions.Contains(partition);
            if (lastSeq == KafkaConstants.Producer.NoSequence && !hasTxn) continue;

            result.Add(new ActiveProducerInfo(
                ps.ProducerId,
                ps.Epoch,
                lastSeq,
                hasTxn));
        }
        return result;
    }

    /// <summary>
    /// Allocates a new producer ID with epoch 0.
    /// </summary>
    public (long ProducerId, short Epoch) AllocateProducerId()
    {
        lock (_idLock)
        {
            var producerId = _nextProducerId++;
            var state = new ProducerState(producerId, 0);
            _producers[producerId] = state;
            return (producerId, 0);
        }
    }

    /// <summary>
    /// Registers an existing producer ID and epoch (used during recovery from persistent state).
    /// Ensures _nextProducerId is always greater than any registered producer ID.
    /// </summary>
    public void RegisterProducerId(long producerId, short epoch)
    {
        lock (_idLock)
        {
            // Ensure next allocated ID is always higher than any registered ID
            if (producerId >= _nextProducerId)
            {
                _nextProducerId = producerId + 1;
            }

            var state = new ProducerState(producerId, epoch);
            _producers[producerId] = state;
        }
    }

    /// <summary>
    /// Gets or bumps the epoch for an existing producer (for transactional producers).
    /// </summary>
    public (long ProducerId, short Epoch, ErrorCode Error) GetOrBumpEpoch(
        long producerId,
        short currentEpoch,
        string? transactionalId)
    {
        if (producerId == KafkaConstants.Producer.NoProducerId)
        {
            // New producer - allocate ID
            var (newId, epoch) = AllocateProducerId();
            if (transactionalId != null)
            {
                _producers[newId].TransactionalId = transactionalId;
            }
            return (newId, epoch, ErrorCode.None);
        }

        if (!_producers.TryGetValue(producerId, out var state))
        {
            // Unknown producer - allocate new
            var (newId, epoch) = AllocateProducerId();
            return (newId, epoch, ErrorCode.None);
        }

        // Validate epoch
        if (currentEpoch != state.Epoch)
        {
            if (currentEpoch < state.Epoch)
            {
                // Fenced - old producer
                return (producerId, state.Epoch, ErrorCode.InvalidProducerEpoch);
            }
            // Future epoch - bump to match
            state.Epoch = currentEpoch;
        }

        // Bump epoch for new transaction
        if (transactionalId != null && state.TransactionState == TransactionState.Empty)
        {
            state.Epoch++;
            state.TransactionalId = transactionalId;
        }

        return (producerId, state.Epoch, ErrorCode.None);
    }

    /// <summary>
    /// Validates a produce request's sequence number for idempotent delivery.
    /// </summary>
    public ProduceSequenceStatus ValidateSequence(long producerId, short epoch, int baseSequence, TopicPartition topicPartition)
    {
        if (producerId == KafkaConstants.Producer.NoProducerId)
        {
            // Non-idempotent producer - no validation needed
            return ProduceSequenceStatus.Ok;
        }

        if (!_producers.TryGetValue(producerId, out var state))
        {
            // Unknown producer - accept but track
            var newState = new ProducerState(producerId, epoch);
            _producers[producerId] = newState;
            newState.UpdateSequence(topicPartition, baseSequence);
            return ProduceSequenceStatus.Ok;
        }

        // Validate epoch
        if (epoch != state.Epoch)
        {
            return epoch < state.Epoch
                ? ProduceSequenceStatus.InvalidProducerEpoch
                : ProduceSequenceStatus.UnknownProducerId;
        }

        // Validate sequence
        var lastSequence = state.GetLastSequence(topicPartition);
        if (lastSequence == KafkaConstants.Producer.NoSequence)
        {
            // First batch for this partition - accept any sequence
            state.UpdateSequence(topicPartition, baseSequence);
            return ProduceSequenceStatus.Ok;
        }

        var expectedSequence = (lastSequence + 1) & int.MaxValue; // Wrap around
        if (baseSequence == expectedSequence)
        {
            // Expected sequence - accept
            state.UpdateSequence(topicPartition, baseSequence);
            return ProduceSequenceStatus.Ok;
        }

        if (baseSequence == lastSequence)
        {
            // Duplicate - reject silently (or return specific error)
            return ProduceSequenceStatus.DuplicateSequence;
        }

        // Out of order
        return ProduceSequenceStatus.OutOfOrderSequence;
    }

    /// <summary>
    /// Begins a transaction for a producer.
    /// </summary>
    public ErrorCode BeginTransaction(long producerId, short epoch)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return ErrorCode.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return ErrorCode.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Empty &&
            state.TransactionState != TransactionState.CompleteCommit &&
            state.TransactionState != TransactionState.CompleteAbort)
        {
            return ErrorCode.ConcurrentTransactions;
        }

        state.TransactionState = TransactionState.Ongoing;
        state.TransactionPartitions.Clear();
        return ErrorCode.None;
    }

    /// <summary>
    /// Adds a partition to an ongoing transaction.
    /// </summary>
    public ErrorCode AddPartitionToTransaction(long producerId, short epoch, TopicPartition partition)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return ErrorCode.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return ErrorCode.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Ongoing)
        {
            return ErrorCode.InvalidTxnState;
        }

        state.TransactionPartitions.Add(partition);
        return ErrorCode.None;
    }

    /// <summary>
    /// Prepares to commit or abort a transaction.
    /// </summary>
    public ErrorCode PrepareEndTransaction(long producerId, short epoch, bool commit)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return ErrorCode.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return ErrorCode.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Ongoing)
        {
            return ErrorCode.InvalidTxnState;
        }

        state.TransactionState = commit
            ? TransactionState.PrepareCommit
            : TransactionState.PrepareAbort;

        return ErrorCode.None;
    }

    /// <summary>
    /// Completes a transaction (after writing markers).
    /// </summary>
    public void CompleteTransaction(long producerId, bool commit)
    {
        if (_producers.TryGetValue(producerId, out var state))
        {
            state.TransactionState = commit
                ? TransactionState.CompleteCommit
                : TransactionState.CompleteAbort;
            state.TransactionPartitions.Clear();
        }
    }

    /// <summary>
    /// Gets the partitions involved in a producer's transaction.
    /// </summary>
    public IReadOnlySet<TopicPartition>? GetTransactionPartitions(long producerId)
    {
        return _producers.TryGetValue(producerId, out var state)
            ? state.TransactionPartitions
            : null;
    }

    /// <summary>
    /// Gets the current state of a producer's transaction.
    /// </summary>
    public TransactionState GetTransactionState(long producerId)
    {
        return _producers.TryGetValue(producerId, out var state)
            ? state.TransactionState
            : TransactionState.Empty;
    }

    /// <summary>
    /// Checks if a producer has an ongoing transaction.
    /// </summary>
    public bool HasOngoingTransaction(long producerId)
    {
        return _producers.TryGetValue(producerId, out var state) &&
               state.TransactionState == TransactionState.Ongoing;
    }

    /// <summary>
    /// Updates the epoch for a producer (used when fencing during transactional recovery).
    /// Also resets sequence numbers since the new epoch starts fresh.
    /// </summary>
    public void UpdateEpoch(long producerId, short newEpoch)
    {
        if (_producers.TryGetValue(producerId, out var state))
        {
            state.Epoch = newEpoch;
            state.TransactionState = TransactionState.Empty;
            state.TransactionPartitions.Clear();
            state.ClearSequences();
        }
    }

    /// <summary>
    /// Internal producer state tracking.
    /// </summary>
    private sealed class ProducerState
    {
        public long ProducerId { get; }
        public short Epoch { get; set; }
        public string? TransactionalId { get; set; }
        public TransactionState TransactionState { get; set; } = TransactionState.Empty;
        public HashSet<TopicPartition> TransactionPartitions { get; } = new();

        private readonly ConcurrentDictionary<TopicPartition, int> _lastSequences = new();

        public ProducerState(long producerId, short epoch)
        {
            ProducerId = producerId;
            Epoch = epoch;
        }

        public int GetLastSequence(TopicPartition partition)
        {
            return _lastSequences.TryGetValue(partition, out var seq)
                ? seq
                : KafkaConstants.Producer.NoSequence;
        }

        public void UpdateSequence(TopicPartition partition, int sequence)
        {
            _lastSequences[partition] = sequence;
        }

        public void ClearSequences()
        {
            _lastSequences.Clear();
        }
    }
}
