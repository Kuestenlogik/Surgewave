using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Coordination.Transactions;
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
            var lastSeq = ps.GetLastWritten(partition)?.LastSequence ?? KafkaConstants.Producer.NoSequence;
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
    public (long ProducerId, short Epoch, TxnErrorStatus Error) GetOrBumpEpoch(
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
            return (newId, epoch, TxnErrorStatus.None);
        }

        if (!_producers.TryGetValue(producerId, out var state))
        {
            // Unknown producer - allocate new
            var (newId, epoch) = AllocateProducerId();
            return (newId, epoch, TxnErrorStatus.None);
        }

        // Validate epoch
        if (currentEpoch != state.Epoch)
        {
            if (currentEpoch < state.Epoch)
            {
                // Fenced - old producer
                return (producerId, state.Epoch, TxnErrorStatus.InvalidProducerEpoch);
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

        return (producerId, state.Epoch, TxnErrorStatus.None);
    }

    /// <summary>
    /// Validates a produce request's sequence number for idempotent delivery, WITHOUT recording it.
    /// <see cref="CommitSequence"/> does the recording, once the batch is actually in the log.
    /// </summary>
    /// <param name="partitionNeverWritten">
    /// No record was ever appended to the partition (its log end offset is 0). Then no producer state
    /// can have been lost, and a producer without state must start at sequence 0 (KAFKA-15591).
    /// </param>
    public ProduceSequenceCheck ValidateSequence(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta, TopicPartition topicPartition,
        bool partitionNeverWritten)
    {
        if (producerId == KafkaConstants.Producer.NoProducerId)
        {
            // Non-idempotent producer - no validation needed
            return ProduceSequenceCheck.Ok;
        }

        if (!_producers.TryGetValue(producerId, out var state))
        {
            // Unknown producer - accept; it is registered when the batch lands.
            return FirstSequenceCheck(baseSequence, partitionNeverWritten);
        }

        // Validate epoch
        if (epoch != state.Epoch)
        {
            return ProduceSequenceCheck.Failed(epoch < state.Epoch
                ? ProduceSequenceStatus.InvalidProducerEpoch
                : ProduceSequenceStatus.UnknownProducerId);
        }

        // Validate sequence
        var written = state.GetLastWritten(topicPartition);
        if (written is null)
        {
            // First batch of this producer on this partition.
            return FirstSequenceCheck(baseSequence, partitionNeverWritten);
        }

        // The producer numbers RECORDS, not batches: the batch that follows one spanning sequences
        // [first..last] starts at last + 1.
        var expectedSequence = (written.Value.LastSequence + 1) & int.MaxValue;
        if (baseSequence == expectedSequence)
        {
            return ProduceSequenceCheck.Ok;
        }

        if (baseSequence == written.Value.FirstSequence)
        {
            // The producer resent a batch we already have — the ordinary outcome of an
            // acknowledgement that never arrived. Hand back where it landed the first time.
            return ProduceSequenceCheck.Duplicate(written.Value.BaseOffset);
        }

        // Out of order
        return ProduceSequenceCheck.Failed(ProduceSequenceStatus.OutOfOrderSequence);
    }

    /// <summary>
    /// The first batch of a producer without state on a partition. On a partition that has had records
    /// any sequence is accepted, because the state may have expired or been cleaned with them. On a
    /// partition that never had one, only 0 is: a producer whose requests were reordered while the
    /// partition was created would otherwise get its later batch appended first, and every retry of the
    /// earlier one would be out of order — its records lost despite idempotence (KAFKA-15591). The
    /// refusal is retriable; the earlier batch lands, then the later one in order.
    /// </summary>
    private static ProduceSequenceCheck FirstSequenceCheck(int baseSequence, bool partitionNeverWritten) =>
        partitionNeverWritten && baseSequence != 0
            ? ProduceSequenceCheck.Failed(ProduceSequenceStatus.OutOfOrderSequence)
            : ProduceSequenceCheck.Ok;

    /// <summary>
    /// Records a batch that has been appended, so a retransmit is recognisable and answerable with
    /// the offset it originally landed at.
    /// </summary>
    /// <remarks>
    /// Only the most recent batch per partition is remembered. Kafka keeps the last five, which
    /// tolerates a producer with several requests in flight retrying an older one; this recognises
    /// the immediately preceding batch, which is the case that a single in-flight request produces.
    /// </remarks>
    public void CommitSequence(
        long producerId, short epoch, int baseSequence, int lastOffsetDelta,
        TopicPartition topicPartition, long baseOffset)
    {
        if (producerId == KafkaConstants.Producer.NoProducerId)
            return;

        var state = _producers.GetOrAdd(producerId, id => new ProducerState(id, epoch));
        var lastSequence = (baseSequence + Math.Max(lastOffsetDelta, 0)) & int.MaxValue;
        state.RecordWritten(topicPartition, baseSequence, lastSequence, baseOffset);
    }

    /// <summary>
    /// Begins a transaction for a producer.
    /// </summary>
    public TxnErrorStatus BeginTransaction(long producerId, short epoch)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return TxnErrorStatus.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return TxnErrorStatus.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Empty &&
            state.TransactionState != TransactionState.CompleteCommit &&
            state.TransactionState != TransactionState.CompleteAbort)
        {
            return TxnErrorStatus.ConcurrentTransactions;
        }

        state.TransactionState = TransactionState.Ongoing;
        state.TransactionPartitions.Clear();
        return TxnErrorStatus.None;
    }

    /// <summary>
    /// Adds a partition to an ongoing transaction.
    /// </summary>
    public TxnErrorStatus AddPartitionToTransaction(long producerId, short epoch, TopicPartition partition)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return TxnErrorStatus.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return TxnErrorStatus.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Ongoing)
        {
            return TxnErrorStatus.InvalidTxnState;
        }

        state.TransactionPartitions.Add(partition);
        return TxnErrorStatus.None;
    }

    /// <summary>
    /// Prepares to commit or abort a transaction.
    /// </summary>
    public TxnErrorStatus PrepareEndTransaction(long producerId, short epoch, bool commit)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            return TxnErrorStatus.UnknownProducerId;
        }

        if (epoch != state.Epoch)
        {
            return TxnErrorStatus.InvalidProducerEpoch;
        }

        if (state.TransactionState != TransactionState.Ongoing)
        {
            return TxnErrorStatus.InvalidTxnState;
        }

        state.TransactionState = commit
            ? TransactionState.PrepareCommit
            : TransactionState.PrepareAbort;

        return TxnErrorStatus.None;
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
    /// Moves a producer to <paramref name="epoch"/> when that is not its current epoch, with fresh sequences —
    /// the epoch a transactional producer continues with after EndTxn under transactions V2 or after a fence.
    /// An unchanged epoch keeps the sequences: under transactions V1 the producer numbers on across transactions.
    /// </summary>
    public void AdvanceEpoch(long producerId, short epoch)
    {
        if (!_producers.TryGetValue(producerId, out var state))
        {
            RegisterProducerId(producerId, epoch);
            return;
        }
        if (state.Epoch != epoch)
        {
            UpdateEpoch(producerId, epoch);
        }
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

        private readonly ConcurrentDictionary<TopicPartition, WrittenBatch> _lastWritten = new();

        public ProducerState(long producerId, short epoch)
        {
            ProducerId = producerId;
            Epoch = epoch;
        }

        public WrittenBatch? GetLastWritten(TopicPartition partition)
            => _lastWritten.TryGetValue(partition, out var written) ? written : null;

        public void RecordWritten(TopicPartition partition, int firstSequence, int lastSequence, long baseOffset)
            => _lastWritten[partition] = new WrittenBatch(firstSequence, lastSequence, baseOffset);

        public void ClearSequences()
        {
            _lastWritten.Clear();
        }

        /// <summary>The last batch this producer actually got into the log for a partition.</summary>
        public readonly record struct WrittenBatch(int FirstSequence, int LastSequence, long BaseOffset);
    }
}
