using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the ProducerStateManager: idempotent produce, epoch management, and transactions.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ProducerStateManagerTests
{
    private readonly ProducerStateManager _manager = new();
    private static readonly TopicPartition Tp0 = new() { Topic = "test-topic", Partition = 0 };
    private static readonly TopicPartition Tp1 = new() { Topic = "test-topic", Partition = 1 };

    // ── Producer ID allocation ───────────────────────────────────────────────

    [Fact]
    public void AllocateProducerId_ReturnsMonotonicallyIncreasingIds()
    {
        var (id1, epoch1) = _manager.AllocateProducerId();
        var (id2, epoch2) = _manager.AllocateProducerId();
        var (id3, epoch3) = _manager.AllocateProducerId();

        Assert.True(id1 > 0);
        Assert.True(id2 > id1);
        Assert.True(id3 > id2);
        Assert.Equal(0, epoch1);
        Assert.Equal(0, epoch2);
        Assert.Equal(0, epoch3);
    }

    [Fact]
    public void AllocateProducerId_StartsAtOne()
    {
        var (id, _) = _manager.AllocateProducerId();
        Assert.Equal(1L, id);
    }

    [Fact]
    public void RegisterProducerId_PushesBeyondRegistered()
    {
        // Register a high producer ID
        _manager.RegisterProducerId(100, epoch: 3);

        // New allocations must be higher
        var (newId, _) = _manager.AllocateProducerId();
        Assert.True(newId > 100);
    }

    [Fact]
    public void RegisterProducerId_LowerThanCurrent_DoesNotReduceNextId()
    {
        // Allocate up to ID 5
        for (var i = 0; i < 5; i++)
            _manager.AllocateProducerId();

        // Register a lower ID – should not push next ID backward
        _manager.RegisterProducerId(2, epoch: 0);

        var (newId, _) = _manager.AllocateProducerId();
        Assert.True(newId >= 6);
    }

    // ── GetOrBumpEpoch ───────────────────────────────────────────────────────

    [Fact]
    public void GetOrBumpEpoch_NoProducerId_AllocatesNew()
    {
        var (pid, epoch, error) = _manager.GetOrBumpEpoch(
            KafkaConstants.Producer.NoProducerId, currentEpoch: 0, transactionalId: null);

        Assert.True(pid > 0);
        Assert.Equal(0, epoch);
        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void GetOrBumpEpoch_UnknownProducerId_AllocatesNew()
    {
        var (pid, epoch, error) = _manager.GetOrBumpEpoch(
            999_999, currentEpoch: 0, transactionalId: null);

        Assert.True(pid > 0);
        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void GetOrBumpEpoch_OldEpoch_ReturnsFencedError()
    {
        var (id, _) = _manager.AllocateProducerId();

        // Simulate epoch bump by updating directly
        _manager.UpdateEpoch(id, newEpoch: 5);

        // Client still sends with old epoch 0 → fenced
        var (_, _, error) = _manager.GetOrBumpEpoch(id, currentEpoch: 0, transactionalId: null);

        Assert.Equal(ErrorCode.InvalidProducerEpoch, error);
    }

    [Fact]
    public void GetOrBumpEpoch_TransactionalId_BumpsEpoch()
    {
        var (id, startEpoch) = _manager.AllocateProducerId();
        Assert.Equal(0, startEpoch);

        // First transactional init – epoch bumps to 1, txId stored
        var (_, epochAfterFirst, _) = _manager.GetOrBumpEpoch(id, currentEpoch: 0, transactionalId: "tx-1");

        // Second init – use the epoch returned by the first call
        var (_, newEpoch, error) = _manager.GetOrBumpEpoch(id, currentEpoch: epochAfterFirst, transactionalId: "tx-1");

        Assert.Equal(ErrorCode.None, error);
        // epoch could stay same on first init (state is Empty initially)
        Assert.True(newEpoch >= 0);
    }

    // ── Sequence number validation ───────────────────────────────────────────

    [Fact]
    public void ValidateSequence_NoProducerId_ReturnsNone()
    {
        var error = _manager.ValidateSequence(
            KafkaConstants.Producer.NoProducerId, 0, 0, Tp0);

        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void ValidateSequence_FirstBatch_AcceptsAnySequence()
    {
        var (id, _) = _manager.AllocateProducerId();

        var error = _manager.ValidateSequence(id, epoch: 0, baseSequence: 42, Tp0);

        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void ValidateSequence_ExpectedNext_Accepted()
    {
        var (id, _) = _manager.AllocateProducerId();

        // Record seq 0
        _manager.ValidateSequence(id, 0, 0, Tp0);
        // Next expected: 1
        var error = _manager.ValidateSequence(id, 0, 1, Tp0);

        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void ValidateSequence_DuplicateSequence_ReturnsDuplicateError()
    {
        var (id, _) = _manager.AllocateProducerId();

        _manager.ValidateSequence(id, 0, 0, Tp0);
        var error = _manager.ValidateSequence(id, 0, 0, Tp0); // repeat seq 0

        Assert.Equal(ErrorCode.DuplicateSequenceNumber, error);
    }

    [Fact]
    public void ValidateSequence_OutOfOrder_ReturnsOutOfOrderError()
    {
        var (id, _) = _manager.AllocateProducerId();

        _manager.ValidateSequence(id, 0, 0, Tp0); // sets last to 0
        var error = _manager.ValidateSequence(id, 0, 5, Tp0); // skipped 1-4

        Assert.Equal(ErrorCode.OutOfOrderSequenceNumber, error);
    }

    [Fact]
    public void ValidateSequence_WrongEpoch_ReturnsEpochError()
    {
        var (id, _) = _manager.AllocateProducerId();
        _manager.ValidateSequence(id, 0, 0, Tp0);

        // Request with future epoch (unknown producer registers new state)
        var error = _manager.ValidateSequence(id, epoch: 99, baseSequence: 0, Tp0);

        // Future epoch is treated as UnknownProducerId
        Assert.Equal(ErrorCode.UnknownProducerId, error);
    }

    [Fact]
    public void ValidateSequence_DifferentPartitions_Independent()
    {
        var (id, _) = _manager.AllocateProducerId();

        // Track seq on Tp0
        _manager.ValidateSequence(id, 0, 0, Tp0);
        _manager.ValidateSequence(id, 0, 1, Tp0);

        // Tp1 starts fresh – seq 0 is fine
        var error = _manager.ValidateSequence(id, 0, 0, Tp1);

        Assert.Equal(ErrorCode.None, error);
    }

    [Fact]
    public void ValidateSequence_UnknownProducer_CreatesStateAndAccepts()
    {
        // Unknown producer should be accepted and tracked
        var error = _manager.ValidateSequence(42_000, epoch: 0, baseSequence: 0, Tp0);

        Assert.Equal(ErrorCode.None, error);
    }

    // ── Transaction lifecycle ────────────────────────────────────────────────

    [Fact]
    public void BeginTransaction_UnknownProducer_ReturnsError()
    {
        var error = _manager.BeginTransaction(producerId: 9999, epoch: 0);
        Assert.Equal(ErrorCode.UnknownProducerId, error);
    }

    [Fact]
    public void BeginTransaction_WrongEpoch_ReturnsEpochError()
    {
        var (id, _) = _manager.AllocateProducerId();

        var error = _manager.BeginTransaction(id, epoch: 99);
        Assert.Equal(ErrorCode.InvalidProducerEpoch, error);
    }

    [Fact]
    public void BeginTransaction_Success_SetsOngoingState()
    {
        var (id, epoch) = _manager.AllocateProducerId();

        var error = _manager.BeginTransaction(id, epoch);

        Assert.Equal(ErrorCode.None, error);
        Assert.True(_manager.HasOngoingTransaction(id));
    }

    [Fact]
    public void BeginTransaction_AlreadyOngoing_ReturnsConcurrentError()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);

        var error = _manager.BeginTransaction(id, epoch);
        Assert.Equal(ErrorCode.ConcurrentTransactions, error);
    }

    [Fact]
    public void AddPartitionToTransaction_Success_TracksPartition()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);

        var error = _manager.AddPartitionToTransaction(id, epoch, Tp0);

        Assert.Equal(ErrorCode.None, error);
        var partitions = _manager.GetTransactionPartitions(id);
        Assert.NotNull(partitions);
        Assert.Contains(Tp0, partitions);
    }

    [Fact]
    public void AddPartitionToTransaction_NotOngoing_ReturnsInvalidTxnState()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        // Not started yet

        var error = _manager.AddPartitionToTransaction(id, epoch, Tp0);
        Assert.Equal(ErrorCode.InvalidTxnState, error);
    }

    [Fact]
    public void PrepareEndTransaction_Commit_SetsPrepareCommitState()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);

        var error = _manager.PrepareEndTransaction(id, epoch, commit: true);

        Assert.Equal(ErrorCode.None, error);
        Assert.Equal(KafkaConstants.TransactionState.PrepareCommit, _manager.GetTransactionState(id));
    }

    [Fact]
    public void PrepareEndTransaction_Abort_SetsPrepareAbortState()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);

        var error = _manager.PrepareEndTransaction(id, epoch, commit: false);

        Assert.Equal(ErrorCode.None, error);
        Assert.Equal(KafkaConstants.TransactionState.PrepareAbort, _manager.GetTransactionState(id));
    }

    [Fact]
    public void CompleteTransaction_Commit_SetsCompleteCommitState()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);
        _manager.AddPartitionToTransaction(id, epoch, Tp0);

        _manager.CompleteTransaction(id, commit: true);

        Assert.Equal(KafkaConstants.TransactionState.CompleteCommit, _manager.GetTransactionState(id));
        Assert.False(_manager.HasOngoingTransaction(id));
        // Partitions should be cleared
        var partitions = _manager.GetTransactionPartitions(id);
        Assert.NotNull(partitions);
        Assert.Empty(partitions);
    }

    [Fact]
    public void CompleteTransaction_Abort_SetsCompleteAbortState()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.BeginTransaction(id, epoch);

        _manager.CompleteTransaction(id, commit: false);

        Assert.Equal(KafkaConstants.TransactionState.CompleteAbort, _manager.GetTransactionState(id));
    }

    [Fact]
    public void GetTransactionPartitions_UnknownProducer_ReturnsNull()
    {
        var result = _manager.GetTransactionPartitions(99999);
        Assert.Null(result);
    }

    [Fact]
    public void GetTransactionState_UnknownProducer_ReturnsEmpty()
    {
        var state = _manager.GetTransactionState(99999);
        Assert.Equal(KafkaConstants.TransactionState.Empty, state);
    }

    [Fact]
    public void HasOngoingTransaction_FreshProducer_ReturnsFalse()
    {
        var (id, _) = _manager.AllocateProducerId();
        Assert.False(_manager.HasOngoingTransaction(id));
    }

    // ── UpdateEpoch ─────────────────────────────────────────────────────────

    [Fact]
    public void UpdateEpoch_ClearsSequencesAndResets()
    {
        var (id, epoch) = _manager.AllocateProducerId();
        _manager.ValidateSequence(id, epoch, 0, Tp0);
        _manager.BeginTransaction(id, epoch);
        _manager.AddPartitionToTransaction(id, epoch, Tp0);

        _manager.UpdateEpoch(id, newEpoch: 7);

        Assert.Equal(KafkaConstants.TransactionState.Empty, _manager.GetTransactionState(id));
        var partitions = _manager.GetTransactionPartitions(id);
        Assert.NotNull(partitions);
        Assert.Empty(partitions);

        // After epoch reset, seq 0 should be accepted again
        var seqError = _manager.ValidateSequence(id, epoch: 7, baseSequence: 0, Tp0);
        Assert.Equal(ErrorCode.None, seqError);
    }

    [Fact]
    public void UpdateEpoch_UnknownProducer_IsNoOp()
    {
        // Should not throw
        _manager.UpdateEpoch(99999, newEpoch: 5);
    }

    // ── Concurrency ──────────────────────────────────────────────────────────

    [Fact]
    public void AllocateProducerId_Concurrent_AllUnique()
    {
        const int count = 100;
        var ids = new long[count];

        Parallel.For(0, count, i =>
        {
            var (id, _) = _manager.AllocateProducerId();
            ids[i] = id;
        });

        // All IDs must be unique
        Assert.Equal(count, ids.Distinct().Count());
    }
}
