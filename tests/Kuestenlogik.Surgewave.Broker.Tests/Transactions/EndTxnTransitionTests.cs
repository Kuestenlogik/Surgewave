using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Testing;
using Xunit;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// EndTxn's answer per state, as Kafka's coordinator gives it (#190): transactions V1 with the session's
/// epoch, transactions V2 with an epoch bump per EndTxn and retries recognised by the epoch below.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class EndTxnTransitionTests
{
    private const long Pid = 1000;
    private const short Epoch = 5;

    private static TransactionMetadata Txn(TransactionState state, long pid = Pid, short epoch = Epoch, long previousPid = -1)
        => new() { TransactionalId = "t", ProducerId = pid, ProducerEpoch = epoch, PreviousProducerId = previousPid, State = state };

    private static EndTxnDecision V1(TransactionMetadata txn, bool commit, long pid = Pid, short epoch = Epoch)
        => EndTxnTransition.Decide(txn, pid, epoch, commit, TransactionVersion.Tv0);

    private static EndTxnDecision V2(TransactionMetadata txn, bool commit, long pid = Pid, short epoch = Epoch)
        => EndTxnTransition.Decide(txn, pid, epoch, commit, TransactionVersion.Tv2);

    // ── Transactions V1 ─────────────────────────────────────────────────────

    [Fact]
    public void V1_Ongoing_Completes()
        => Assert.Equal(EndTxnDecision.Complete(empty: false), V1(Txn(TransactionState.Ongoing), commit: true));

    [Theory]
    [InlineData(TransactionState.CompleteCommit, true)]
    [InlineData(TransactionState.CompleteAbort, false)]
    public void V1_ARetryOfACompletedEndTxn_IsASuccess(TransactionState state, bool commit)
        => Assert.Equal(EndTxnDecision.AlreadyDone, V1(Txn(state), commit));

    [Theory]
    [InlineData(TransactionState.CompleteCommit, false)]
    [InlineData(TransactionState.CompleteAbort, true)]
    [InlineData(TransactionState.PrepareCommit, false)]
    [InlineData(TransactionState.PrepareAbort, true)]
    [InlineData(TransactionState.Empty, true)]
    public void V1_TheOtherOutcomeOrNothingToEnd_IsAnInvalidTransition(TransactionState state, bool commit)
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.InvalidTxnState), V1(Txn(state), commit));

    [Theory]
    [InlineData(TransactionState.PrepareCommit, true)]
    [InlineData(TransactionState.PrepareAbort, false)]
    public void V1_WhileTheMarkersAreWritten_IsConcurrentTransactions(TransactionState state, bool commit)
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ConcurrentTransactions), V1(Txn(state), commit));

    [Fact]
    public void V1_AnotherEpoch_IsProducerFenced()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced), V1(Txn(TransactionState.Ongoing), true, epoch: Epoch - 1));

    [Fact]
    public void V1_AnotherProducerId_IsAnInvalidMapping()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.InvalidProducerIdMapping), V1(Txn(TransactionState.Ongoing), true, pid: Pid + 1));

    // ── Transactions V2 ─────────────────────────────────────────────────────

    [Fact]
    public void V2_Ongoing_Completes()
        => Assert.Equal(EndTxnDecision.Complete(empty: false), V2(Txn(TransactionState.Ongoing), commit: true));

    [Fact]
    public void V2_Ongoing_AtThePreviousEpoch_IsProducerFenced()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced), V2(Txn(TransactionState.Ongoing), true, epoch: Epoch - 1));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void V2_Empty_EndsAnEmptyTransaction(bool commit)
        => Assert.Equal(EndTxnDecision.Complete(empty: true), V2(Txn(TransactionState.Empty), commit));

    [Theory]
    [InlineData(TransactionState.CompleteCommit, true)]
    [InlineData(TransactionState.CompleteAbort, false)]
    public void V2_ARetryAtTheEpochBelow_IsASuccess(TransactionState state, bool commit)
        => Assert.Equal(EndTxnDecision.AlreadyDone, V2(Txn(state, epoch: Epoch + 1), commit));

    [Theory]
    [InlineData(TransactionState.CompleteCommit, true)]
    [InlineData(TransactionState.CompleteCommit, false)]
    [InlineData(TransactionState.CompleteAbort, true)]
    [InlineData(TransactionState.CompleteAbort, false)]
    public void V2_AtTheCurrentEpochOnACompletedTransaction_EndsTheNextEmptyOne(TransactionState state, bool commit)
        => Assert.Equal(EndTxnDecision.Complete(empty: true), V2(Txn(state), commit));

    [Fact]
    public void V2_ACommitRetriedOnATransactionTheCoordinatorAborted_IsProducerFenced()
    {
        // KAFKA-20785: the timeout abort bumped the epoch while the commit was in flight. The commit never
        // took effect; PRODUCER_FENCED lets the client recover where INVALID_TXN_STATE would kill it.
        Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced),
            V2(Txn(TransactionState.CompleteAbort, epoch: Epoch + 1), commit: true));
    }

    [Fact]
    public void V2_AnAbortRetriedOnACommittedTransaction_IsAnInvalidTransition()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.InvalidTxnState),
            V2(Txn(TransactionState.CompleteCommit, epoch: Epoch + 1), commit: false));

    [Fact]
    public void V2_ARetryWhileTheMarkersAreWritten_IsConcurrentTransactions()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ConcurrentTransactions),
            V2(Txn(TransactionState.PrepareCommit, epoch: Epoch + 1), commit: true));

    [Fact]
    public void V2_ThePreparedEpochItself_IsProducerFenced()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced),
            V2(Txn(TransactionState.PrepareCommit), commit: true));

    [Fact]
    public void V2_TwoEpochsBelow_IsProducerFenced()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.ProducerFenced),
            V2(Txn(TransactionState.CompleteCommit, epoch: Epoch + 2), commit: true));

    [Fact]
    public void V2_ARetryAcrossAProducerIdRotation_IsASuccess()
    {
        // The epoch was exhausted: the transaction completed onto a fresh id at epoch 0, and the client still
        // holds the old id at the last epoch it saw.
        var txn = Txn(TransactionState.CompleteCommit, pid: Pid + 1, epoch: 0, previousPid: Pid);

        Assert.Equal(EndTxnDecision.AlreadyDone, V2(txn, commit: true, pid: Pid, epoch: short.MaxValue - 1));
    }

    [Fact]
    public void V2_AnotherProducerId_IsAnInvalidMapping()
        => Assert.Equal(EndTxnDecision.Refuse(TxnErrorStatus.InvalidProducerIdMapping),
            V2(Txn(TransactionState.Ongoing), true, pid: Pid + 1));
}
