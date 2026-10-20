using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// The transaction coordinator under transactions V2 and with Kafka's EndTxn answers (#190): the epoch is
/// bumped on every EndTxn and returned, retries are answered with the same outcome, the producer id rotates
/// when the epoch is exhausted, the timeout abort fences the producer, and a transaction left prepared by a
/// restart is completed.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionsV2CoordinatorTests : IAsyncDisposable
{
    private readonly string _dir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private TransactionCoordinator _coordinator;

    public TransactionsV2CoordinatorTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"surgewave-tv2-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _logManager = new LogManager(_dir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _offsetStore = new OffsetStore(_dir, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_dir, "txn-state"), NullLogger<TransactionStateStore>.Instance);
        _coordinator = NewCoordinator();
    }

    private TransactionCoordinator NewCoordinator() => new(
        new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _stateStore,
        NullLogger<TransactionCoordinator>.Instance);

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _stateStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private async Task<(long ProducerId, short Epoch)> InitAsync(string txnId, int timeoutMs = 60_000)
    {
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = txnId, TransactionTimeoutMs = timeoutMs, ProducerId = -1, ProducerEpoch = -1 },
            CancellationToken.None);
        Assert.Equal(TxnErrorStatus.None, init.Status);
        return (init.ProducerId, init.ProducerEpoch);
    }

    private TxnErrorStatus AddPartition(string txnId, long pid, short epoch)
        => _coordinator.AddPartitionsToTxn(new AddPartitionsToTxnCommand
        {
            TransactionalId = txnId,
            ProducerId = pid,
            ProducerEpoch = epoch,
            Topics = [new AddPartitionsTopic("orders", [0])],
        }).Topics[0].Partitions[0].Status;

    private Task<EndTxnResult> EndAsync(string txnId, long pid, short epoch, bool commit, TransactionVersion version)
        => _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = txnId, ProducerId = pid, ProducerEpoch = epoch, Committed = commit, TransactionVersion = version },
            CancellationToken.None);

    private TransactionDescription Describe(string txnId) => Assert.Single(_coordinator.DescribeTransactions([txnId]));

    private void Seed(PersistedTransactionState state) => _stateStore.SaveTransactionState(state);

    // ── Transactions V2 ─────────────────────────────────────────────────────

    [Fact]
    public async Task V2_Commit_BumpsTheEpochAndReturnsIt()
    {
        var (pid, epoch) = await InitAsync("tv2-a");
        Assert.Equal(TxnErrorStatus.None, AddPartition("tv2-a", pid, epoch));

        var result = await EndAsync("tv2-a", pid, epoch, commit: true, TransactionVersion.Tv2);

        Assert.Equal(new EndTxnResult(TxnErrorStatus.None, pid, (short)(epoch + 1)), result);
        Assert.Equal("CompleteCommit", Describe("tv2-a").State);
    }

    [Fact]
    public async Task V2_ARetriedCommit_IsAnsweredWithTheSameEpoch()
    {
        var (pid, epoch) = await InitAsync("tv2-b");
        AddPartition("tv2-b", pid, epoch);
        await EndAsync("tv2-b", pid, epoch, commit: true, TransactionVersion.Tv2);

        var retry = await EndAsync("tv2-b", pid, epoch, commit: true, TransactionVersion.Tv2);

        Assert.Equal(new EndTxnResult(TxnErrorStatus.None, pid, (short)(epoch + 1)), retry);
    }

    [Fact]
    public async Task V2_ATransactionThatAddedNothing_IsEndedAndTheEpochBumped()
    {
        var (pid, epoch) = await InitAsync("tv2-c");

        var first = await EndAsync("tv2-c", pid, epoch, commit: true, TransactionVersion.Tv2);
        var next = await EndAsync("tv2-c", pid, first.ProducerEpoch, commit: false, TransactionVersion.Tv2);

        Assert.Equal((short)(epoch + 1), first.ProducerEpoch);
        Assert.Equal(new EndTxnResult(TxnErrorStatus.None, pid, (short)(epoch + 2)), next);
        Assert.Equal("CompleteAbort", Describe("tv2-c").State);
    }

    [Fact]
    public async Task V2_AnExhaustedEpoch_RotatesTheProducerId_AndTheRetryOnTheOldIdSucceeds()
    {
        const long oldPid = 4242;
        const short lastEpoch = short.MaxValue - 1;
        Seed(new PersistedTransactionState
        {
            TransactionalId = "tv2-d", ProducerId = oldPid, ProducerEpoch = lastEpoch, State = "Empty", TransactionTimeoutMs = 60_000,
        });
        await _coordinator.DisposeAsync();
        _coordinator = NewCoordinator();

        var result = await EndAsync("tv2-d", oldPid, lastEpoch, commit: true, TransactionVersion.Tv2);
        var retry = await EndAsync("tv2-d", oldPid, lastEpoch, commit: true, TransactionVersion.Tv2);

        Assert.Equal(TxnErrorStatus.None, result.Status);
        Assert.NotEqual(oldPid, result.ProducerId);
        Assert.Equal(0, result.ProducerEpoch);
        Assert.Equal(result, retry);
    }

    [Fact]
    public async Task V2_TheLastEpochSurvivesARestart_SoARetryIsStillRecognised()
    {
        var (pid, epoch) = await InitAsync("tv2-e");
        AddPartition("tv2-e", pid, epoch);
        await EndAsync("tv2-e", pid, epoch, commit: true, TransactionVersion.Tv2);

        await _coordinator.DisposeAsync();
        _coordinator = NewCoordinator();
        var retry = await EndAsync("tv2-e", pid, epoch, commit: true, TransactionVersion.Tv2);

        Assert.Equal(new EndTxnResult(TxnErrorStatus.None, pid, (short)(epoch + 1)), retry);
    }

    // ── Transactions V1 ─────────────────────────────────────────────────────

    [Fact]
    public async Task V1_ARetriedCommit_IsASuccessAndTheEpochStays()
    {
        var (pid, epoch) = await InitAsync("tv1-a");
        AddPartition("tv1-a", pid, epoch);

        var first = await EndAsync("tv1-a", pid, epoch, commit: true, TransactionVersion.Tv0);
        var retry = await EndAsync("tv1-a", pid, epoch, commit: true, TransactionVersion.Tv0);

        Assert.Equal(new EndTxnResult(TxnErrorStatus.None, pid, epoch), first);
        Assert.Equal(first, retry);
    }

    [Fact]
    public async Task V1_AnOldEpoch_IsProducerFenced()
    {
        var (pid, epoch) = await InitAsync("tv1-b");
        AddPartition("tv1-b", pid, epoch);

        var result = await EndAsync("tv1-b", pid, (short)(epoch - 1), commit: true, TransactionVersion.Tv0);

        Assert.Equal(TxnErrorStatus.ProducerFenced, result.Status);
    }

    [Fact]
    public async Task AddingOffsetsAfterACompletedTransaction_StartsTheNextOne()
    {
        // Was INVALID_TXN_STATE: AddOffsetsToTxn accepted only Empty and Ongoing, so a consume-transform-produce
        // loop that adds its offsets before its partitions failed on its second transaction.
        var (pid, epoch) = await InitAsync("tv1-c");
        AddPartition("tv1-c", pid, epoch);
        await EndAsync("tv1-c", pid, epoch, commit: true, TransactionVersion.Tv0);

        var added = _coordinator.AddOffsetsToTxn(new AddOffsetsToTxnCommand
        {
            TransactionalId = "tv1-c", ProducerId = pid, ProducerEpoch = epoch, GroupId = "g",
        });

        Assert.Equal(TxnErrorStatus.None, added.Status);
        Assert.Equal("Ongoing", Describe("tv1-c").State);
    }

    // ── Timeout abort and recovery ──────────────────────────────────────────

    [Theory]
    [InlineData(TransactionVersion.Tv2)]
    [InlineData(TransactionVersion.Tv0)]
    public async Task TheTimeoutAbort_FencesTheProducer_AndACommitInFlightIsProducerFenced(TransactionVersion version)
    {
        var (pid, epoch) = await InitAsync("to-" + version, timeoutMs: 1_000);
        AddPartition("to-" + version, pid, epoch);

        await _coordinator.ExpireTransactionsAsync(DateTimeOffset.UtcNow.AddSeconds(5), CancellationToken.None);

        var described = Describe("to-" + version);
        Assert.Equal("CompleteAbort", described.State);
        Assert.Equal((short)(epoch + 1), described.ProducerEpoch);

        var commit = await EndAsync("to-" + version, pid, epoch, commit: true, version);
        Assert.Equal(TxnErrorStatus.ProducerFenced, commit.Status);
    }

    [Fact]
    public async Task ATransactionLeftPreparedByARestart_IsCompletedAndItsOffsetsCommitted()
    {
        Seed(new PersistedTransactionState
        {
            TransactionalId = "rec-a", ProducerId = 77, ProducerEpoch = 3, State = "PrepareCommit", TransactionTimeoutMs = 60_000,
            Partitions = [new PersistedPartition { Topic = "orders", Partition = 0 }],
            ConsumerGroups = ["g"],
            PendingOffsets = [new PendingOffsetEntry { GroupId = "g", Topic = "orders", Partition = 0, Offset = 42 }],
        });
        await _coordinator.DisposeAsync();
        _coordinator = NewCoordinator();

        await _coordinator.ExpireTransactionsAsync(DateTimeOffset.UtcNow, CancellationToken.None);

        Assert.Equal("CompleteCommit", Describe("rec-a").State);
        Assert.Equal(42, _offsetStore.GetCommittedOffset("g", "orders", 0));
    }

    [Fact]
    public async Task WhileATransactionIsPrepared_EndTxnAndInitProducerIdAreConcurrentTransactions()
    {
        Seed(new PersistedTransactionState
        {
            TransactionalId = "prep-a", ProducerId = 78, ProducerEpoch = 3, State = "PrepareCommit", TransactionTimeoutMs = 60_000,
        });
        await _coordinator.DisposeAsync();
        _coordinator = NewCoordinator();

        var end = await EndAsync("prep-a", 78, 3, commit: true, TransactionVersion.Tv0);
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = "prep-a", TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            CancellationToken.None);

        Assert.Equal(TxnErrorStatus.ConcurrentTransactions, end.Status);
        Assert.Equal(TxnErrorStatus.ConcurrentTransactions, init.Status);
    }
}
