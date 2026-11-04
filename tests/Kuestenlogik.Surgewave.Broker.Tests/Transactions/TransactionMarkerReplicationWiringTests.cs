using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// A transaction's markers go to the leaders of its partitions, and the transaction completes once every partition
/// has its marker (#197). The coordinator appends only to the partitions it leads itself; the replicator hands the
/// others to their leaders. It used to write every marker into its own log — a follower's copy, for a partition led
/// elsewhere — send them on best-effort and complete the transaction whatever the leaders said.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionMarkerReplicationWiringTests : IAsyncDisposable
{
    private const string Topic = "orders";

    // The cluster as the coordinator sees it: which partitions other brokers lead, and which of those take a marker.
    private sealed class ClusterReplicator : ITransactionMarkerReplicator
    {
        public readonly List<IReadOnlyList<TopicPartition>> Calls = [];
        public readonly HashSet<int> LedHere = [];
        public readonly HashSet<int> Refusing = [];
        public bool Throw;

        public Task<MarkerReplicationResult> ReplicateMarkersAsync(
            string transactionalId, long producerId, short producerEpoch,
            IReadOnlyList<TopicPartition> partitions, bool commit, int coordinatorEpoch, CancellationToken ct)
        {
            Calls.Add([.. partitions]);
            if (Throw)
                throw new InvalidOperationException("simulated transport fault");
            var result = new MarkerReplicationResult();
            foreach (var tp in partitions)
            {
                result.PartitionOutcomes[tp] = LedHere.Contains(tp.Partition) ? MarkerPartitionOutcome.LocalLeader
                    : Refusing.Contains(tp.Partition) ? MarkerPartitionOutcome.Failed
                    : MarkerPartitionOutcome.Replicated;
            }
            result.IsSuccess = !result.PartitionOutcomes.ContainsValue(MarkerPartitionOutcome.Failed);
            return Task.FromResult(result);
        }
    }

    private readonly string _dir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog _stateStore;
    private readonly TransactionCoordinator _coordinator;
    private readonly ClusterReplicator _cluster = new();

    public TransactionMarkerReplicationWiringTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"surgewave-markers-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _logManager = new LogManager(_dir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _offsetStore = OffsetStore.InMemory();
        _stateStore = Kuestenlogik.Surgewave.Broker.Transactions.TransactionStateLog.InMemory();
        _coordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _stateStore,
            NullLogger<TransactionCoordinator>.Instance);
        _coordinator.JoinCluster(_cluster, commitGate: null, remoteVerifier: null);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task AMarkerForAPartitionLedElsewhereGoesToItsLeaderOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        await _logManager.CreateTopicAsync(Topic, 2, 1, cancellationToken: ct);
        _cluster.LedHere.Add(0);
        var (pid, epoch) = await StartTxn("tx-1", 0, 1);

        var result = await EndAsync("tx-1", pid, epoch, commit: true);

        Assert.Equal(TxnErrorStatus.None, result.Status);
        var sent = Assert.Single(_cluster.Calls);
        Assert.Equal([0, 1], sent.Select(p => p.Partition).Order());
        Assert.Equal(1, NextOffset(0)); // this broker leads partition 0 and wrote its marker
        Assert.Equal(0, NextOffset(1)); // partition 1's leader wrote it; this broker's copy is its follower's
        Assert.Equal("CompleteCommit", State("tx-1"));
    }

    [Fact]
    public async Task ATransactionStaysPreparedUntilEveryLeaderHasItsMarker()
    {
        _cluster.Refusing.Add(0);
        var (pid, epoch) = await StartTxn("tx-2", 0, 1);

        // The outcome is decided and durable, so EndTxn is answered; the transaction is not complete.
        var result = await EndAsync("tx-2", pid, epoch, commit: true);
        Assert.Equal(TxnErrorStatus.None, result.Status);
        Assert.Equal("PrepareCommit", State("tx-2"));
        var again = await _coordinator.AddPartitionsToTxnAsync(new AddPartitionsToTxnCommand
        {
            TransactionalId = "tx-2", ProducerId = pid, ProducerEpoch = epoch, Topics = [new AddPartitionsTopic(Topic, [2])],
        }, TestContext.Current.CancellationToken);
        Assert.Equal(TxnErrorStatus.ConcurrentTransactions, again.Topics.Single().Partitions.Single().Status);

        // The leader takes it now: the next pass writes the one marker still missing, and completes the transaction.
        _cluster.Refusing.Clear();
        await _coordinator.ExpireTransactionsAsync(DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);

        Assert.Equal(0, Assert.Single(_cluster.Calls[1]).Partition);
        Assert.Equal("CompleteCommit", State("tx-2"));
    }

    [Fact]
    public async Task ATransportFaultLeavesTheTransactionPreparedForTheNextPass()
    {
        _cluster.Throw = true;
        var (pid, epoch) = await StartTxn("tx-3", 0);

        var result = await EndAsync("tx-3", pid, epoch, commit: false);

        Assert.Equal(TxnErrorStatus.None, result.Status);
        Assert.Equal("PrepareAbort", State("tx-3"));

        _cluster.Throw = false;
        await _coordinator.ExpireTransactionsAsync(DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);
        Assert.Equal("CompleteAbort", State("tx-3"));
    }

    [Fact]
    public async Task FencingAProducerWhoseAbortALeaderRefusesAnswersConcurrentTransactions()
    {
        _cluster.Refusing.Add(0);
        await StartTxn("tx-4", 0);

        // A new incarnation fences the ongoing transaction; its abort is not complete, so the producer retries.
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = "tx-4", TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            TestContext.Current.CancellationToken);
        Assert.Equal(TxnErrorStatus.ConcurrentTransactions, init.Status);
        Assert.Equal("PrepareAbort", State("tx-4"));

        _cluster.Refusing.Clear();
        await _coordinator.ExpireTransactionsAsync(DateTimeOffset.UtcNow, TestContext.Current.CancellationToken);
        var retried = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = "tx-4", TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            TestContext.Current.CancellationToken);
        Assert.Equal(TxnErrorStatus.None, retried.Status);
    }

    private long NextOffset(int partition) => _logManager.GetLog(new TopicPartition { Topic = Topic, Partition = partition })!.NextOffset;

    private string State(string txnId) => _coordinator.DescribeTransactions([txnId]).Single().State;

    private Task<EndTxnResult> EndAsync(string txnId, long pid, short epoch, bool commit) =>
        _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = txnId, ProducerId = pid, ProducerEpoch = epoch, Committed = commit },
            TestContext.Current.CancellationToken);

    private async Task<(long ProducerId, short Epoch)> StartTxn(string txnId, params int[] partitions)
    {
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = txnId, TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            TestContext.Current.CancellationToken);

        await _coordinator.AddPartitionsToTxnAsync(new AddPartitionsToTxnCommand
        {
            TransactionalId = txnId,
            ProducerId = init.ProducerId,
            ProducerEpoch = init.ProducerEpoch,
            Topics = [new AddPartitionsTopic(Topic, partitions)],
        }, TestContext.Current.CancellationToken);

        return (init.ProducerId, init.ProducerEpoch);
    }
}
