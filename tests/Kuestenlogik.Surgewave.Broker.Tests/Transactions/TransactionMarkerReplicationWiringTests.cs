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
/// #72 Inc7 — the LIVE TransactionCoordinator replicates commit/abort markers to follower brokers
/// (best-effort) after writing them locally, via the wired ITransactionMarkerReplicator (merged from
/// the deleted ClusteredTransactionCoordinator). A replication failure must NOT fail the transaction —
/// the markers are already durable in the local log.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionMarkerReplicationWiringTests : IAsyncDisposable
{
    private sealed class RecordingReplicator : ITransactionMarkerReplicator
    {
        public readonly List<(string TxnId, bool Commit, int PartitionCount)> Calls = [];
        public bool ReturnFailure;
        public bool Throw;

        public Task<MarkerReplicationResult> ReplicateMarkersAsync(
            string transactionalId, long producerId, short producerEpoch,
            IReadOnlyList<TopicPartition> partitions, bool commit, int coordinatorEpoch, CancellationToken ct)
        {
            Calls.Add((transactionalId, commit, partitions.Count));
            if (Throw)
                throw new InvalidOperationException("simulated transport fault");
            return Task.FromResult(ReturnFailure
                ? new MarkerReplicationResult { IsSuccess = false }
                : MarkerReplicationResult.Success());
        }
    }

    private readonly string _dir;
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly TransactionCoordinator _coordinator;
    private readonly RecordingReplicator _replicator = new();

    public TransactionMarkerReplicationWiringTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"surgewave-inc7-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _logManager = new LogManager(_dir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _offsetStore = new OffsetStore(_dir, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_dir, "txn-state"), NullLogger<TransactionStateStore>.Instance);
        _coordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, _stateStore,
            NullLogger<TransactionCoordinator>.Instance);
        _coordinator.SetMarkerReplicator(_replicator);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _stateStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private async Task<(long ProducerId, short Epoch)> StartTxnWithPartition(string txnId)
    {
        var init = await _coordinator.InitProducerIdAsync(
            new InitProducerIdCommand { TransactionalId = txnId, TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1 },
            CancellationToken.None);

        _coordinator.AddPartitionsToTxn(new AddPartitionsToTxnCommand
        {
            TransactionalId = txnId,
            ProducerId = init.ProducerId,
            ProducerEpoch = init.ProducerEpoch,
            Topics = [new AddPartitionsTopic("orders", [0])],
        });

        return (init.ProducerId, init.ProducerEpoch);
    }

    [Fact]
    public async Task EndTxnCommit_ReplicatesMarkersToFollowers()
    {
        var (pid, epoch) = await StartTxnWithPartition("tx-1");

        var result = await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = "tx-1", ProducerId = pid, ProducerEpoch = epoch, Committed = true },
            CancellationToken.None);

        Assert.Equal(TxnErrorStatus.None, result.Status);
        var call = Assert.Single(_replicator.Calls);
        Assert.Equal(("tx-1", true, 1), call);
    }

    [Fact]
    public async Task EndTxnAbort_ReplicatesAbortMarkers()
    {
        var (pid, epoch) = await StartTxnWithPartition("tx-2");

        await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = "tx-2", ProducerId = pid, ProducerEpoch = epoch, Committed = false },
            CancellationToken.None);

        var call = Assert.Single(_replicator.Calls);
        Assert.False(call.Commit);
    }

    [Fact]
    public async Task EndTxnCommit_ReplicationFailure_DoesNotFailTheTransaction()
    {
        _replicator.ReturnFailure = true;
        var (pid, epoch) = await StartTxnWithPartition("tx-3");

        var result = await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = "tx-3", ProducerId = pid, ProducerEpoch = epoch, Committed = true },
            CancellationToken.None);

        // Best-effort: the commit still succeeds because the markers are durable in the local log.
        Assert.Equal(TxnErrorStatus.None, result.Status);
        Assert.Single(_replicator.Calls);
    }

    [Fact]
    public async Task EndTxnCommit_ReplicatorThrows_DoesNotFailTheTransaction()
    {
        _replicator.Throw = true;
        var (pid, epoch) = await StartTxnWithPartition("tx-4");

        // A transport throw from the replicator must not escape EndTxn — the local markers are durable.
        var result = await _coordinator.EndTxnAsync(
            new EndTxnCommand { TransactionalId = "tx-4", ProducerId = pid, ProducerEpoch = epoch, Committed = true },
            CancellationToken.None);

        Assert.Equal(TxnErrorStatus.None, result.Status);
    }
}
