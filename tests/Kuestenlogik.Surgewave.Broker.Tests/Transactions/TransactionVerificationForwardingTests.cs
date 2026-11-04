using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// A partition leader whose broker does not coordinate the transactional id asks the coordinator that does (#197),
/// as Kafka's leaders send AddPartitionsToTxn v4+; it used to answer NOT_COORDINATOR, which the produce path maps to
/// the retriable NOT_ENOUGH_REPLICAS, so a transactional batch to a partition led away from the coordinator was never
/// appended. A coordinator asked over the inter-broker channel answers for itself and never asks on.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionVerificationForwardingTests : IAsyncDisposable
{
    private static readonly TopicPartition Orders = new() { Topic = "orders", Partition = 1 };

    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"surgewave-verify-{Guid.NewGuid():N}");
    private readonly LogManager _logManager;
    private readonly OffsetStore _offsetStore = OffsetStore.InMemory();
    private readonly TransactionCoordinator _coordinator;
    private readonly RecordingVerifier _remote = new();

    public TransactionVerificationForwardingTests()
    {
        Directory.CreateDirectory(_dir);
        _logManager = new LogManager(_dir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);

        // A state log of a broker in a cluster that leads none of __transaction_state's partitions.
        var stateLog = new TransactionStateLog(_logManager, new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance), 3, NullLogger<TransactionStateLog>.Instance);
        _coordinator = new TransactionCoordinator(
            new ProducerStateManager(), _logManager, new TransactionIndex(), _offsetStore, stateLog,
            NullLogger<TransactionCoordinator>.Instance);
        _coordinator.JoinCluster(markerReplicator: null, commitGate: null, _remote);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _offsetStore.Dispose();
        _logManager.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task ALeaderAsksTheCoordinatorOfAnIdItDoesNotCoordinate()
    {
        _remote.Answer = TxnErrorStatus.None;

        var status = await _coordinator.VerifyTransactionalBatchAsync("tx-1", 7, 0, Orders, TransactionVersion.Tv2, TestContext.Current.CancellationToken);

        Assert.Equal(TxnErrorStatus.None, status);
        Assert.Equal(("tx-1", Orders), Assert.Single(_remote.Asked));
    }

    [Fact]
    public async Task ACoordinatorAskedByAnotherBrokerAnswersForItselfOnly()
    {
        ITransactionPartitionVerifier asked = _coordinator;

        var status = await asked.VerifyPartitionAsync("tx-1", 7, 0, Orders, TransactionVersion.Tv2, TestContext.Current.CancellationToken);

        Assert.Equal(TxnErrorStatus.NotCoordinator, status);
        Assert.Empty(_remote.Asked);
    }

    private sealed class RecordingVerifier : ITransactionPartitionVerifier
    {
        public TxnErrorStatus Answer { get; set; }

        public List<(string, TopicPartition)> Asked { get; } = [];

        public Task<TxnErrorStatus> VerifyPartitionAsync(
            string transactionalId, long producerId, short producerEpoch, TopicPartition partition, TransactionVersion version,
            CancellationToken cancellationToken)
        {
            Asked.Add((transactionalId, partition));
            return Task.FromResult(Answer);
        }
    }
}
