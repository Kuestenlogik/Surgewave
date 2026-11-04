using Kuestenlogik.Surgewave.Broker.Coordination;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Transaction state in <c>__transaction_state</c> (#197): the coordinator of a transactional id is the broker that
/// leads its partition, a broker that does not answers NOT_COORDINATOR (KAFKA-21181), and one that takes the
/// partition over carries on with the transactions the previous coordinator wrote.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransactionStateLogTests : IAsyncDisposable
{
    private const int Partitions = 3;
    private const string Id = "orders-tx";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "surgewave-txn-state-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _log;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly List<TransactionCoordinator> _coordinators = [];
    private readonly CancellationToken _ct = TestContext.Current.CancellationToken;

    public TransactionStateLogTests()
    {
        _log = new LogManager(_dir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var coordinator in _coordinators)
            await coordinator.DisposeAsync();
        _log.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void ARecordCarriesEverythingATakeoverNeeds()
    {
        var txn = new TransactionMetadata
        {
            TransactionalId = Id,
            ProducerId = 7L << 40 | 12,
            ProducerEpoch = 4,
            LastProducerEpoch = 3,
            PreviousProducerId = 5,
            NextProducerId = 9,
            ClientTransactionVersion = TransactionVersion.Tv2,
            State = TransactionState.Ongoing,
            TransactionTimeoutMs = 60_000,
            TxnStartTime = DateTimeOffset.FromUnixTimeMilliseconds(1_000),
            LastActivityTime = DateTimeOffset.FromUnixTimeMilliseconds(2_000),
            Partitions = { new TopicPartition { Topic = "orders", Partition = 0 }, new TopicPartition { Topic = "orders", Partition = 2 } },
            ConsumerGroups = { "billing" },
        };

        // Kafka's TransactionLogKey v0: version, transactional id.
        var key = TransactionRecords.Key(Id);
        Assert.Equal([0, 0, 0, 9, .. "orders-tx"u8.ToArray()], key);

        var read = TransactionRecords.ReadValue(TransactionRecords.ReadKey(key)!, TransactionRecords.Value(txn));

        Assert.Equal((txn.ProducerId, txn.ProducerEpoch, txn.LastProducerEpoch, txn.PreviousProducerId, txn.NextProducerId),
            (read.ProducerId, read.ProducerEpoch, read.LastProducerEpoch, read.PreviousProducerId, read.NextProducerId));
        Assert.Equal((txn.State, txn.TransactionTimeoutMs, txn.ClientTransactionVersion), (read.State, read.TransactionTimeoutMs, read.ClientTransactionVersion));
        Assert.Equal((txn.TxnStartTime, txn.LastActivityTime), (read.TxnStartTime, read.LastActivityTime));
        Assert.Equal(txn.Partitions.OrderBy(p => p.Partition), read.Partitions.OrderBy(p => p.Partition));
        Assert.Equal(txn.ConsumerGroups, read.ConsumerGroups);
    }

    [Fact]
    public async Task ABrokerThatDoesNotLeadTheIdsPartitionIsNotItsCoordinator()
    {
        var (coordinator, _) = Coordinator(); // follows nothing: no partition is led here

        var init = await coordinator.InitProducerIdAsync(Init(), _ct);
        var add = await coordinator.AddPartitionsToTxnAsync(Add(1, 0), _ct);
        var end = await coordinator.EndTxnAsync(new EndTxnCommand { TransactionalId = Id, ProducerId = 1, ProducerEpoch = 0, Committed = true }, _ct);

        Assert.Equal(TxnErrorStatus.NotCoordinator, init.Status);
        Assert.Equal(TxnErrorStatus.NotCoordinator, add.Topics[0].Partitions[0].Status);
        Assert.Equal(TxnErrorStatus.NotCoordinator, end.Status);
    }

    [Fact]
    public async Task ABrokerThatTakesThePartitionOverFencesTheProducerOfTheOngoingTransaction()
    {
        // The first coordinator: the producer starts a transaction and adds a partition.
        var (first, firstLog) = Coordinator();
        await firstLog.StartLocalAsync(_ct);
        var init = await first.InitProducerIdAsync(Init(), _ct);
        Assert.Equal(TxnErrorStatus.None, (await first.AddPartitionsToTxnAsync(Add(init.ProducerId, init.ProducerEpoch), _ct)).Topics[0].Partitions[0].Status);

        // It loses the id's partition; the next broker reads it.
        var partition = CoordinatorPartitioner.PartitionFor(Id, Partitions);
        firstLog.OnLeadershipChanged(partition, isLeader: false);
        var (next, nextLog) = Coordinator();
        nextLog.OnLeadershipChanged(partition, isLeader: true);
        await nextLog.WhenLoadedAsync(Id).WaitAsync(TimeSpan.FromSeconds(30), _ct);

        // The previous coordinator answers for the id no more; the next one knows the producer and its transaction.
        Assert.Equal(TxnErrorStatus.NotCoordinator, (await first.InitProducerIdAsync(Init(), _ct)).Status);
        var described = Assert.Single(next.DescribeTransactions([Id]));
        Assert.Equal(("Ongoing", init.ProducerId, init.ProducerEpoch), (described.State, described.ProducerId, described.ProducerEpoch));

        // A new producer for the id aborts the ongoing transaction and fences the old one with the next epoch.
        var reinit = await next.InitProducerIdAsync(Init(), _ct);
        Assert.Equal((TxnErrorStatus.None, init.ProducerId, (short)(init.ProducerEpoch + 1)), (reinit.Status, reinit.ProducerId, reinit.ProducerEpoch));
        var zombie = await next.AddPartitionsToTxnAsync(Add(init.ProducerId, init.ProducerEpoch), _ct);
        Assert.Equal(TxnErrorStatus.ProducerFenced, zombie.Topics[0].Partitions[0].Status);
    }

    private (TransactionCoordinator Coordinator, TransactionStateLog Log) Coordinator()
    {
        var log = new TransactionStateLog(_log, _serializer, Partitions, NullLogger<TransactionStateLog>.Instance);
        var coordinator = new TransactionCoordinator(
            new ProducerStateManager(), _log, new TransactionIndex(), OffsetStore.InMemory(), log,
            NullLogger<TransactionCoordinator>.Instance);
        _coordinators.Add(coordinator);
        return (coordinator, log);
    }

    private static InitProducerIdCommand Init() => new()
    {
        TransactionalId = Id, TransactionTimeoutMs = 60_000, ProducerId = -1, ProducerEpoch = -1,
    };

    private static AddPartitionsToTxnCommand Add(long producerId, short epoch) => new()
    {
        TransactionalId = Id, ProducerId = producerId, ProducerEpoch = epoch, Topics = [new AddPartitionsTopic("orders", [0])],
    };
}
