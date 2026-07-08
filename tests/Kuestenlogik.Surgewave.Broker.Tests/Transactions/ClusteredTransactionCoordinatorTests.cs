using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Tests for ClusteredTransactionCoordinator. Exercises the protocol-neutral coordinator
/// surface directly (#59); ValidateProduceBatch stays on the Kafka ErrorCode (produce path).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class ClusteredTransactionCoordinatorTests : IAsyncDisposable
{
    private readonly string _testDirectory;
    private readonly LogManager _logManager;
    private readonly ProducerStateManager _producerStateManager;
    private readonly TransactionIndex _transactionIndex;
    private readonly OffsetStore _offsetStore;
    private readonly TransactionStateStore _stateStore;
    private readonly ClusterState _clusterState;
    private readonly ConnectionPool _connectionPool;
    private readonly ClusteredTransactionCoordinator _coordinator;

    public ClusteredTransactionCoordinatorTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
        _producerStateManager = new ProducerStateManager();
        _transactionIndex = new TransactionIndex();
        _offsetStore = new OffsetStore(_testDirectory, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_testDirectory, "txn-state"), NullLogger<TransactionStateStore>.Instance);
        _clusterState = new ClusterState();
        _connectionPool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        // Register local broker
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");

        _coordinator = new ClusteredTransactionCoordinator(
            _producerStateManager,
            _logManager,
            _transactionIndex,
            _offsetStore,
            _stateStore,
            _clusterState,
            _connectionPool,
            localBrokerId: 1,
            NullLoggerFactory.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _coordinator.DisposeAsync();
        _connectionPool.Dispose();
        _offsetStore.Dispose();
        _stateStore.Dispose();
        _logManager.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }
    }

    private static InitProducerIdCommand InitCommand(string? txnId, int timeoutMs = 60000, long producerId = -1, short producerEpoch = -1)
        => new()
        {
            TransactionalId = txnId,
            TransactionTimeoutMs = timeoutMs,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
        };

    [Fact]
    public async Task HandleInitProducerIdAsync_NonTransactional_AllocatesProducerId()
    {
        var response = await _coordinator.InitProducerIdAsync(InitCommand(null), CancellationToken.None);

        Assert.Equal(TxnErrorStatus.None, response.Status);
        Assert.True(response.ProducerId >= 0);
        Assert.True(response.ProducerEpoch >= 0);
    }

    [Fact]
    public async Task HandleInitProducerIdAsync_Transactional_AllocatesProducerIdAndTracksTransaction()
    {
        var response = await _coordinator.InitProducerIdAsync(InitCommand("test-txn-id"), CancellationToken.None);

        Assert.Equal(TxnErrorStatus.None, response.Status);
        Assert.True(response.ProducerId >= 0);
        Assert.True(response.ProducerEpoch >= 0);

        // Verify transaction is listed
        var transactions = _coordinator.ListTransactions();
        Assert.Single(transactions);
        Assert.Equal("test-txn-id", transactions[0].TransactionalId);
    }

    [Fact]
    public async Task HandleInitProducerIdAsync_SameTransactionalId_ReturnsSameProducerId()
    {
        var response1 = await _coordinator.InitProducerIdAsync(InitCommand("persistent-txn"), CancellationToken.None);
        var response2 = await _coordinator.InitProducerIdAsync(InitCommand("persistent-txn"), CancellationToken.None);

        Assert.Equal(response1.ProducerId, response2.ProducerId);
        // Epoch may start fresh for each allocation (implementation-specific)
        Assert.True(response2.ProducerEpoch >= 0);
    }

    [Fact]
    public void HandleAddPartitionsToTxn_UnknownTransactionalId_ReturnsError()
    {
        var request = new AddPartitionsToTxnCommand
        {
            TransactionalId = "unknown-txn",
            ProducerId = 12345,
            ProducerEpoch = 0,
            Topics = [new AddPartitionsTopic("test-topic", [0, 1])]
        };

        var response = _coordinator.AddPartitionsToTxn(request);

        Assert.All(response.Topics.SelectMany(t => t.Partitions),
            p => Assert.Equal(TxnErrorStatus.UnknownProducerId, p.Status));
    }

    [Fact]
    public async Task HandleAddPartitionsToTxn_ValidTransaction_AddsPartitions()
    {
        // Arrange - First init the producer
        var initResponse = await _coordinator.InitProducerIdAsync(InitCommand("add-parts-txn"), CancellationToken.None);

        var addRequest = new AddPartitionsToTxnCommand
        {
            TransactionalId = "add-parts-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            Topics =
            [
                new AddPartitionsTopic("topic-a", [0, 1]),
                new AddPartitionsTopic("topic-b", [0]),
            ]
        };

        var response = _coordinator.AddPartitionsToTxn(addRequest);

        Assert.All(response.Topics.SelectMany(t => t.Partitions),
            p => Assert.Equal(TxnErrorStatus.None, p.Status));

        // Verify transaction state includes partitions
        var descriptions = _coordinator.DescribeTransactions(["add-parts-txn"]);
        Assert.Single(descriptions);
        Assert.Equal(3, descriptions[0].Partitions.Count);
    }

    [Fact]
    public async Task HandleAddOffsetsToTxn_ValidTransaction_AddsConsumerGroup()
    {
        var initResponse = await _coordinator.InitProducerIdAsync(InitCommand("offsets-txn"), CancellationToken.None);

        var addOffsetsRequest = new AddOffsetsToTxnCommand
        {
            TransactionalId = "offsets-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            GroupId = "my-consumer-group"
        };

        var response = _coordinator.AddOffsetsToTxn(addOffsetsRequest);

        Assert.Equal(TxnErrorStatus.None, response.Status);
    }

    [Fact]
    public void HandleAddOffsetsToTxn_UnknownTransaction_ReturnsError()
    {
        var request = new AddOffsetsToTxnCommand
        {
            TransactionalId = "unknown-txn",
            ProducerId = 12345,
            ProducerEpoch = 0,
            GroupId = "my-group"
        };

        var response = _coordinator.AddOffsetsToTxn(request);

        Assert.Equal(TxnErrorStatus.UnknownProducerId, response.Status);
    }

    [Fact]
    public async Task ListTransactions_NoFilter_ReturnsAllTransactions()
    {
        // Arrange - Create multiple transactions
        for (int i = 0; i < 3; i++)
        {
            await _coordinator.InitProducerIdAsync(InitCommand($"txn-{i}"), CancellationToken.None);
        }

        var transactions = _coordinator.ListTransactions();

        Assert.Equal(3, transactions.Count);
        Assert.Contains(transactions, t => t.TransactionalId == "txn-0");
        Assert.Contains(transactions, t => t.TransactionalId == "txn-1");
        Assert.Contains(transactions, t => t.TransactionalId == "txn-2");
    }

    [Fact]
    public async Task DescribeTransactions_ExistingTransaction_ReturnsDetails()
    {
        var initResponse = await _coordinator.InitProducerIdAsync(InitCommand("describe-txn", timeoutMs: 45000), CancellationToken.None);

        var descriptions = _coordinator.DescribeTransactions(["describe-txn"]);

        Assert.Single(descriptions);
        var desc = descriptions[0];
        Assert.Equal("describe-txn", desc.TransactionalId);
        Assert.Equal(initResponse.ProducerId, desc.ProducerId);
        Assert.Equal(initResponse.ProducerEpoch, desc.ProducerEpoch);
        Assert.Equal(45000, desc.TransactionTimeoutMs);
        Assert.Equal(0, desc.ErrorCode);
    }

    [Fact]
    public void DescribeTransactions_UnknownTransaction_ReturnsErrorCode()
    {
        var descriptions = _coordinator.DescribeTransactions(["non-existent-txn"]);

        Assert.Single(descriptions);
        Assert.Equal("Unknown", descriptions[0].State);
        Assert.Equal(59, descriptions[0].ErrorCode); // UNKNOWN_PRODUCER_ID
    }

    [Fact]
    public void CoordinatorEpoch_CanBeSetAndRetrieved()
    {
        _coordinator.CoordinatorEpoch = 42;
        Assert.Equal(42, _coordinator.CoordinatorEpoch);
    }

    [Fact]
    public void TransactionIndex_IsExposed()
    {
        Assert.NotNull(_coordinator.TransactionIndex);
        Assert.Same(_transactionIndex, _coordinator.TransactionIndex);
    }

    [Fact]
    public void ValidateProduceBatch_ValidSequence_ReturnsNone()
    {
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var (producerId, epoch) = _producerStateManager.AllocateProducerId();

        // First batch with sequence 0 — produce path still returns the Kafka ErrorCode.
        var result1 = _coordinator.ValidateProduceBatch(producerId, epoch, baseSequence: 0, tp);

        Assert.Equal(ErrorCode.None, result1);
    }

    [Fact]
    public async Task RecordTransactionalBatch_TracksPartition()
    {
        var initResponse = await _coordinator.InitProducerIdAsync(InitCommand("batch-track-txn"), CancellationToken.None);

        // Add partition first
        var addRequest = new AddPartitionsToTxnCommand
        {
            TransactionalId = "batch-track-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            Topics = [new AddPartitionsTopic("tracked-topic", [0])]
        };
        _coordinator.AddPartitionsToTxn(addRequest);

        var tp = new TopicPartition { Topic = "tracked-topic", Partition = 0 };

        _coordinator.RecordTransactionalBatch(tp, initResponse.ProducerId, baseOffset: 100);

        // Assert - Transaction should track the partition
        var descriptions = _coordinator.DescribeTransactions(["batch-track-txn"]);
        Assert.Contains(descriptions[0].Partitions, p => p.Topic == "tracked-topic" && p.Partition == 0);
    }
}
