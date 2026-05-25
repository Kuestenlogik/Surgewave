using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Tests for ClusteredTransactionCoordinator.
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

    [Fact]
    public async Task HandleInitProducerIdAsync_NonTransactional_AllocatesProducerId()
    {
        // Arrange
        var request = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = null,
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };

        // Act
        var response = await _coordinator.HandleInitProducerIdAsync(request, CancellationToken.None);

        // Assert
        Assert.Equal(ErrorCode.None, response.ErrorCode);
        Assert.True(response.ProducerId >= 0);
        Assert.True(response.ProducerEpoch >= 0);
    }

    [Fact]
    public async Task HandleInitProducerIdAsync_Transactional_AllocatesProducerIdAndTracksTransaction()
    {
        // Arrange
        var request = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "test-txn-id",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };

        // Act
        var response = await _coordinator.HandleInitProducerIdAsync(request, CancellationToken.None);

        // Assert
        Assert.Equal(ErrorCode.None, response.ErrorCode);
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
        // Arrange
        var request = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "persistent-txn",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };

        // Act
        var response1 = await _coordinator.HandleInitProducerIdAsync(request, CancellationToken.None);
        var response2 = await _coordinator.HandleInitProducerIdAsync(request, CancellationToken.None);

        // Assert
        Assert.Equal(response1.ProducerId, response2.ProducerId);
        // Epoch may start fresh for each allocation (implementation-specific)
        Assert.True(response2.ProducerEpoch >= 0);
    }

    [Fact]
    public void HandleAddPartitionsToTxn_UnknownTransactionalId_ReturnsError()
    {
        // Arrange
        var request = new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "unknown-txn",
            ProducerId = 12345,
            ProducerEpoch = 0,
            Topics = new Dictionary<string, List<int>>
            {
                ["test-topic"] = [0, 1]
            }
        };

        // Act
        var response = _coordinator.HandleAddPartitionsToTxn(request);

        // Assert
        Assert.All(response.Results.Values.SelectMany(p => p),
            p => Assert.Equal(ErrorCode.UnknownProducerId, p.ErrorCode));
    }

    [Fact]
    public async Task HandleAddPartitionsToTxn_ValidTransaction_AddsPartitions()
    {
        // Arrange - First init the producer
        var initRequest = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "add-parts-txn",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };
        var initResponse = await _coordinator.HandleInitProducerIdAsync(initRequest, CancellationToken.None);

        var addRequest = new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = 3,
            CorrelationId = 2,
            ClientId = "test-client",
            TransactionalId = "add-parts-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            Topics = new Dictionary<string, List<int>>
            {
                ["topic-a"] = [0, 1],
                ["topic-b"] = [0]
            }
        };

        // Act
        var response = _coordinator.HandleAddPartitionsToTxn(addRequest);

        // Assert
        Assert.All(response.Results.Values.SelectMany(p => p),
            p => Assert.Equal(ErrorCode.None, p.ErrorCode));

        // Verify transaction state includes partitions
        var descriptions = _coordinator.DescribeTransactions(["add-parts-txn"]);
        Assert.Single(descriptions);
        Assert.Equal(3, descriptions[0].Partitions.Count);
    }

    [Fact]
    public async Task HandleAddOffsetsToTxn_ValidTransaction_AddsConsumerGroup()
    {
        // Arrange
        var initRequest = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "offsets-txn",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };
        var initResponse = await _coordinator.HandleInitProducerIdAsync(initRequest, CancellationToken.None);

        var addOffsetsRequest = new AddOffsetsToTxnRequest
        {
            ApiKey = ApiKey.AddOffsetsToTxn,
            ApiVersion = 3,
            CorrelationId = 2,
            ClientId = "test-client",
            TransactionalId = "offsets-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            GroupId = "my-consumer-group"
        };

        // Act
        var response = _coordinator.HandleAddOffsetsToTxn(addOffsetsRequest);

        // Assert
        Assert.Equal(ErrorCode.None, response.ErrorCode);
    }

    [Fact]
    public void HandleAddOffsetsToTxn_UnknownTransaction_ReturnsError()
    {
        // Arrange
        var request = new AddOffsetsToTxnRequest
        {
            ApiKey = ApiKey.AddOffsetsToTxn,
            ApiVersion = 3,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "unknown-txn",
            ProducerId = 12345,
            ProducerEpoch = 0,
            GroupId = "my-group"
        };

        // Act
        var response = _coordinator.HandleAddOffsetsToTxn(request);

        // Assert
        Assert.Equal(ErrorCode.UnknownProducerId, response.ErrorCode);
    }

    [Fact]
    public async Task ListTransactions_NoFilter_ReturnsAllTransactions()
    {
        // Arrange - Create multiple transactions
        for (int i = 0; i < 3; i++)
        {
            var request = new InitProducerIdRequest
            {
                ApiKey = ApiKey.InitProducerId,
                ApiVersion = 4,
                CorrelationId = i,
                ClientId = "test-client",
                TransactionalId = $"txn-{i}",
                TransactionTimeoutMs = 60000,
                ProducerId = -1,
                ProducerEpoch = -1
            };
            await _coordinator.HandleInitProducerIdAsync(request, CancellationToken.None);
        }

        // Act
        var transactions = _coordinator.ListTransactions();

        // Assert
        Assert.Equal(3, transactions.Count);
        Assert.Contains(transactions, t => t.TransactionalId == "txn-0");
        Assert.Contains(transactions, t => t.TransactionalId == "txn-1");
        Assert.Contains(transactions, t => t.TransactionalId == "txn-2");
    }

    [Fact]
    public async Task DescribeTransactions_ExistingTransaction_ReturnsDetails()
    {
        // Arrange
        var initRequest = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "describe-txn",
            TransactionTimeoutMs = 45000,
            ProducerId = -1,
            ProducerEpoch = -1
        };
        var initResponse = await _coordinator.HandleInitProducerIdAsync(initRequest, CancellationToken.None);

        // Act
        var descriptions = _coordinator.DescribeTransactions(["describe-txn"]);

        // Assert
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
        // Act
        var descriptions = _coordinator.DescribeTransactions(["non-existent-txn"]);

        // Assert
        Assert.Single(descriptions);
        Assert.Equal("Unknown", descriptions[0].State);
        Assert.Equal(59, descriptions[0].ErrorCode); // UNKNOWN_PRODUCER_ID
    }

    [Fact]
    public void CoordinatorEpoch_CanBeSetAndRetrieved()
    {
        // Act
        _coordinator.CoordinatorEpoch = 42;

        // Assert
        Assert.Equal(42, _coordinator.CoordinatorEpoch);
    }

    [Fact]
    public void TransactionIndex_IsExposed()
    {
        // Assert
        Assert.NotNull(_coordinator.TransactionIndex);
        Assert.Same(_transactionIndex, _coordinator.TransactionIndex);
    }

    [Fact]
    public void ValidateProduceBatch_ValidSequence_ReturnsNone()
    {
        // Arrange
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };
        var (producerId, epoch) = _producerStateManager.AllocateProducerId();

        // First batch with sequence 0
        var result1 = _coordinator.ValidateProduceBatch(producerId, epoch, baseSequence: 0, tp);

        // Assert
        Assert.Equal(ErrorCode.None, result1);
    }

    [Fact]
    public async Task RecordTransactionalBatch_TracksPartition()
    {
        // Arrange
        var initRequest = new InitProducerIdRequest
        {
            ApiKey = ApiKey.InitProducerId,
            ApiVersion = 4,
            CorrelationId = 1,
            ClientId = "test-client",
            TransactionalId = "batch-track-txn",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };
        var initResponse = await _coordinator.HandleInitProducerIdAsync(initRequest, CancellationToken.None);

        // Add partition first
        var addRequest = new AddPartitionsToTxnRequest
        {
            ApiKey = ApiKey.AddPartitionsToTxn,
            ApiVersion = 3,
            CorrelationId = 2,
            ClientId = "test-client",
            TransactionalId = "batch-track-txn",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            Topics = new Dictionary<string, List<int>> { ["tracked-topic"] = [0] }
        };
        _coordinator.HandleAddPartitionsToTxn(addRequest);

        var tp = new TopicPartition { Topic = "tracked-topic", Partition = 0 };

        // Act
        _coordinator.RecordTransactionalBatch(tp, initResponse.ProducerId, baseOffset: 100);

        // Assert - Transaction should track the partition
        var descriptions = _coordinator.DescribeTransactions(["batch-track-txn"]);
        Assert.Contains(descriptions[0].Partitions, p => p.Topic == "tracked-topic" && p.Partition == 0);
    }
}
