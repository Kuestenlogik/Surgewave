using System.Text.Json;
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
/// Tests for TransactionStateSynchronizer.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class TransactionStateSynchronizerTests : IAsyncDisposable
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

    public TransactionStateSynchronizerTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"surgewave-sync-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);

        _logManager = new LogManager(_testDirectory, new MemoryLogSegmentFactory());
        _producerStateManager = new ProducerStateManager();
        _transactionIndex = new TransactionIndex();
        _offsetStore = new OffsetStore(_testDirectory, NullLogger<OffsetStore>.Instance);
        _stateStore = new TransactionStateStore(Path.Combine(_testDirectory, "txn-state"), NullLogger<TransactionStateStore>.Instance);
        _clusterState = new ClusterState();
        _connectionPool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        // Register brokers
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");

        _coordinator = new ClusteredTransactionCoordinator(
            _producerStateManager,
            _logManager,
            _transactionIndex,
            _offsetStore,
            _stateStore,
            _clusterState,
            new TransactionMarkerReplicator(_connectionPool, _clusterState, 1, NullLogger<TransactionMarkerReplicator>.Instance),
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
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public void ExportTransactionState_NoTransactions_ReturnsEmptyArray()
    {
        // Arrange
        var synchronizer = CreateSynchronizer();

        // Act
        var exported = synchronizer.ExportTransactionState();

        // Assert
        Assert.NotNull(exported);
        var snapshots = JsonSerializer.Deserialize<List<TransactionStateSnapshot>>(exported);
        Assert.Empty(snapshots!);
    }

    [Fact]
    public async Task ExportTransactionState_WithTransactions_ExportsAll()
    {
        // Arrange
        var synchronizer = CreateSynchronizer();

        // Create some transactions
        for (int i = 0; i < 3; i++)
        {
            var request = new InitProducerIdCommand
            {
                TransactionalId = $"export-txn-{i}",
                TransactionTimeoutMs = 60000,
                ProducerId = -1,
                ProducerEpoch = -1
            };
            await _coordinator.InitProducerIdAsync(request, CancellationToken.None);
        }

        // Act
        var exported = synchronizer.ExportTransactionState();

        // Assert
        var snapshots = JsonSerializer.Deserialize<List<TransactionStateSnapshot>>(exported);
        Assert.NotNull(snapshots);
        Assert.Equal(3, snapshots.Count);
        Assert.Contains(snapshots, s => s.TransactionalId == "export-txn-0");
        Assert.Contains(snapshots, s => s.TransactionalId == "export-txn-1");
        Assert.Contains(snapshots, s => s.TransactionalId == "export-txn-2");
    }

    [Fact]
    public async Task ExportTransactionState_WithPartitions_IncludesPartitions()
    {
        // Arrange
        var synchronizer = CreateSynchronizer();

        var initRequest = new InitProducerIdCommand
        {
            TransactionalId = "txn-with-parts",
            TransactionTimeoutMs = 60000,
            ProducerId = -1,
            ProducerEpoch = -1
        };
        var initResponse = await _coordinator.InitProducerIdAsync(initRequest, CancellationToken.None);

        var addRequest = new AddPartitionsToTxnCommand
        {
            TransactionalId = "txn-with-parts",
            ProducerId = initResponse.ProducerId,
            ProducerEpoch = initResponse.ProducerEpoch,
            Topics =
            [
                new AddPartitionsTopic("topic-x", [0, 1]),
                new AddPartitionsTopic("topic-y", [2]),
            ]
        };
        _coordinator.AddPartitionsToTxn(addRequest);

        // Act
        var exported = synchronizer.ExportTransactionState();

        // Assert
        var snapshots = JsonSerializer.Deserialize<List<TransactionStateSnapshot>>(exported);
        var snapshot = snapshots!.First(s => s.TransactionalId == "txn-with-parts");
        Assert.Equal(3, snapshot.Partitions.Count);
    }

    [Fact]
    public async Task SyncFromPreviousCoordinatorAsync_SameBroker_ReturnsTrue()
    {
        // Arrange
        var synchronizer = CreateSynchronizer();

        // Act - Syncing from self should succeed immediately
        var result = await synchronizer.SyncFromPreviousCoordinatorAsync(
            previousCoordinatorId: 1, // Same as local broker
            CancellationToken.None);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task SyncFromPreviousCoordinatorAsync_UnknownBroker_ReturnsFalse()
    {
        // Arrange
        var synchronizer = CreateSynchronizer();

        // Act
        var result = await synchronizer.SyncFromPreviousCoordinatorAsync(
            previousCoordinatorId: 999, // Non-existent broker
            CancellationToken.None);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void TransactionStateSynchronizerOptions_DefaultValues()
    {
        // Act
        var options = new TransactionStateSynchronizerOptions();

        // Assert
        Assert.Equal(30000, options.SyncTimeoutMs);
        Assert.Equal(100 * 1024 * 1024, options.MaxResponseSizeBytes);
    }

    [Fact]
    public void TransactionStateSynchronizerOptions_CustomValues()
    {
        // Act
        var options = new TransactionStateSynchronizerOptions
        {
            SyncTimeoutMs = 60000,
            MaxResponseSizeBytes = 200 * 1024 * 1024
        };

        // Assert
        Assert.Equal(60000, options.SyncTimeoutMs);
        Assert.Equal(200 * 1024 * 1024, options.MaxResponseSizeBytes);
    }

    [Fact]
    public void TransactionStateSnapshot_Serialization_RoundTrips()
    {
        // Arrange
        var snapshot = new TransactionStateSnapshot
        {
            TransactionalId = "test-snap",
            ProducerId = 12345,
            ProducerEpoch = 3,
            State = "Ongoing",
            TransactionTimeoutMs = 60000,
            LastActivityTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Partitions =
            [
                new Kuestenlogik.Surgewave.Broker.Transactions.PartitionSnapshot("topic-a", 0),
                new Kuestenlogik.Surgewave.Broker.Transactions.PartitionSnapshot("topic-a", 1)
            ],
            ConsumerGroups = ["group-1", "group-2"],
            PendingOffsets =
            [
                new OffsetSnapshot("group-1", "topic-a", 0, 100, "metadata")
            ]
        };

        // Act
        var json = JsonSerializer.Serialize(snapshot);
        var deserialized = JsonSerializer.Deserialize<TransactionStateSnapshot>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(snapshot.TransactionalId, deserialized.TransactionalId);
        Assert.Equal(snapshot.ProducerId, deserialized.ProducerId);
        Assert.Equal(snapshot.ProducerEpoch, deserialized.ProducerEpoch);
        Assert.Equal(snapshot.State, deserialized.State);
        Assert.Equal(2, deserialized.Partitions.Count);
        Assert.Equal(2, deserialized.ConsumerGroups.Count);
        Assert.Single(deserialized.PendingOffsets);
    }

    [Fact]
    public void PartitionSnapshot_RecordStruct()
    {
        // Act
        var snapshot = new Kuestenlogik.Surgewave.Broker.Transactions.PartitionSnapshot("my-topic", 5);

        // Assert
        Assert.Equal("my-topic", snapshot.Topic);
        Assert.Equal(5, snapshot.Partition);
    }

    [Fact]
    public void OffsetSnapshot_RecordStruct()
    {
        // Act
        var snapshot = new OffsetSnapshot("group", "topic", 0, 999, "meta");

        // Assert
        Assert.Equal("group", snapshot.GroupId);
        Assert.Equal("topic", snapshot.Topic);
        Assert.Equal(0, snapshot.Partition);
        Assert.Equal(999, snapshot.Offset);
        Assert.Equal("meta", snapshot.Metadata);
    }

    [Fact]
    public void OffsetSnapshot_NullMetadata()
    {
        // Act
        var snapshot = new OffsetSnapshot("group", "topic", 0, 999, null);

        // Assert
        Assert.Null(snapshot.Metadata);
    }

    [Fact]
    public void ExportTransactionState_NoCoordinator_ReturnsEmpty()
    {
        // Arrange - Create synchronizer without coordinator
        var synchronizer = new TransactionStateSynchronizer(
            _connectionPool,
            _clusterState,
            coordinator: null,
            localBrokerId: 1,
            NullLogger<TransactionStateSynchronizer>.Instance);

        // Act
        var exported = synchronizer.ExportTransactionState();

        // Assert
        Assert.Empty(exported);
    }

    [Fact]
    public async Task SyncFromPreviousCoordinatorAsync_NoCoordinator_ReturnsFalse()
    {
        // Arrange - Create synchronizer without coordinator
        var synchronizer = new TransactionStateSynchronizer(
            _connectionPool,
            _clusterState,
            coordinator: null,
            localBrokerId: 1,
            NullLogger<TransactionStateSynchronizer>.Instance);

        // Act
        var result = await synchronizer.SyncFromPreviousCoordinatorAsync(2, CancellationToken.None);

        // Assert
        Assert.False(result);
    }

    private TransactionStateSynchronizer CreateSynchronizer(TransactionStateSynchronizerOptions? options = null)
    {
        return new TransactionStateSynchronizer(
            _connectionPool,
            _clusterState,
            _coordinator,
            localBrokerId: 1,
            NullLogger<TransactionStateSynchronizer>.Instance,
            options);
    }
}
