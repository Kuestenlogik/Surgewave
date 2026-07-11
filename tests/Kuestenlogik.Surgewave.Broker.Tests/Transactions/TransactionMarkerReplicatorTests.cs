using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Tests for TransactionMarkerReplicator.
/// Tests core logic without requiring actual network connections.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class TransactionMarkerReplicatorTests : IDisposable
{
    private readonly ClusterState _clusterState;
    private readonly ConnectionPool _connectionPool;
    private readonly int _localBrokerId = 1;

    public TransactionMarkerReplicatorTests()
    {
        _clusterState = new ClusterState();
        _connectionPool = new ConnectionPool(NullLogger<ConnectionPool>.Instance, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport());

        // Set up test brokers using individual parameters
        _clusterState.RegisterBroker(1, "localhost", 9092, "rack-1");
        _clusterState.RegisterBroker(2, "localhost", 9093, "rack-2");
        _clusterState.RegisterBroker(3, "localhost", 9094, "rack-3");
    }

    public void Dispose()
    {
        _connectionPool.Dispose();
    }

    [Fact]
    public async Task ReplicateMarkersAsync_EmptyPartitions_ReturnsSuccess()
    {
        // Arrange
        var replicator = CreateReplicator();
        var txnMetadata = CreateTransactionMetadata("test-txn", partitions: []);

        // Act
        var result = await replicator.ReplicateMarkersAsync(txnMetadata.TransactionalId, txnMetadata.ProducerId, txnMetadata.ProducerEpoch, [.. txnMetadata.Partitions], commit: true, coordinatorEpoch: 1, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Empty(result.SuccessfulBrokers);
        Assert.Empty(result.FailedBrokers);
    }

    [Fact]
    public async Task ReplicateMarkersAsync_NoFollowers_ReturnsSuccess()
    {
        // Arrange
        var replicator = CreateReplicator();

        // Create partition with only local broker as replica
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };
        _clusterState.AssignReplicas(tp, [_localBrokerId]);

        var txnMetadata = CreateTransactionMetadata("test-txn", partitions: [tp]);

        // Act
        var result = await replicator.ReplicateMarkersAsync(txnMetadata.TransactionalId, txnMetadata.ProducerId, txnMetadata.ProducerEpoch, [.. txnMetadata.Partitions], commit: true, coordinatorEpoch: 1, CancellationToken.None);

        // Assert
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void MarkerReplicationResult_Success_Factory()
    {
        // Act
        var result = MarkerReplicationResult.Success();

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Empty(result.SuccessfulBrokers);
        Assert.Empty(result.FailedBrokers);
    }

    [Fact]
    public void MarkerReplicationResult_TracksSuccessfulBrokers()
    {
        // Arrange
        var result = new MarkerReplicationResult();

        // Act
        result.SuccessfulBrokers.Add(1);
        result.SuccessfulBrokers.Add(2);

        // Assert
        Assert.Contains(1, result.SuccessfulBrokers);
        Assert.Contains(2, result.SuccessfulBrokers);
        Assert.Equal(2, result.SuccessfulBrokers.Count);
    }

    [Fact]
    public void MarkerReplicationResult_TracksFailedBrokers()
    {
        // Arrange
        var result = new MarkerReplicationResult();

        // Act
        result.FailedBrokers[1] = "Connection refused";
        result.FailedBrokers[2] = "Timeout";

        // Assert
        Assert.Equal("Connection refused", result.FailedBrokers[1]);
        Assert.Equal("Timeout", result.FailedBrokers[2]);
        Assert.Equal(2, result.FailedBrokers.Count);
    }

    [Fact]
    public void BrokerReplicationResult_Success_HasNoError()
    {
        // Act
        var result = new BrokerReplicationResult(1, true, null);

        // Assert
        Assert.Equal(1, result.BrokerId);
        Assert.True(result.Success);
        Assert.Null(result.Error);
    }

    [Fact]
    public void BrokerReplicationResult_Failure_HasError()
    {
        // Act
        var result = new BrokerReplicationResult(2, false, "Connection timeout");

        // Assert
        Assert.Equal(2, result.BrokerId);
        Assert.False(result.Success);
        Assert.Equal("Connection timeout", result.Error);
    }

    [Fact]
    public void PartitionTopicPair_StoresTopicAndPartition()
    {
        // Act
        var pair = new PartitionTopicPair("my-topic", 5);

        // Assert
        Assert.Equal("my-topic", pair.Topic);
        Assert.Equal(5, pair.Partition);
    }

    [Fact]
    public void PartitionTopicPair_Equality()
    {
        // Arrange
        var pair1 = new PartitionTopicPair("topic-a", 0);
        var pair2 = new PartitionTopicPair("topic-a", 0);
        var pair3 = new PartitionTopicPair("topic-a", 1);

        // Assert
        Assert.Equal(pair1, pair2);
        Assert.NotEqual(pair1, pair3);
    }

    [Fact]
    public void TransactionMarkerReplicatorOptions_DefaultValues()
    {
        // Act
        var options = new TransactionMarkerReplicatorOptions();

        // Assert
        Assert.Equal(5000, options.TimeoutMs);
        Assert.Equal(3, options.MaxRetries);
        Assert.Equal(100, options.RetryBackoffMs);
    }

    [Fact]
    public void TransactionMarkerReplicatorOptions_CustomValues()
    {
        // Act
        var options = new TransactionMarkerReplicatorOptions
        {
            TimeoutMs = 10000,
            MaxRetries = 5,
            RetryBackoffMs = 200
        };

        // Assert
        Assert.Equal(10000, options.TimeoutMs);
        Assert.Equal(5, options.MaxRetries);
        Assert.Equal(200, options.RetryBackoffMs);
    }

    [Fact]
    public async Task ReplicateMarkersAsync_CancellationRequested_ThrowsOperationCanceledException()
    {
        // Arrange
        var replicator = CreateReplicator();
        var tp = new TopicPartition { Topic = "test-topic", Partition = 0 };
        _clusterState.AssignReplicas(tp, [1, 2, 3]);
        var txnMetadata = CreateTransactionMetadata("test-txn", partitions: [tp]);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert - TaskCanceledException inherits from OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await replicator.ReplicateMarkersAsync(txnMetadata.TransactionalId, txnMetadata.ProducerId, txnMetadata.ProducerEpoch, [.. txnMetadata.Partitions], commit: true, coordinatorEpoch: 1, cts.Token));
    }

    private TransactionMarkerReplicator CreateReplicator(TransactionMarkerReplicatorOptions? options = null)
    {
        return new TransactionMarkerReplicator(
            _connectionPool,
            _clusterState,
            _localBrokerId,
            NullLogger<TransactionMarkerReplicator>.Instance,
            options);
    }

    private static TransactionMetadata CreateTransactionMetadata(
        string transactionalId,
        long producerId = 12345,
        short producerEpoch = 1,
        IEnumerable<TopicPartition>? partitions = null)
    {
        var metadata = new TransactionMetadata
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            State = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState.Ongoing,
            TransactionTimeoutMs = 60000
        };

        if (partitions != null)
        {
            foreach (var tp in partitions)
            {
                metadata.Partitions.Add(tp);
            }
        }

        return metadata;
    }
}
