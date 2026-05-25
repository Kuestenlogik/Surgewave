using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.Transactions;

/// <summary>
/// Tests for TransactionLogEntry serialization and deserialization.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class TransactionLogEntryTests
{
    [Fact]
    public void Serialize_EmptyEntry_RoundTrips()
    {
        // Arrange
        var entry = new TransactionLogEntry
        {
            TransactionalId = "test-txn-1",
            ProducerId = 12345,
            ProducerEpoch = 1,
            State = TransactionLogState.Empty,
            TransactionTimeoutMs = 60000,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = 5,
            Partitions = []
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(entry.TransactionalId, deserialized.TransactionalId);
        Assert.Equal(entry.ProducerId, deserialized.ProducerId);
        Assert.Equal(entry.ProducerEpoch, deserialized.ProducerEpoch);
        Assert.Equal(entry.State, deserialized.State);
        Assert.Equal(entry.TransactionTimeoutMs, deserialized.TransactionTimeoutMs);
        Assert.Equal(entry.TimestampMs, deserialized.TimestampMs);
        Assert.Equal(entry.CoordinatorEpoch, deserialized.CoordinatorEpoch);
        Assert.Empty(deserialized.Partitions);
    }

    [Fact]
    public void Serialize_WithPartitions_RoundTrips()
    {
        // Arrange
        var entry = new TransactionLogEntry
        {
            TransactionalId = "txn-with-partitions",
            ProducerId = 99999,
            ProducerEpoch = 3,
            State = TransactionLogState.Ongoing,
            TransactionTimeoutMs = 30000,
            TimestampMs = 1704067200000,
            CoordinatorEpoch = 10,
            Partitions =
            [
                new TransactionLogPartition("topic-a", 0),
                new TransactionLogPartition("topic-a", 1),
                new TransactionLogPartition("topic-b", 0)
            ]
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(entry.TransactionalId, deserialized.TransactionalId);
        Assert.Equal(entry.ProducerId, deserialized.ProducerId);
        Assert.Equal(entry.State, deserialized.State);
        Assert.Equal(3, deserialized.Partitions.Count);
        Assert.Equal("topic-a", deserialized.Partitions[0].Topic);
        Assert.Equal(0, deserialized.Partitions[0].Partition);
        Assert.Equal("topic-a", deserialized.Partitions[1].Topic);
        Assert.Equal(1, deserialized.Partitions[1].Partition);
        Assert.Equal("topic-b", deserialized.Partitions[2].Topic);
        Assert.Equal(0, deserialized.Partitions[2].Partition);
    }

    [Theory]
    [InlineData(0)] // Empty
    [InlineData(1)] // Ongoing
    [InlineData(2)] // PrepareCommit
    [InlineData(3)] // PrepareAbort
    [InlineData(4)] // CompleteCommit
    [InlineData(5)] // CompleteAbort
    [InlineData(6)] // Dead
    public void Serialize_AllStates_RoundTrip(int stateValue)
    {
        // Arrange
        var state = (TransactionLogState)stateValue;
        var entry = new TransactionLogEntry
        {
            TransactionalId = $"txn-state-{stateValue}",
            ProducerId = 1,
            ProducerEpoch = 1,
            State = state,
            TransactionTimeoutMs = 60000,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = 1,
            Partitions = []
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(state, deserialized.State);
    }

    [Fact]
    public void Serialize_LongTransactionalId_RoundTrips()
    {
        // Arrange
        var longId = new string('x', 1000);
        var entry = new TransactionLogEntry
        {
            TransactionalId = longId,
            ProducerId = 1,
            ProducerEpoch = 1,
            State = TransactionLogState.Ongoing,
            TransactionTimeoutMs = 60000,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = 1,
            Partitions = []
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(longId, deserialized.TransactionalId);
    }

    [Fact]
    public void Serialize_UnicodeTransactionalId_RoundTrips()
    {
        // Arrange
        var unicodeId = "txn-日本語-émoji-🎉";
        var entry = new TransactionLogEntry
        {
            TransactionalId = unicodeId,
            ProducerId = 1,
            ProducerEpoch = 1,
            State = TransactionLogState.Ongoing,
            TransactionTimeoutMs = 60000,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = 1,
            Partitions = []
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(unicodeId, deserialized.TransactionalId);
    }

    [Fact]
    public void Serialize_MaxValues_RoundTrips()
    {
        // Arrange
        var entry = new TransactionLogEntry
        {
            TransactionalId = "max-values-txn",
            ProducerId = long.MaxValue,
            ProducerEpoch = short.MaxValue,
            State = TransactionLogState.Ongoing,
            TransactionTimeoutMs = int.MaxValue,
            TimestampMs = long.MaxValue,
            CoordinatorEpoch = int.MaxValue,
            Partitions =
            [
                new TransactionLogPartition("topic", int.MaxValue)
            ]
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(long.MaxValue, deserialized.ProducerId);
        Assert.Equal(short.MaxValue, deserialized.ProducerEpoch);
        Assert.Equal(int.MaxValue, deserialized.TransactionTimeoutMs);
        Assert.Equal(long.MaxValue, deserialized.TimestampMs);
        Assert.Equal(int.MaxValue, deserialized.CoordinatorEpoch);
        Assert.Equal(int.MaxValue, deserialized.Partitions[0].Partition);
    }

    [Fact]
    public void CreateKey_ReturnsUtf8Bytes()
    {
        // Arrange
        var transactionalId = "my-transaction";

        // Act
        var key = TransactionLogEntry.CreateKey(transactionalId);

        // Assert
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(transactionalId), key);
    }

    [Fact]
    public void CreateKey_UnicodeId_ReturnsUtf8Bytes()
    {
        // Arrange
        var transactionalId = "txn-日本語";

        // Act
        var key = TransactionLogEntry.CreateKey(transactionalId);

        // Assert
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(transactionalId), key);
    }

    [Fact]
    public void Deserialize_InvalidVersion_ThrowsException()
    {
        // Arrange - create bytes with invalid version (999)
        var invalidBytes = new byte[100];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(invalidBytes, 999);

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => TransactionLogEntry.Deserialize(invalidBytes));
    }

    [Fact]
    public void Serialize_ManyPartitions_RoundTrips()
    {
        // Arrange
        var partitions = Enumerable.Range(0, 100)
            .Select(i => new TransactionLogPartition($"topic-{i % 10}", i))
            .ToList();

        var entry = new TransactionLogEntry
        {
            TransactionalId = "many-partitions",
            ProducerId = 1,
            ProducerEpoch = 1,
            State = TransactionLogState.Ongoing,
            TransactionTimeoutMs = 60000,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = 1,
            Partitions = partitions
        };

        // Act
        var bytes = entry.Serialize();
        var deserialized = TransactionLogEntry.Deserialize(bytes);

        // Assert
        Assert.Equal(100, deserialized.Partitions.Count);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal($"topic-{i % 10}", deserialized.Partitions[i].Topic);
            Assert.Equal(i, deserialized.Partitions[i].Partition);
        }
    }
}
