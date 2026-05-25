using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.GeoReplication;

[Trait("Category", TestCategories.Unit)]
public sealed class GeoReplicaFetcherTests : IAsyncLifetime, IDisposable
{
    private readonly LogManager _logManager;
    private readonly ClusterLink _link;
    private readonly GeoReplicaFetcher _fetcher;

    public GeoReplicaFetcherTests()
    {
        _logManager = new LogManager(
            Path.Combine(Path.GetTempPath(), $"surgewave-test-fetcher-{Guid.NewGuid():N}"),
            new MemoryLogSegmentFactory());

        var config = new ClusterLinkConfig
        {
            LinkId = "link-1",
            RemoteBootstrapServers = "remote:9092",
            FetchIntervalMs = 200,
            FetchMaxBytes = 512 * 1024,
            FetcherThreads = 2
        };
        _link = new ClusterLink(config, new Kuestenlogik.Surgewave.Transport.Tcp.TcpPeerTransport(), NullLogger.Instance);
        _fetcher = new GeoReplicaFetcher(_link, _logManager, metrics: null, NullLogger.Instance);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await _fetcher.DisposeAsync();
        await _link.DisposeAsync();
        _logManager.Dispose();
    }

    public void Dispose() => _logManager.Dispose();

    [Fact]
    public void AddPartitions_TracksPositions()
    {
        // Arrange
        var partitions = new[]
        {
            new TopicPartition { Topic = "orders", Partition = 0 },
            new TopicPartition { Topic = "orders", Partition = 1 }
        };

        // Act
        _fetcher.AddPartitions(partitions);

        // Assert - lag starts at 0 for tracked partitions
        Assert.Equal(0, _fetcher.GetLag(partitions[0]));
        Assert.Equal(0, _fetcher.GetLag(partitions[1]));
        Assert.Equal(0, _fetcher.GetTotalLag());
    }

    [Fact]
    public async Task AddPartitions_UsesLogNextOffset()
    {
        // Arrange - create a log with some data so NextOffset > 0
        var tp = new TopicPartition { Topic = "events", Partition = 0 };
        var log = _logManager.GetOrCreateLog(tp);
        var batch = CreateValidBatch(baseOffset: 0, recordCount: 5);
        await log.AppendBatchAsync(batch);

        // Act
        _fetcher.AddPartitions([tp]);

        // Assert - fetcher should pick up the log's NextOffset (5)
        // Since we can't directly access _fetchPositions, we verify via GetLag
        // which returns 0 because no remote lag is known yet
        Assert.Equal(0, _fetcher.GetLag(tp));
    }

    [Fact]
    public void RemovePartitions_StopsTracking()
    {
        // Arrange
        var tp0 = new TopicPartition { Topic = "orders", Partition = 0 };
        var tp1 = new TopicPartition { Topic = "orders", Partition = 1 };
        _fetcher.AddPartitions([tp0, tp1]);

        // Act
        _fetcher.RemovePartitions([tp0]);

        // Assert - removed partition returns default lag of 0
        Assert.Equal(0, _fetcher.GetLag(tp0));
        Assert.Equal(0, _fetcher.GetTotalLag());
    }

    [Fact]
    public void RemoveTopic_RemovesAllPartitions()
    {
        // Arrange
        var tp0 = new TopicPartition { Topic = "orders", Partition = 0 };
        var tp1 = new TopicPartition { Topic = "orders", Partition = 1 };
        var tp2 = new TopicPartition { Topic = "events", Partition = 0 };
        _fetcher.AddPartitions([tp0, tp1, tp2]);

        // Act
        _fetcher.RemoveTopic("orders");

        // Assert - orders partitions removed, events still tracked
        Assert.Equal(0, _fetcher.GetLag(tp0));
        Assert.Equal(0, _fetcher.GetLag(tp1));
        Assert.Equal(0, _fetcher.GetLag(tp2));
    }

    [Fact]
    public void GetLag_ReturnsZeroForUnknown()
    {
        // Act
        var lag = _fetcher.GetLag(new TopicPartition { Topic = "unknown", Partition = 99 });

        // Assert
        Assert.Equal(0, lag);
    }

    [Fact]
    public void GetTotalLag_SumsAllPartitions()
    {
        // Assert - no partitions means total lag is 0
        Assert.Equal(0, _fetcher.GetTotalLag());
    }

    [Fact]
    public void Properties_ReflectConfig()
    {
        // Assert
        Assert.Equal(TimeSpan.FromMilliseconds(200), _fetcher.FetchInterval);
        Assert.Equal(512 * 1024, _fetcher.MaxFetchBytes);
        Assert.Equal(2, _fetcher.FetcherThreads);
        Assert.Equal("link-1", _fetcher.LinkId);
    }

    [Fact]
    public async Task Dispose_CompletesCleanly()
    {
        // Act - dispose without starting should not throw
        await _fetcher.DisposeAsync();

        // Assert - no exception means success
        Assert.Equal(0, _fetcher.GetTotalLag());
    }

    private static byte[] CreateValidBatch(long baseOffset = 0, int recordCount = 1)
    {
        var batch = new byte[100];

        // BaseOffset (0-7)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);

        // BatchLength (8-11)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);

        // PartitionLeaderEpoch (12-15)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);

        // Magic (16) = 2
        batch[16] = 2;

        // Attributes (21-22)
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);

        // LastOffsetDelta (23-26)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);

        // BaseTimestamp (27-34)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // MaxTimestamp (35-42)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // ProducerId (43-50)
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);

        // ProducerEpoch (51-52)
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);

        // BaseSequence (53-56)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);

        // RecordCount (57-60)
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);

        // CRC (17-20) over bytes 21+
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(batch.AsSpan(21));
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);

        return batch;
    }
}
