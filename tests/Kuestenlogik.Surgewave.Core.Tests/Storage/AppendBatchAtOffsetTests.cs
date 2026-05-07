using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

[Trait("Category", TestCategories.Unit)]
public sealed class AppendBatchAtOffsetTests : IAsyncLifetime, IDisposable
{
    private readonly TopicPartition _tp = new() { Topic = "test-topic", Partition = 0 };
    private PartitionLog _log = null!;

    public void Dispose() => _log?.Dispose();

    public ValueTask InitializeAsync()
    {
        _log = new PartitionLog("in-memory", _tp, new MemoryLogSegmentFactory());
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _log.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task AppendAtOffset_WritesAtCorrectOffset()
    {
        // Arrange
        var batch = CreateValidBatch(baseOffset: 0, recordCount: 3);

        // Act
        var result = await _log.AppendBatchAtOffsetAsync(batch, targetOffset: 0);

        // Assert
        Assert.Equal(0, result);
        Assert.Equal(3, _log.NextOffset);
    }

    [Fact]
    public async Task AppendAtOffset_SparseOffset_Advances()
    {
        // Arrange
        var batch = CreateValidBatch(baseOffset: 100, recordCount: 5);

        // Act
        var result = await _log.AppendBatchAtOffsetAsync(batch, targetOffset: 100);

        // Assert
        Assert.Equal(100, result);
        Assert.Equal(105, _log.NextOffset);
    }

    [Fact]
    public async Task AppendAtOffset_LessThanNext_Throws()
    {
        // Arrange - advance NextOffset to 10
        var batch1 = CreateValidBatch(baseOffset: 0, recordCount: 10);
        await _log.AppendBatchAtOffsetAsync(batch1, targetOffset: 0);
        Assert.Equal(10, _log.NextOffset);

        // Act & Assert
        var batch2 = CreateValidBatch(baseOffset: 5, recordCount: 1);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _log.AppendBatchAtOffsetAsync(batch2, targetOffset: 5).AsTask());
    }

    [Fact]
    public async Task AppendAtOffset_EmptyBatch_ReturnsCurrentOffset()
    {
        // Arrange
        var emptyBatch = Array.Empty<byte>();

        // Act
        var result = await _log.AppendBatchAtOffsetAsync(emptyBatch, targetOffset: 0);

        // Assert
        Assert.Equal(0, result); // NextOffset was 0, returns current
        Assert.Equal(0, _log.NextOffset);
    }

    [Fact]
    public async Task AppendAtOffset_SequentialWrites()
    {
        // Arrange & Act
        var batch1 = CreateValidBatch(baseOffset: 0, recordCount: 3);
        await _log.AppendBatchAtOffsetAsync(batch1, targetOffset: 0);

        var batch2 = CreateValidBatch(baseOffset: 3, recordCount: 2);
        await _log.AppendBatchAtOffsetAsync(batch2, targetOffset: 3);

        var batch3 = CreateValidBatch(baseOffset: 5, recordCount: 4);
        await _log.AppendBatchAtOffsetAsync(batch3, targetOffset: 5);

        // Assert
        Assert.Equal(9, _log.NextOffset);
    }

    [Fact]
    public async Task AppendAtOffset_PreservesData()
    {
        // Arrange
        var batch = CreateValidBatch(baseOffset: 0, recordCount: 1);
        await _log.AppendBatchAtOffsetAsync(batch, targetOffset: 0);

        // Act
        var batches = await _log.ReadBatchesAsync(startOffset: 0);

        // Assert
        Assert.NotEmpty(batches);
        Assert.Equal(batch.Length, batches[0].Length);
    }

    private static byte[] CreateValidBatch(long baseOffset = 0, int recordCount = 1)
    {
        var batch = new byte[100];

        // BaseOffset (0-7)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);

        // BatchLength (8-11)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);

        // PartitionLeaderEpoch (12-15)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(12, 4), 0);

        // Magic (16) = 2
        batch[16] = 2;

        // Attributes (21-22)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(21, 2), 0);

        // LastOffsetDelta (23-26)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(23, 4), recordCount - 1);

        // BaseTimestamp (27-34)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(27, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // MaxTimestamp (35-42)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(35, 8), DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // ProducerId (43-50)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(43, 8), -1);

        // ProducerEpoch (51-52)
        BinaryPrimitives.WriteInt16BigEndian(batch.AsSpan(51, 2), -1);

        // BaseSequence (53-56)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(53, 4), -1);

        // RecordCount (57-60)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), recordCount);

        // CRC (17-20) over bytes 21+
        var crc = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), crc);

        return batch;
    }
}
