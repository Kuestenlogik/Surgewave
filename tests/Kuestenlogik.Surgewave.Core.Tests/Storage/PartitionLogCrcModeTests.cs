using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// Append-time CRC handling (#85). The producer's CRC covers bytes 21.., and the append only
/// stamps the base offset into bytes 0-7, so the checksum survives — which is what makes
/// validating it instead of overwriting it possible.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PartitionLogCrcModeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);

    public PartitionLogCrcModeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-crcmode-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
    }

    private byte[] BuildBatch(int recordCount = 3)
    {
        var messages = new List<Message>();
        for (var i = 0; i < recordCount; i++)
        {
            messages.Add(new Message
            {
                Offset = i,
                Timestamp = 1_700_000_000_000,
                Key = ReadOnlyMemory<byte>.Empty,
                Value = new byte[] { (byte)i, 2, 3, 4 },
                Headers = ReadOnlyMemory<byte>.Empty
            });
        }

        return _serializer.SerializeMessages(messages);
    }

    private PartitionLog CreateLog(string name)
    {
        var dir = Path.Combine(_tempDir, name);
        Directory.CreateDirectory(dir);
        return new PartitionLog(dir, new TopicPartition { Topic = name, Partition = 0 }, new MemoryLogSegmentFactory());
    }

    private static uint ReadStoredCrc(ReadOnlySpan<byte> batch)
        => BinaryPrimitives.ReadUInt32BigEndian(batch.Slice(17, 4));

    [Fact]
    public async Task Validate_ValidCrc_AppendsAndKeepsProducerCrc()
    {
        using var log = CreateLog("validate-ok");
        var batch = BuildBatch();
        var producerCrc = ReadStoredCrc(batch);

        var offset = await log.AppendBatchAsync(batch, 0, batch.Length, BatchCrcMode.Validate);

        Assert.Equal(0, offset);
        Assert.Equal(3, log.NextOffset);
        Assert.Equal(producerCrc, ReadStoredCrc(batch));
        Assert.True(RecordBatchValidator.ValidateCrc(batch));
    }

    [Fact]
    public async Task Validate_CorruptPayload_ThrowsAndLeavesLogUntouched()
    {
        using var log = CreateLog("validate-corrupt");
        var batch = BuildBatch();
        batch[^1] ^= 0xFF; // flip a payload byte: the stored CRC no longer describes the bytes

        var ex = await Assert.ThrowsAsync<DataCorruptionException>(
            async () => await log.AppendBatchAsync(batch, 0, batch.Length, BatchCrcMode.Validate));

        Assert.Contains("validate-corrupt", ex.Message, StringComparison.Ordinal);
        // Rejected before any offset was claimed or anything written.
        Assert.Equal(0, log.NextOffset);
        Assert.Equal(0, log.HighWatermark);
    }

    [Fact]
    public async Task Validate_BatchShorterThanHeader_ThrowsWithLengthMessage()
    {
        using var log = CreateLog("validate-short");
        var tooShort = new byte[40];

        var ex = await Assert.ThrowsAsync<DataCorruptionException>(
            async () => await log.AppendBatchAsync(tooShort, 0, tooShort.Length, BatchCrcMode.Validate));

        Assert.Contains("below the 61-byte", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Trusted_SkipsTheCheck_AndLeavesTheStoredCrcAlone()
    {
        // Proves the pass is genuinely skipped: a deliberately wrong CRC is neither caught
        // (as Validate would) nor overwritten (as Recompute would).
        using var log = CreateLog("trusted");
        var batch = BuildBatch();
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), 0xDEADBEEF);

        var offset = await log.AppendBatchAsync(batch, 0, batch.Length, BatchCrcMode.Trusted);

        Assert.Equal(0, offset);
        Assert.Equal(0xDEADBEEF, ReadStoredCrc(batch));
    }

    [Fact]
    public async Task Recompute_IsTheDefault_AndHealsAPlaceholderCrc()
    {
        using var log = CreateLog("recompute");
        var batch = BuildBatch();
        var expected = Crc32C.Compute(batch.AsSpan(21));
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), 0); // placeholder, as older callers do

        await log.AppendBatchAsync(batch, 0, batch.Length); // default overload

        Assert.Equal(expected, ReadStoredCrc(batch));
    }

    [Fact]
    public async Task Validate_OffsetStamping_DoesNotInvalidateTheCrc()
    {
        // Second append gets a non-zero base offset stamped into bytes 0-7. If the CRC covered
        // those bytes, this append would fail — the whole approach rests on it not doing so.
        using var log = CreateLog("offset-stamp");
        await log.AppendBatchAsync(BuildBatch(), 0, BuildBatch().Length, BatchCrcMode.Validate);

        var second = BuildBatch();
        var offset = await log.AppendBatchAsync(second, 0, second.Length, BatchCrcMode.Validate);

        Assert.Equal(3, offset);
        Assert.Equal(3, BinaryPrimitives.ReadInt64BigEndian(second.AsSpan(0, 8)));
        Assert.True(RecordBatchValidator.ValidateCrc(second));
    }

    [Fact]
    public async Task Ephemeral_Validate_RejectsCorruptBatch()
    {
        using var log = new EphemeralPartitionLog(
            new TopicPartition { Topic = "ephemeral", Partition = 0 }, bufferBytes: 1024 * 1024);
        var batch = BuildBatch();
        batch[^1] ^= 0xFF;

        await Assert.ThrowsAsync<DataCorruptionException>(
            async () => await log.AppendBatchAsync(batch, 0, batch.Length, BatchCrcMode.Validate));

        Assert.Equal(0, log.NextOffset);
    }

    [Fact]
    public async Task Ephemeral_Trusted_KeepsStoredCrc()
    {
        using var log = new EphemeralPartitionLog(
            new TopicPartition { Topic = "ephemeral-trusted", Partition = 0 }, bufferBytes: 1024 * 1024);
        var batch = BuildBatch();
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), 0xDEADBEEF);

        await log.AppendBatchAsync(batch, 0, batch.Length, BatchCrcMode.Trusted);

        Assert.Equal(0xDEADBEEF, ReadStoredCrc(batch));
    }
}
