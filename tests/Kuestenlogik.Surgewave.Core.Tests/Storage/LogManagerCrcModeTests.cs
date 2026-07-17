using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Exceptions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Storage;

/// <summary>
/// CRC mode travels from the caller through the pooled WriteRequest and the write pipeline into
/// the log, and a rejection must come back out typed (#85).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LogManagerCrcModeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly RecordBatchSerializer _serializer = new(NullLogger<RecordBatchSerializer>.Instance);
    private readonly LogManager _logManager;

    public LogManagerCrcModeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-lm-crcmode", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _logManager = new LogManager(_tempDir, new MemoryLogSegmentFactory());
    }

    public void Dispose()
    {
        _logManager.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
    }

    private byte[] BuildBatch(byte marker)
    {
        var messages = new List<Message>
        {
            new()
            {
                Offset = 0,
                Timestamp = 1_700_000_000_000,
                Key = ReadOnlyMemory<byte>.Empty,
                Value = new byte[] { marker, 1, 2, 3 },
                Headers = ReadOnlyMemory<byte>.Empty
            }
        };
        return _serializer.SerializeMessages(messages);
    }

    [Fact]
    public async Task Validate_CorruptBatch_FaultsThroughThePipeline_AndThePartitionStaysUsable()
    {
        var tp = new TopicPartition { Topic = "pipeline-crc", Partition = 0 };

        var corrupt = BuildBatch(1);
        corrupt[^1] ^= 0xFF;

        await Assert.ThrowsAsync<DataCorruptionException>(
            async () => await _logManager.AppendBatchAsync(tp, corrupt, BatchCrcMode.Validate));

        // The pooled WriteRequest must come back clean: a valid append on the same partition
        // still works and gets offset 0 (nothing was written by the rejected one).
        var valid = BuildBatch(2);
        var offset = await _logManager.AppendBatchAsync(tp, valid, BatchCrcMode.Validate);

        Assert.Equal(0, offset);
    }

    [Fact]
    public async Task Validate_CorruptBatch_DoesNotFaultOtherProducersOnTheSamePartition()
    {
        // Requests for one partition are flushed as a list in a single try. Without per-request
        // isolation a corrupt batch would fault the valid batches queued behind it.
        var tp = new TopicPartition { Topic = "co-batch", Partition = 0 };

        var corrupt = BuildBatch(1);
        corrupt[^1] ^= 0xFF;
        var valid = BuildBatch(2);

        var corruptTask = _logManager.AppendBatchAsync(tp, corrupt, BatchCrcMode.Validate).AsTask();
        var validTask = _logManager.AppendBatchAsync(tp, valid, BatchCrcMode.Validate).AsTask();

        await Assert.ThrowsAsync<DataCorruptionException>(async () => await corruptTask);
        var offset = await validTask; // must have landed regardless

        Assert.Equal(0, offset);
        Assert.Equal(1, _logManager.GetLog(tp)!.NextOffset);
    }

    [Fact]
    public async Task DefaultOverload_StaysOnRecompute()
    {
        var tp = new TopicPartition { Topic = "default-mode", Partition = 0 };
        var batch = BuildBatch(3);
        BinaryPrimitives.WriteUInt32BigEndian(batch.AsSpan(17, 4), 0); // placeholder CRC

        // No mode given: the legacy path must still heal it rather than reject it.
        var offset = await _logManager.AppendBatchAsync(tp, batch);

        Assert.Equal(0, offset);
        Assert.True(RecordBatchValidator.ValidateCrc(batch));
    }

    [Fact]
    public async Task Trusted_ThenValidate_OnPooledRequests_DoesNotLeakTheMode()
    {
        // WriteRequest is pooled: a Trusted append must not leave the mode behind for the next
        // caller, which would silently skip validation.
        var tp = new TopicPartition { Topic = "pool-reuse", Partition = 0 };

        var trusted = BuildBatch(4);
        BinaryPrimitives.WriteUInt32BigEndian(trusted.AsSpan(17, 4), 0xDEADBEEF);
        await _logManager.AppendBatchAsync(tp, trusted, BatchCrcMode.Trusted);

        var corrupt = BuildBatch(5);
        corrupt[^1] ^= 0xFF;

        await Assert.ThrowsAsync<DataCorruptionException>(
            async () => await _logManager.AppendBatchAsync(tp, corrupt, BatchCrcMode.Validate));
    }
}
