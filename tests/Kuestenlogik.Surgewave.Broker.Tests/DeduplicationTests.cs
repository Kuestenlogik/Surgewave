using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

[Trait("Category", TestCategories.Unit)]
public class DeduplicationTests : IDisposable
{
    private readonly DeduplicationConfig _config;
    private readonly DeduplicationManager _manager;
    private readonly TopicPartition _tp = new() { Topic = "test-topic", Partition = 0 };

    public DeduplicationTests()
    {
        _config = new DeduplicationConfig
        {
            Enabled = true,
            MaxEntriesPerPartition = 100,
            WindowSizeMs = 60_000,
            CleanupIntervalMs = 600_000 // Long interval so cleanup doesn't interfere with tests
        };
        _manager = new DeduplicationManager(_config, null, NullLogger<DeduplicationManager>.Instance);
    }

    [Fact]
    public void CheckDuplicate_NoPriorRecord_ReturnsNotDuplicate()
    {
        var batch = CreateRecordBatch("hello world");

        var result = _manager.CheckDuplicate(_tp, batch);

        Assert.False(result.IsDuplicate);
        Assert.Equal(-1, result.OriginalOffset);
    }

    [Fact]
    public void CheckDuplicate_AfterRegister_DetectsDuplicate()
    {
        var batch = CreateRecordBatch("duplicate content");

        // Register the batch at offset 42
        _manager.Register(_tp, batch, 42);

        // Check again — should be detected as duplicate
        var result = _manager.CheckDuplicate(_tp, batch);

        Assert.True(result.IsDuplicate);
        Assert.Equal(42, result.OriginalOffset);
    }

    [Fact]
    public void CheckDuplicate_DifferentContent_ReturnsNotDuplicate()
    {
        var batch1 = CreateRecordBatch("message A");
        var batch2 = CreateRecordBatch("message B");

        _manager.Register(_tp, batch1, 10);

        var result = _manager.CheckDuplicate(_tp, batch2);

        Assert.False(result.IsDuplicate);
    }

    [Fact]
    public void CheckDuplicate_DifferentPartitions_Independent()
    {
        var tp2 = new TopicPartition { Topic = "test-topic", Partition = 1 };
        var batch = CreateRecordBatch("shared content");

        _manager.Register(_tp, batch, 100);

        // Same content on different partition is not a duplicate
        var result = _manager.CheckDuplicate(tp2, batch);

        Assert.False(result.IsDuplicate);
    }

    [Fact]
    public void TotalEntries_TracksRegisteredEntries()
    {
        Assert.Equal(0, _manager.TotalEntries);

        _manager.Register(_tp, CreateRecordBatch("msg1"), 0);
        _manager.Register(_tp, CreateRecordBatch("msg2"), 1);
        _manager.Register(_tp, CreateRecordBatch("msg3"), 2);

        Assert.Equal(3, _manager.TotalEntries);
    }

    [Fact]
    public void PartitionWindow_EvictsEntry_WhenAtCapacity()
    {
        var smallConfig = new DeduplicationConfig
        {
            Enabled = true,
            MaxEntriesPerPartition = 3,
            WindowSizeMs = 60_000,
            CleanupIntervalMs = 600_000
        };
        var manager = new DeduplicationManager(smallConfig, null, NullLogger<DeduplicationManager>.Instance);

        // Register 3 entries (at capacity)
        var batch1 = CreateRecordBatch("entry1");
        var batch2 = CreateRecordBatch("entry2");
        var batch3 = CreateRecordBatch("entry3");
        manager.Register(_tp, batch1, 0);
        manager.Register(_tp, batch2, 1);
        manager.Register(_tp, batch3, 2);

        Assert.Equal(3, manager.TotalEntries);

        // Register a 4th — should evict one entry to stay at capacity
        var batch4 = CreateRecordBatch("entry4");
        manager.Register(_tp, batch4, 3);

        // After eviction, count stays at 3 (evicted 1, added 1)
        Assert.Equal(3, manager.TotalEntries);

        // The newest entry must be present
        var result4 = manager.CheckDuplicate(_tp, batch4);
        Assert.True(result4.IsDuplicate);
        Assert.Equal(3, result4.OriginalOffset);

        manager.Dispose();
    }

    [Fact]
    public void ContentHash_IgnoresHeaderMetadata()
    {
        // Two batches with same content but different offsets should hash the same
        var batch1 = CreateRecordBatch("same content", baseOffset: 0);
        var batch2 = CreateRecordBatch("same content", baseOffset: 999);

        var hash1 = RecordBatchHasher.ComputeContentHash(batch1);
        var hash2 = RecordBatchHasher.ComputeContentHash(batch2);

        Assert.Equal(hash1, hash2);
        Assert.NotEqual(0UL, hash1);
    }

    [Fact]
    public void ContentHash_DifferentContent_DifferentHash()
    {
        var batch1 = CreateRecordBatch("content A");
        var batch2 = CreateRecordBatch("content B");

        var hash1 = RecordBatchHasher.ComputeContentHash(batch1);
        var hash2 = RecordBatchHasher.ComputeContentHash(batch2);

        Assert.NotEqual(hash1, hash2);
    }

    [Fact]
    public void ContentHash_TooSmallBatch_ReturnsZero()
    {
        var tinyBatch = new byte[10];
        var hash = RecordBatchHasher.ComputeContentHash(tinyBatch);
        Assert.Equal(0UL, hash);
    }

    [Fact]
    public void DuplicateResponse_ReturnsOriginalOffset_Transparently()
    {
        var batch = CreateRecordBatch("important message");

        _manager.Register(_tp, batch, 777);
        var result = _manager.CheckDuplicate(_tp, batch);

        // Client sees the original offset — idempotent produce
        Assert.True(result.IsDuplicate);
        Assert.Equal(777, result.OriginalOffset);
    }

    /// <summary>
    /// Creates a minimal valid RecordBatch with the given content as records data.
    /// The 61-byte header uses the given baseOffset; records data is the UTF-8 content.
    /// </summary>
    private static byte[] CreateRecordBatch(string content, long baseOffset = 0)
    {
        var recordsData = Encoding.UTF8.GetBytes(content);
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + recordsData.Length];

        // Write base offset (first 8 bytes)
        BinaryPrimitives.WriteInt64BigEndian(batch.AsSpan(0, 8), baseOffset);

        // Write batch length (bytes 8-11)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(8, 4), batch.Length - 12);

        // Write record count at offset 57 (4 bytes)
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(57, 4), 1);

        // Copy content as records data
        recordsData.CopyTo(batch, KafkaConstants.RecordBatch.HeaderSize);

        return batch;
    }

    public void Dispose()
    {
        _manager.Dispose();
    }
}
