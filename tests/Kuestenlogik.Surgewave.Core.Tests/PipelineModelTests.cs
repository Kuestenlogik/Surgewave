using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Pipeline;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for pipeline models: ReadRequest, PooledCompletionSource, MessageBatch,
/// RecordBatchHeader, PartitionMetadata.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PipelineModelTests
{
    #region ReadRequest Tests

    [Fact]
    public void ReadRequest_Properties_SetCorrectly()
    {
        var tp = new TopicPartition { Topic = "events", Partition = 2 };
        var tcs = new TaskCompletionSource<List<Message>>();
        using var cts = new CancellationTokenSource();

        var request = new ReadRequest(tp, StartOffset: 100, MaxMessages: 50, tcs, cts.Token);

        Assert.Equal("events", request.TopicPartition.Topic);
        Assert.Equal(2, request.TopicPartition.Partition);
        Assert.Equal(100, request.StartOffset);
        Assert.Equal(50, request.MaxMessages);
        Assert.Same(tcs, request.CompletionSource);
    }

    [Fact]
    public void ReadRequest_Equality_SameInstance_AreEqual()
    {
        var tp = new TopicPartition { Topic = "t", Partition = 0 };
        var tcs = new TaskCompletionSource<List<Message>>();

        var r1 = new ReadRequest(tp, 0, 10, tcs, CancellationToken.None);
        var r2 = r1;

        Assert.Equal(r1, r2);
    }

    #endregion

    #region PooledCompletionSource Tests

    [Fact]
    public async Task PooledCompletionSource_SetResult_CompletesValueTask()
    {
        var source = PooledCompletionSource<int>.Rent();

        source.SetResult(42);
        var result = await source.ValueTask;

        Assert.Equal(42, result);
        source.Return();
    }

    [Fact]
    public async Task PooledCompletionSource_SetException_ThrowsOnAwait()
    {
        var source = PooledCompletionSource<string>.Rent();

        source.SetException(new InvalidOperationException("test error"));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await source.ValueTask;
        });

        source.Return();
    }

    [Fact]
    public async Task PooledCompletionSource_TrySetResult_ReturnsTrueFirstTime()
    {
        var source = PooledCompletionSource<int>.Rent();

        Assert.True(source.TrySetResult(10));

        var result = await source.ValueTask;
        Assert.Equal(10, result);

        source.Return();
    }

    [Fact]
    public async Task PooledCompletionSource_TrySetException_ReturnsTrueFirstTime()
    {
        var source = PooledCompletionSource<int>.Rent();

        Assert.True(source.TrySetException(new Exception("first")));
        Assert.False(source.TrySetException(new Exception("second")));

        await Assert.ThrowsAsync<Exception>(async () => await source.ValueTask);
        source.Return();
    }

    [Fact]
    public async Task PooledCompletionSource_RentReturnReuse_Works()
    {
        // Rent, use, return, rent again
        var source1 = PooledCompletionSource<int>.Rent();
        source1.SetResult(1);
        Assert.Equal(1, await source1.ValueTask);
        source1.Return();

        var source2 = PooledCompletionSource<int>.Rent();
        source2.SetResult(2);
        Assert.Equal(2, await source2.ValueTask);
        source2.Return();
    }

    #endregion

    #region MessageBatch Tests

    [Fact]
    public void MessageBatch_Properties_SetCorrectly()
    {
        var messages = new List<Message>
        {
            CreateMessage(0),
            CreateMessage(1),
            CreateMessage(2)
        };

        var batch = new MessageBatch
        {
            BaseOffset = 100,
            PartitionId = 3,
            Messages = messages,
            Timestamp = 1234567890
        };

        Assert.Equal(100, batch.BaseOffset);
        Assert.Equal(3, batch.PartitionId);
        Assert.Equal(3, batch.MessageCount);
        Assert.Equal(102, batch.LastOffset); // 100 + 3 - 1
        Assert.Equal(1234567890, batch.Timestamp);
    }

    [Fact]
    public void MessageBatch_SingleMessage_LastOffsetEqualsBaseOffset()
    {
        var batch = new MessageBatch
        {
            BaseOffset = 50,
            PartitionId = 0,
            Messages = [CreateMessage(0)],
            Timestamp = 0
        };

        Assert.Equal(1, batch.MessageCount);
        Assert.Equal(50, batch.LastOffset);
    }

    #endregion

    #region RecordBatchHeader Tests

    [Fact]
    public void RecordBatchHeader_Properties_SetCorrectly()
    {
        var header = new RecordBatchHeader
        {
            BaseOffset = 100,
            BatchLength = 500,
            PartitionLeaderEpoch = 1,
            Magic = 2,
            Crc = 0x12345678u,
            Attributes = 0,
            LastOffsetDelta = 4,
            BaseTimestamp = 1000000,
            MaxTimestamp = 1000500,
            ProducerId = -1,
            ProducerEpoch = -1,
            BaseSequence = -1,
            RecordCount = 5
        };

        Assert.Equal(100, header.BaseOffset);
        Assert.Equal(500, header.BatchLength);
        Assert.Equal(1, header.PartitionLeaderEpoch);
        Assert.Equal(2, header.Magic);
        Assert.Equal(0x12345678u, header.Crc);
        Assert.Equal(0, header.Attributes);
        Assert.Equal(4, header.LastOffsetDelta);
        Assert.Equal(1000000, header.BaseTimestamp);
        Assert.Equal(1000500, header.MaxTimestamp);
        Assert.Equal(-1, header.ProducerId);
        Assert.Equal(-1, header.ProducerEpoch);
        Assert.Equal(-1, header.BaseSequence);
        Assert.Equal(5, header.RecordCount);
    }

    [Fact]
    public void RecordBatchHeader_Equality_SameValues_AreEqual()
    {
        var a = new RecordBatchHeader
        {
            BaseOffset = 0, BatchLength = 100, PartitionLeaderEpoch = 0,
            Magic = 2, Crc = 0, Attributes = 0, LastOffsetDelta = 0,
            BaseTimestamp = 1000, MaxTimestamp = 1000, ProducerId = -1,
            ProducerEpoch = -1, BaseSequence = -1, RecordCount = 1
        };
        var b = new RecordBatchHeader
        {
            BaseOffset = 0, BatchLength = 100, PartitionLeaderEpoch = 0,
            Magic = 2, Crc = 0, Attributes = 0, LastOffsetDelta = 0,
            BaseTimestamp = 1000, MaxTimestamp = 1000, ProducerId = -1,
            ProducerEpoch = -1, BaseSequence = -1, RecordCount = 1
        };

        Assert.Equal(a, b);
    }

    #endregion

    #region PartitionMetadata Tests

    [Fact]
    public void PartitionMetadata_Properties_SetCorrectly()
    {
        var metadata = new PartitionMetadata(
            PartitionId: 3,
            Leader: 1,
            Replicas: [1, 2, 3],
            InSyncReplicas: [1, 2]);

        Assert.Equal(3, metadata.PartitionId);
        Assert.Equal(1, metadata.Leader);
        Assert.Equal([1, 2, 3], metadata.Replicas);
        Assert.Equal([1, 2], metadata.InSyncReplicas);
    }

    [Fact]
    public void PartitionMetadata_Equality_SameValues_AreEqual()
    {
        var replicas = new List<int> { 1, 2 };
        var isr = new List<int> { 1 };

        var a = new PartitionMetadata(0, 1, replicas, isr);
        var b = new PartitionMetadata(0, 1, replicas, isr);

        Assert.Equal(a, b);
    }

    #endregion

    #region Helpers

    private static Message CreateMessage(long offset) => new()
    {
        Offset = offset,
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Key = ReadOnlyMemory<byte>.Empty,
        Value = new byte[] { 1, 2, 3 },
        Headers = ReadOnlyMemory<byte>.Empty
    };

    #endregion
}
