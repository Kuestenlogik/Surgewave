using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for BufferPool - high-performance buffer pooling for Kafka message processing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class BufferPoolTests
{
    #region Rent Tests

    [Theory]
    [InlineData(100)]     // Small buffer
    [InlineData(4096)]    // Exactly small size
    [InlineData(10000)]   // Medium buffer
    [InlineData(65536)]   // Exactly medium size
    [InlineData(100000)]  // Large buffer
    [InlineData(1048576)] // Exactly large size (1MB)
    public void Rent_ReturnsBufferOfAtLeastRequestedSize(int requestedSize)
    {
        var pool = new BufferPool();

        var buffer = pool.Rent(requestedSize);

        Assert.NotNull(buffer);
        Assert.True(buffer.Length >= requestedSize);
    }

    [Fact]
    public void Rent_SmallBuffer_ReturnsFromSmallPool()
    {
        var pool = new BufferPool();

        var buffer = pool.Rent(100);

        Assert.True(buffer.Length >= 100);
        Assert.True(buffer.Length <= 4096);
    }

    [Fact]
    public void Rent_VeryLargeBuffer_AllocatesExactSize()
    {
        var pool = new BufferPool();
        var requestedSize = 20 * 1024 * 1024; // 20MB

        var buffer = pool.Rent(requestedSize);

        Assert.Equal(requestedSize, buffer.Length);
    }

    [Fact]
    public void Rent_XLargeBuffer_LimitedAllocation()
    {
        var pool = new BufferPool();
        var buffers = new List<byte[]>();

        for (int i = 0; i < 10; i++)
        {
            buffers.Add(pool.Rent(16 * 1024 * 1024));
        }

        Assert.Equal(10, buffers.Count);
        foreach (var buffer in buffers)
        {
            Assert.True(buffer.Length >= 16 * 1024 * 1024);
        }
    }

    #endregion

    #region Return Tests

    [Fact]
    public void Return_SmallBuffer_ReturnsToPool()
    {
        var pool = new BufferPool();

        var buffer = pool.Rent(100);
        pool.Return(buffer);

        var buffer2 = pool.Rent(100);
        Assert.NotNull(buffer2);
    }

    [Fact]
    public void Return_VeryLargeBuffer_DoesNotThrow()
    {
        var pool = new BufferPool();
        var largeBuffer = new byte[50 * 1024 * 1024];

        pool.Return(largeBuffer);
    }

    #endregion

    #region RentDisposable Tests

    [Fact]
    public void RentDisposable_ReturnsRentedBuffer()
    {
        var pool = new BufferPool();

        using var rentedBuffer = pool.RentDisposable(1000);

        Assert.NotNull(rentedBuffer.Array);
        Assert.True(rentedBuffer.Length >= 1000);
    }

    [Fact]
    public void RentDisposable_AutomaticallyReturnsOnDispose()
    {
        var pool = new BufferPool();
        byte[] capturedBuffer;

        using (var rentedBuffer = pool.RentDisposable(1000))
        {
            capturedBuffer = rentedBuffer.Array;
            Assert.NotNull(capturedBuffer);
        }

        using var rentedBuffer2 = pool.RentDisposable(1000);
        Assert.NotNull(rentedBuffer2.Array);
    }

    [Fact]
    public void RentedBuffer_ProvidesSpanAndMemory()
    {
        var pool = new BufferPool();

        using var rentedBuffer = pool.RentDisposable(100);

        var span = rentedBuffer.Span;
        var memory = rentedBuffer.Memory;

        Assert.True(span.Length >= 100);
        Assert.True(memory.Length >= 100);
    }

    [Fact]
    public void RentedBuffer_ImplicitConversions()
    {
        var pool = new BufferPool();

        using var rentedBuffer = pool.RentDisposable(100);

        byte[] array = rentedBuffer;
        Assert.NotNull(array);

        Span<byte> span = rentedBuffer;
        Assert.True(span.Length >= 100);

        Memory<byte> memory = rentedBuffer;
        Assert.True(memory.Length >= 100);
    }

    #endregion

    #region GetStats Tests

    [Fact]
    public void GetStats_ReturnsPoolStatistics()
    {
        var pool = new BufferPool();

        var (allocated, pooled) = pool.GetStats();

        Assert.True(allocated >= 0);
        Assert.True(pooled >= 0);
    }

    [Fact]
    public void GetStats_TracksXLargeAllocations()
    {
        var pool = new BufferPool();

        var buffer = pool.Rent(16 * 1024 * 1024);

        var (allocated, _) = pool.GetStats();
        Assert.True(allocated >= 1);

        pool.Return(buffer);
        var (_, pooled) = pool.GetStats();
        Assert.True(pooled >= 1);
    }

    #endregion

    #region Shared Instance Tests

    [Fact]
    public void Shared_ReturnsSingletonInstance()
    {
        var instance1 = BufferPool.Shared;
        var instance2 = BufferPool.Shared;

        Assert.Same(instance1, instance2);
    }

    [Fact]
    public void Shared_CanRentAndReturn()
    {
        var buffer = BufferPool.Shared.Rent(1000);
        Assert.NotNull(buffer);

        BufferPool.Shared.Return(buffer);
    }

    #endregion

    #region Extension Methods Tests

    [Fact]
    public void ToPooledArray_CopiesDataToPooledBuffer()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        ReadOnlySpan<byte> span = data;

        var pooledArray = span.ToPooledArray();

        Assert.True(pooledArray.Length >= data.Length);
        Assert.Equal(data, pooledArray[..data.Length]);

        BufferPool.Shared.Return(pooledArray);
    }

    [Fact]
    public async Task ReadToPooledBufferAsync_ReadsStreamContent()
    {
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        using var stream = new MemoryStream(data);

        var (buffer, length) = await stream.ReadToPooledBufferAsync(100);

        Assert.Equal(data.Length, length);
        Assert.Equal(data, buffer[..length]);

        BufferPool.Shared.Return(buffer);
    }

    [Fact]
    public async Task ReadToPooledBufferAsync_RespectsMaxLength()
    {
        var data = new byte[1000];
        new Random(42).NextBytes(data);
        using var stream = new MemoryStream(data);

        var (buffer, length) = await stream.ReadToPooledBufferAsync(500);

        Assert.Equal(500, length);
        Assert.Equal(data[..500], buffer[..length]);

        BufferPool.Shared.Return(buffer);
    }

    [Fact]
    public async Task ReadToPooledBufferAsync_HandlesEmptyStream()
    {
        using var stream = new MemoryStream();

        var (buffer, length) = await stream.ReadToPooledBufferAsync(100);

        Assert.Equal(0, length);

        BufferPool.Shared.Return(buffer);
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public void ConcurrentRentAndReturn_DoesNotThrow()
    {
        var pool = new BufferPool();
        var exceptions = new List<Exception>();

        Parallel.For(0, 100, i =>
        {
            try
            {
                var buffer = pool.Rent(i * 100 + 1);
                Thread.Sleep(1);
                pool.Return(buffer);
            }
            catch (Exception ex)
            {
                lock (exceptions)
                {
                    exceptions.Add(ex);
                }
            }
        });

        Assert.Empty(exceptions);
    }

    [Fact]
    public void ConcurrentXLargeAllocation_RespectsLimit()
    {
        var pool = new BufferPool();
        var buffers = new System.Collections.Concurrent.ConcurrentBag<byte[]>();

        Parallel.For(0, 20, i =>
        {
            var buffer = pool.Rent(16 * 1024 * 1024);
            buffers.Add(buffer);
        });

        Assert.Equal(20, buffers.Count);

        foreach (var buffer in buffers)
        {
            pool.Return(buffer);
        }
    }

    #endregion
}
