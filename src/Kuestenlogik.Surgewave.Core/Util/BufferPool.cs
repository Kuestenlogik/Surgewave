using System.Buffers;
using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// High-performance buffer pool optimized for Kafka message processing.
/// Provides reusable byte buffers in common sizes to reduce GC pressure.
///
/// Buffer sizes are tuned for typical Kafka workloads:
/// - Small (4KB): Protocol headers, small messages
/// - Medium (64KB): Standard message batches
/// - Large (1MB): Large batches, bulk transfers
/// - XLarge (16MB): Maximum batch size scenarios
/// </summary>
public sealed class BufferPool
{
    /// <summary>
    /// Shared instance for global buffer pooling
    /// </summary>
    public static BufferPool Shared { get; } = new();

    private readonly ArrayPool<byte> _smallPool;      // 4KB buffers
    private readonly ArrayPool<byte> _mediumPool;     // 64KB buffers
    private readonly ArrayPool<byte> _largePool;      // 1MB buffers
    private readonly ConcurrentBag<byte[]> _xlargePool; // 16MB buffers (limited)

    private const int SmallBufferSize = KafkaConstants.BufferSizes.Small;
    private const int MediumBufferSize = KafkaConstants.BufferSizes.Medium;
    private const int LargeBufferSize = KafkaConstants.BufferSizes.Large;
    private const int XLargeBufferSize = KafkaConstants.BufferSizes.XLarge;
    private const int MaxXLargeBuffers = 8;

    private int _xlargeBufferCount;

    public BufferPool()
    {
        _smallPool = ArrayPool<byte>.Create(SmallBufferSize, 100);
        _mediumPool = ArrayPool<byte>.Create(MediumBufferSize, 50);
        _largePool = ArrayPool<byte>.Create(LargeBufferSize, 20);
        _xlargePool = new ConcurrentBag<byte[]>();
    }

    /// <summary>
    /// Rent a buffer of at least the specified size.
    /// The returned buffer may be larger than requested.
    /// </summary>
    public byte[] Rent(int minimumSize)
    {
        if (minimumSize <= SmallBufferSize)
        {
            return _smallPool.Rent(minimumSize);
        }
        if (minimumSize <= MediumBufferSize)
        {
            return _mediumPool.Rent(minimumSize);
        }
        if (minimumSize <= LargeBufferSize)
        {
            return _largePool.Rent(minimumSize);
        }
        if (minimumSize <= XLargeBufferSize)
        {
            if (_xlargePool.TryTake(out var buffer))
            {
                return buffer;
            }

            // Limit total XLarge allocations
            if (Interlocked.Increment(ref _xlargeBufferCount) <= MaxXLargeBuffers)
            {
                return new byte[XLargeBufferSize];
            }
            Interlocked.Decrement(ref _xlargeBufferCount);
        }

        // Fall through: allocate exact size for very large requests
        return new byte[minimumSize];
    }

    /// <summary>
    /// Return a buffer to the pool for reuse.
    /// </summary>
    public void Return(byte[] buffer)
    {
        if (buffer.Length <= SmallBufferSize)
        {
            _smallPool.Return(buffer, clearArray: false);
        }
        else if (buffer.Length <= MediumBufferSize)
        {
            _mediumPool.Return(buffer, clearArray: false);
        }
        else if (buffer.Length <= LargeBufferSize)
        {
            _largePool.Return(buffer, clearArray: false);
        }
        else if (buffer.Length == XLargeBufferSize && _xlargePool.Count < MaxXLargeBuffers)
        {
            _xlargePool.Add(buffer);
        }
        // Else: let GC collect it
    }

    /// <summary>
    /// Rent a buffer and return a disposable wrapper that automatically returns it.
    /// Usage: using var buffer = BufferPool.Shared.RentDisposable(size);
    /// </summary>
    public RentedBuffer RentDisposable(int minimumSize)
    {
        return new RentedBuffer(this, Rent(minimumSize));
    }

    /// <summary>
    /// Get pool statistics for monitoring
    /// </summary>
    public (int xlargeAllocated, int xlargePooled) GetStats()
    {
        return (_xlargeBufferCount, _xlargePool.Count);
    }
}

/// <summary>
/// A rented buffer that automatically returns to the pool when disposed.
/// </summary>
public readonly struct RentedBuffer : IDisposable
{
    private readonly BufferPool _pool;
    public byte[] Array { get; }
    public int Length => Array.Length;

    public Span<byte> Span => Array.AsSpan();
    public Memory<byte> Memory => Array.AsMemory();

    internal RentedBuffer(BufferPool pool, byte[] array)
    {
        _pool = pool;
        Array = array;
    }

    public void Dispose()
    {
        _pool.Return(Array);
    }

    public static implicit operator byte[](RentedBuffer buffer) => buffer.Array;
    public static implicit operator Span<byte>(RentedBuffer buffer) => buffer.Span;
    public static implicit operator Memory<byte>(RentedBuffer buffer) => buffer.Memory;
}

/// <summary>
/// Extension methods for working with pooled buffers
/// </summary>
public static class BufferPoolExtensions
{
    /// <summary>
    /// Copy data to a new pooled buffer of exact size
    /// </summary>
    public static byte[] ToPooledArray(this ReadOnlySpan<byte> data, BufferPool? pool = null)
    {
        pool ??= BufferPool.Shared;
        var buffer = pool.Rent(data.Length);
        data.CopyTo(buffer);
        return buffer;
    }

    /// <summary>
    /// Copy stream content to a pooled buffer
    /// </summary>
    public static async Task<(byte[] buffer, int length)> ReadToPooledBufferAsync(
        this Stream stream,
        int maxLength,
        BufferPool? pool = null,
        CancellationToken cancellationToken = default)
    {
        pool ??= BufferPool.Shared;
        var buffer = pool.Rent(maxLength);

        var totalRead = 0;
        while (totalRead < maxLength)
        {
            var read = await stream.ReadAsync(
                buffer.AsMemory(totalRead, maxLength - totalRead),
                cancellationToken);

            if (read == 0) break;
            totalRead += read;
        }

        return (buffer, totalRead);
    }
}
