using System.Text;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Partition selection strategies for produce operations.
/// </summary>
public static class Partitioner
{
    private static int _roundRobinCounter;
    private static readonly object _stickyLock = new();
    private static int _stickyPartition = -1;
    private static int _stickyCount;
    private const int DefaultStickyBatchSize = 100;

    /// <summary>
    /// Round-robin partition selection.
    /// </summary>
    public static IPartitionStrategy RoundRobin { get; } = new RoundRobinStrategy();

    /// <summary>
    /// Key-based hash partition selection (murmur2 compatible with Kafka).
    /// </summary>
    public static IPartitionStrategy ByKey { get; } = new KeyHashStrategy();

    /// <summary>
    /// Sticky partition - stays on same partition until batch size reached.
    /// </summary>
    public static IPartitionStrategy Sticky { get; } = new StickyStrategy(DefaultStickyBatchSize);

    /// <summary>
    /// Create a sticky partitioner with custom batch size.
    /// </summary>
    public static IPartitionStrategy StickyWithBatchSize(int batchSize) => new StickyStrategy(batchSize);

    /// <summary>
    /// Random partition selection.
    /// </summary>
    public static IPartitionStrategy Random { get; } = new RandomStrategy();

    /// <summary>
    /// Create a custom partition strategy from a function.
    /// </summary>
    public static IPartitionStrategy Custom(Func<byte[]?, int, int> selector) => new CustomStrategy(selector);

    private sealed class RoundRobinStrategy : IPartitionStrategy
    {
        public int SelectPartition(byte[]? key, int partitionCount)
        {
            var current = Interlocked.Increment(ref _roundRobinCounter);
            return Math.Abs(current) % partitionCount;
        }
    }

    private sealed class KeyHashStrategy : IPartitionStrategy
    {
        public int SelectPartition(byte[]? key, int partitionCount)
        {
            if (key == null || key.Length == 0)
                return Partitioner.RoundRobin.SelectPartition(key, partitionCount);

            // Murmur2 hash compatible with Kafka's default partitioner
            var hash = Murmur2Hash(key);
            return Math.Abs(hash) % partitionCount;
        }

        private static int Murmur2Hash(byte[] data)
        {
            const uint seed = 0x9747b28c;
            const uint m = 0x5bd1e995;
            const int r = 24;

            var length = data.Length;
            var h = seed ^ (uint)length;
            var currentIndex = 0;

            while (length >= 4)
            {
                var k = (uint)(data[currentIndex++] | data[currentIndex++] << 8 | data[currentIndex++] << 16 | data[currentIndex++] << 24);
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
                length -= 4;
            }

            switch (length)
            {
                case 3: h ^= (uint)data[currentIndex + 2] << 16; goto case 2;
                case 2: h ^= (uint)data[currentIndex + 1] << 8; goto case 1;
                case 1: h ^= data[currentIndex]; h *= m; break;
            }

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return (int)h;
        }
    }

    private sealed class StickyStrategy(int batchSize) : IPartitionStrategy
    {
        public int SelectPartition(byte[]? key, int partitionCount)
        {
            lock (_stickyLock)
            {
                if (_stickyPartition < 0 || _stickyCount >= batchSize)
                {
                    _stickyPartition = System.Random.Shared.Next(partitionCount);
                    _stickyCount = 0;
                }
                _stickyCount++;
                return _stickyPartition;
            }
        }
    }

    private sealed class RandomStrategy : IPartitionStrategy
    {
        public int SelectPartition(byte[]? key, int partitionCount)
            => System.Random.Shared.Next(partitionCount);
    }

    private sealed class CustomStrategy(Func<byte[]?, int, int> selector) : IPartitionStrategy
    {
        public int SelectPartition(byte[]? key, int partitionCount)
            => selector(key, partitionCount);
    }
}

/// <summary>
/// Interface for partition selection strategies.
/// </summary>
public interface IPartitionStrategy
{
    /// <summary>
    /// Select a partition for the given key.
    /// </summary>
    /// <param name="key">Message key (may be null).</param>
    /// <param name="partitionCount">Total number of partitions.</param>
    /// <returns>Selected partition index.</returns>
    int SelectPartition(byte[]? key, int partitionCount);
}

/// <summary>
/// Extension methods for partition selection.
/// </summary>
public static class PartitionStrategyExtensions
{
    /// <summary>
    /// Select partition using string key.
    /// </summary>
    public static int SelectPartition(this IPartitionStrategy strategy, string? key, int partitionCount)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        return strategy.SelectPartition(keyBytes, partitionCount);
    }
}
