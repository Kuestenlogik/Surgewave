using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>Where producer ids come from.</summary>
public interface IProducerIdSource
{
    /// <summary>The next id; never one handed out before.</summary>
    long Next();

    /// <summary>An id that is in use already, read back from state; the source never hands it out.</summary>
    void Observe(long producerId);
}

/// <summary>
/// Producer ids that are unique in the cluster and over restarts (#223): <c>brokerId · 2^40 + n</c>, so the ranges of
/// two brokers never meet, with <c>n</c> taken from blocks this broker reserves before it uses them. The end of the
/// reserved range is written to the data directory, synced, before an id of a new block goes out — a restart
/// continues behind it, as Kafka's blocks of KIP-730 do.
/// </summary>
public sealed class ProducerIdBlocks : IProducerIdSource
{
    /// <summary>Ids reserved at a time — Kafka's block size.</summary>
    public const int BlockSize = 1000;

    private const int CounterBits = 40;
    private const long CounterLimit = 1L << CounterBits;

    private readonly string _path;
    private readonly long _brokerBase;
    private readonly Lock _lock = new();
    private long _next;
    private long _reservedEnd;

    /// <param name="brokerId">This broker's id: unique in the cluster, below 2^23 so every id stays positive.</param>
    /// <param name="dataDirectory">Where the reserved range is kept.</param>
    public ProducerIdBlocks(int brokerId, string dataDirectory)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(brokerId);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(brokerId, 1 << 23);

        var directory = Path.Combine(dataDirectory, ".metadata");
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "producer-id-block");
        _brokerBase = (long)brokerId << CounterBits;

        // Everything up to the stored end may have been handed out; the first id after a start is past it. Id 0 is
        // not used, as before.
        _next = Math.Max(1, ReadReservedEnd());
        _reservedEnd = _next;
    }

    public long Next()
    {
        lock (_lock)
        {
            if (_next >= _reservedEnd)
                Reserve(_next + BlockSize);
            return _brokerBase + _next++;
        }
    }

    // Every id this broker handed out lies below the stored end; ids of other brokers are in other ranges.
    public void Observe(long producerId)
    {
    }

    // Under _lock. The range is on disk before any id of it is used.
    private void Reserve(long end)
    {
        if (end >= CounterLimit)
            throw new InvalidOperationException($"Producer ids of broker {_brokerBase >> CounterBits} are exhausted.");

        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, end);
        var temporary = _path + ".tmp";
        using (var file = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            file.Write(bytes);
            file.Flush(flushToDisk: true);
        }
        File.Move(temporary, _path, overwrite: true);
        _reservedEnd = end;
    }

    private long ReadReservedEnd()
    {
        if (!File.Exists(_path))
            return 0;
        var bytes = File.ReadAllBytes(_path);
        return bytes.Length >= 8 ? BinaryPrimitives.ReadInt64BigEndian(bytes) : 0;
    }
}

/// <summary>A counter from 1 that lives as long as the process: for a coordinator under test.</summary>
public sealed class InMemoryProducerIds : IProducerIdSource
{
    private long _next;

    public long Next() => Interlocked.Increment(ref _next);

    public void Observe(long producerId)
    {
        long current;
        while (producerId > (current = Volatile.Read(ref _next)))
            Interlocked.CompareExchange(ref _next, producerId, current);
    }
}
