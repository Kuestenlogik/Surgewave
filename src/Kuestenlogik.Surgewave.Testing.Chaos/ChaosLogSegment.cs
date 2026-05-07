using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Wraps an <see cref="ILogSegment"/> to inject faults controlled by a <see cref="ChaosEngine"/>.
/// Checks for active faults before each operation and either throws, delays, or corrupts data.
/// </summary>
public sealed class ChaosLogSegment : ILogSegment
{
    private readonly ILogSegment _inner;
    private readonly ChaosEngine _engine;
    private readonly int _brokerId;
    private readonly ILogger _logger;

    [ThreadStatic]
    private static Random? t_random;

    private static Random Random => t_random ??= new Random();

    /// <summary>
    /// Creates a new chaos log segment wrapping the inner segment.
    /// </summary>
    /// <param name="inner">The actual log segment to delegate to.</param>
    /// <param name="engine">The chaos engine controlling fault injection.</param>
    /// <param name="brokerId">The broker ID this segment belongs to.</param>
    /// <param name="logger">Logger for diagnostics.</param>
    public ChaosLogSegment(ILogSegment inner, ChaosEngine engine, int brokerId, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(logger);

        _inner = inner;
        _engine = engine;
        _brokerId = brokerId;
        _logger = logger;
    }

    /// <inheritdoc />
    public long BaseOffset => _inner.BaseOffset;

    /// <inheritdoc />
    public long CurrentOffset => _inner.CurrentOffset;

    /// <inheritdoc />
    public long Size => _inner.Size;

    /// <inheritdoc />
    public bool IsFull => _inner.IsFull;

    /// <inheritdoc />
    public DateTime CreatedAt => _inner.CreatedAt;

    /// <inheritdoc />
    public long MaxTimestamp => _inner.MaxTimestamp;

    /// <inheritdoc />
    public long? GetFirstMessageOffset()
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        return _inner.GetFirstMessageOffset();
    }

    /// <inheritdoc />
    public async ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        byte[] recordBatch, CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        ThrowIfStorageFull();
        await InjectLatencyAsync(cancellationToken);

        if (_engine.IsFaultActive(FaultType.PartialWrite, _brokerId))
        {
            // Simulate partial write by truncating the data
            var partialLength = Math.Max(1, recordBatch.Length / 2);
            var partial = new byte[partialLength];
            Array.Copy(recordBatch, partial, partialLength);
            _logger.LogWarning("Chaos: Injecting partial write ({PartialLength}/{FullLength} bytes) on broker {BrokerId}",
                partialLength, recordBatch.Length, _brokerId);
            return await _inner.AppendBatchAsync(partial, cancellationToken);
        }

        return await _inner.AppendBatchAsync(recordBatch, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        byte[] buffer, int offset, int length, CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        ThrowIfStorageFull();
        await InjectLatencyAsync(cancellationToken);

        if (_engine.IsFaultActive(FaultType.PartialWrite, _brokerId))
        {
            var partialLength = Math.Max(1, length / 2);
            _logger.LogWarning("Chaos: Injecting partial write ({PartialLength}/{FullLength} bytes) on broker {BrokerId}",
                partialLength, length, _brokerId);
            return await _inner.AppendBatchAsync(buffer, offset, partialLength, cancellationToken);
        }

        return await _inner.AppendBatchAsync(buffer, offset, length, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<(long baseOffset, int recordCount)> AppendBatchAsync(
        ReadOnlyMemory<byte> recordBatch, CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        ThrowIfStorageFull();
        await InjectLatencyAsync(cancellationToken);

        if (_engine.IsFaultActive(FaultType.PartialWrite, _brokerId))
        {
            var partialLength = Math.Max(1, recordBatch.Length / 2);
            _logger.LogWarning("Chaos: Injecting partial write ({PartialLength}/{FullLength} bytes) on broker {BrokerId}",
                partialLength, recordBatch.Length, _brokerId);
            return await _inner.AppendBatchAsync(recordBatch[..partialLength], cancellationToken);
        }

        return await _inner.AppendBatchAsync(recordBatch, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        await InjectLatencyAsync(cancellationToken);
        await _inner.FlushAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<List<byte[]>> ReadBatchesAsync(
        long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        await InjectLatencyAsync(cancellationToken);

        var result = await _inner.ReadBatchesAsync(startOffset, maxBytes, cancellationToken);

        if (_engine.IsFaultActive(FaultType.MessageCorruption, _brokerId))
        {
            _logger.LogWarning("Chaos: Injecting message corruption on broker {BrokerId} for read at offset {Offset}",
                _brokerId, startOffset);
            return CorruptBatches(result);
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask<(ReadOnlyMemory<byte> Data, List<int> BatchOffsets)> ReadBatchesContiguousAsync(
        long startOffset, int maxBytes, CancellationToken cancellationToken = default)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        await InjectLatencyAsync(cancellationToken);

        var result = await _inner.ReadBatchesContiguousAsync(startOffset, maxBytes, cancellationToken);

        if (_engine.IsFaultActive(FaultType.MessageCorruption, _brokerId))
        {
            _logger.LogWarning("Chaos: Injecting message corruption on broker {BrokerId} for contiguous read at offset {Offset}",
                _brokerId, startOffset);
            var corrupted = CorruptMemory(result.Data);
            return (corrupted, result.BatchOffsets);
        }

        return result;
    }

    /// <inheritdoc />
    public long? GetFilePositionForOffset(long startOffset)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        return _inner.GetFilePositionForOffset(startOffset);
    }

    /// <inheritdoc />
    public long? FindOffsetByTimestamp(long targetTimestamp)
    {
        ThrowIfCrashed();
        ThrowIfDiskError();
        return _inner.FindOffsetByTimestamp(targetTimestamp);
    }

    /// <inheritdoc />
    public void DeleteFiles()
    {
        ThrowIfDiskError();
        _inner.DeleteFiles();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _inner.Dispose();
    }

    private void ThrowIfCrashed()
    {
        if (_engine.IsFaultActive(FaultType.NodeCrash, _brokerId))
        {
            throw new IOException($"Chaos: Node crash simulated on broker {_brokerId}");
        }
    }

    private void ThrowIfDiskError()
    {
        if (_engine.IsFaultActive(FaultType.DiskIoError, _brokerId))
        {
            throw new IOException($"Chaos: Disk I/O error simulated on broker {_brokerId}");
        }
    }

    private void ThrowIfStorageFull()
    {
        if (_engine.IsFaultActive(FaultType.StorageFullError, _brokerId))
        {
            throw new IOException($"Chaos: No space left on device (simulated) on broker {_brokerId}");
        }
    }

    private async ValueTask InjectLatencyAsync(CancellationToken cancellationToken)
    {
        var latency = _engine.GetInjectedLatency(FaultType.SlowNetwork, _brokerId);
        if (latency.HasValue)
        {
            _logger.LogDebug("Chaos: Injecting {Latency}ms latency on broker {BrokerId}",
                latency.Value.TotalMilliseconds, _brokerId);
            await Task.Delay(latency.Value, cancellationToken);
        }
    }

    private static List<byte[]> CorruptBatches(List<byte[]> batches)
    {
        var corrupted = new List<byte[]>(batches.Count);
        foreach (var batch in batches)
        {
            var copy = new byte[batch.Length];
            Array.Copy(batch, copy, batch.Length);
            FlipRandomBits(copy);
            corrupted.Add(copy);
        }
        return corrupted;
    }

    private static ReadOnlyMemory<byte> CorruptMemory(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty)
            return data;

        var copy = data.ToArray();
        FlipRandomBits(copy);
        return copy;
    }

    private static void FlipRandomBits(byte[] data)
    {
        if (data.Length == 0)
            return;

        // Flip 1-3 random bits to simulate subtle corruption
        var flips = Random.Next(1, 4);
        for (int i = 0; i < flips; i++)
        {
            var byteIndex = Random.Next(data.Length);
            var bitIndex = Random.Next(8);
            data[byteIndex] ^= (byte)(1 << bitIndex);
        }
    }
}
