using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Filters expired record batches from fetch responses.
/// Batches whose TTL has elapsed (expiry time is in the past) are excluded.
/// Typed on the neutral <see cref="ITtlIndex"/> seam so the fetch slow path can run in a
/// protocol plugin without referencing the broker engine (#59 b4-tier2).
/// </summary>
public static class TtlFilter
{
    /// <summary>
    /// Filter out record batches that have expired (expiry time in the past).
    /// </summary>
    public static List<byte[]> FilterExpiredBatches(
        List<byte[]> batches,
        ITtlIndex ttlIndex,
        TopicPartition partition,
        long currentTimeMs)
    {
        var result = new List<byte[]>(batches.Count);

        foreach (var batch in batches)
        {
            var baseOffset = GetBaseOffset(batch);
            if (baseOffset >= 0 && ttlIndex.IsExpired(partition, baseOffset, currentTimeMs))
            {
                continue; // Skip expired batch
            }

            result.Add(batch);
        }

        return result;
    }

    private static long GetBaseOffset(ReadOnlySpan<byte> batch)
    {
        if (batch.Length < 8)
            return -1;

        return BinaryPrimitives.ReadInt64BigEndian(batch[..8]);
    }
}
