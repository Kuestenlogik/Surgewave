using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Filters delayed record batches from fetch responses.
/// Batches whose delivery time is in the future are excluded.
/// Typed on the neutral <see cref="IDelayIndex"/> seam so the fetch slow path can run in a
/// protocol plugin without referencing the broker engine (#59 b4-tier2).
/// </summary>
public static class DelayFilter
{
    /// <summary>
    /// Filter out record batches that are still delayed (delivery time in the future).
    /// </summary>
    public static List<byte[]> FilterDelayedBatches(
        List<byte[]> batches,
        IDelayIndex delayIndex,
        TopicPartition partition,
        long currentTimeMs)
    {
        var result = new List<byte[]>(batches.Count);

        foreach (var batch in batches)
        {
            var baseOffset = GetBaseOffset(batch);
            if (baseOffset >= 0 && delayIndex.IsDelayed(partition, baseOffset, currentTimeMs))
            {
                continue; // Skip delayed batch
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
