using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Read;

/// <summary>
/// Protocol-neutral disaggregated fetch seam. Given a fetch request
/// <c>(partition, fetchOffset)</c>, returns the flushed stream-object bytes when the offset
/// lives in the partition manifest, or <see cref="DisaggregatedReadResult.MissedManifest"/>
/// when it is past the manifest tail (the caller then serves from the local WAL). Optional:
/// injected as <c>null</c> when disaggregated storage is not wired (#59 b4-tier2). The concrete
/// <c>DisaggregatedSegmentReader</c> and its tiering internals stay in the storage engine.
/// </summary>
public interface IDisaggregatedSegmentReader
{
    /// <summary>
    /// Try to fetch the stream-object covering <paramref name="startOffset"/> from the remote
    /// store. The remote payload is returned whole — slicing to <paramref name="maxBytes"/> is
    /// the caller's job.
    /// </summary>
    Task<DisaggregatedReadResult> TryReadAsync(
        TopicPartition partition,
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default);
}
