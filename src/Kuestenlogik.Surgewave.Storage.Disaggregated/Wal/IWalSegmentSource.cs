using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// Abstraction that yields the sealed segments for a partition. Lets
/// the <see cref="WalFlusher"/> stay decoupled from <c>PartitionLog</c>:
/// production passes a wrapper around the live LogManager, tests pass
/// a hand-built list.
///
/// <para>
/// "Sealed" means the segment is no longer the partition's active
/// write target (a new segment has rolled in front of it). Already-
/// flushed segments are filtered by the WalFlusher against the
/// partition manifest's <see cref="PartitionManifest.LastOffset"/> —
/// the source itself does not need to remember what was flushed.
/// </para>
/// </summary>
public interface IWalSegmentSource
{
    /// <summary>
    /// Return the sealed segments currently on disk for
    /// <paramref name="partition"/>, ordered by
    /// <see cref="WalSealedSegment.BaseOffset"/> ascending.
    /// </summary>
    ValueTask<IReadOnlyList<WalSealedSegment>> ListSealedAsync(
        TopicPartition partition,
        CancellationToken cancellationToken = default);
}
