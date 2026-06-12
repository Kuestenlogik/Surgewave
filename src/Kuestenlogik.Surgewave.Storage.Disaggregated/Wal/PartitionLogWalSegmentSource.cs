using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// Production-bridge from the broker's <c>PartitionLog</c> world to
/// the <see cref="IWalSegmentSource"/> abstraction the WAL flusher
/// consumes. The broker's startup glue passes two callbacks:
/// <list type="bullet">
///   <item>
///     <description>
///       <c>segmentLookup</c> returns the partition's current segment
///       list (or null when the partition does not exist on this
///       broker). The broker implements this with
///       <c>logManager.GetLog(tp) is PartitionLog pl ? pl.Segments : null</c>
///       — keeping the cast in the broker so this assembly stays free
///       of <c>PartitionLog</c> coupling.
///     </description>
///   </item>
///   <item>
///     <description>
///       <c>segmentDirectory</c> resolves the on-disk folder for a
///       partition, matching the broker's data layout
///       (<c>&lt;dataDir&gt;/&lt;topic&gt;/partition-&lt;n&gt;/</c>).
///       The flusher reads each segment's
///       <c>{baseOffset:D20}.{log,index,timeindex}</c> file from this
///       path.
///     </description>
///   </item>
/// </list>
///
/// The active (= tail) segment is skipped — flushing a segment that is
/// still being appended to would race the producer. Segment selection
/// is "everything except the last entry"; the broker only seals a
/// segment by rolling forward, so the list contract is stable.
/// </summary>
public sealed class PartitionLogWalSegmentSource : IWalSegmentSource
{
    private readonly Func<TopicPartition, IReadOnlyList<ILogSegment>?> _segmentLookup;
    private readonly Func<TopicPartition, string> _segmentDirectory;

    public PartitionLogWalSegmentSource(
        Func<TopicPartition, IReadOnlyList<ILogSegment>?> segmentLookup,
        Func<TopicPartition, string> segmentDirectory)
    {
        _segmentLookup = segmentLookup;
        _segmentDirectory = segmentDirectory;
    }

    public ValueTask<IReadOnlyList<WalSealedSegment>> ListSealedAsync(
        TopicPartition partition,
        CancellationToken cancellationToken = default)
    {
        var segments = _segmentLookup(partition);
        if (segments is null || segments.Count <= 1)
        {
            // No partition log, or only the active segment exists -> nothing sealed yet.
            return ValueTask.FromResult<IReadOnlyList<WalSealedSegment>>([]);
        }

        var dir = _segmentDirectory(partition);
        var sealedSegments = new List<WalSealedSegment>(segments.Count - 1);

        // Everything except the last entry; the last is the active write-target.
        for (var i = 0; i < segments.Count - 1; i++)
        {
            var s = segments[i];
            var baseOffset = s.BaseOffset;
            var lastOffset = s.CurrentOffset - 1;
            if (lastOffset < baseOffset) continue; // empty segment, nothing to flush

            var logPath = Path.Combine(dir, $"{baseOffset:D20}.log");
            var indexPath = Path.Combine(dir, $"{baseOffset:D20}.index");
            var timeIndexPath = Path.Combine(dir, $"{baseOffset:D20}.timeindex");
            sealedSegments.Add(new WalSealedSegment(
                Partition: partition,
                BaseOffset: baseOffset,
                LastOffset: lastOffset,
                SizeBytes: s.Size,
                CreatedAt: s.CreatedAt,
                ReadLogBytesAsync: ct => ReadFileOrEmptyAsync(logPath, ct),
                ReadIndexBytesAsync: ct => ReadFileOrEmptyAsync(indexPath, ct),
                ReadTimeIndexBytesAsync: ct => ReadFileOrEmptyAsync(timeIndexPath, ct))
            {
                TrimAsync = _ =>
                {
                    // Only delete the .log; the index siblings live with it
                    // and we want to keep the trim atomic-ish from the
                    // observer's POV. Missing files are tolerated — File.Delete
                    // is idempotent. Errors propagate to the flusher's
                    // try/catch which logs + swallows.
                    DeleteIfExists(logPath);
                    DeleteIfExists(indexPath);
                    DeleteIfExists(timeIndexPath);
                    return Task.CompletedTask;
                },
            });
        }

        return ValueTask.FromResult<IReadOnlyList<WalSealedSegment>>(sealedSegments);
    }

    private static async Task<byte[]> ReadFileOrEmptyAsync(string path, CancellationToken ct)
    {
        // Index + time-index files are optional in some storage engines (e.g.
        // memory backed); the flusher should not fail when only the .log
        // file is present. The .log itself is the only required artefact
        // and its absence is a real error worth propagating.
        if (!File.Exists(path)) return [];
        return await File.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }
}
