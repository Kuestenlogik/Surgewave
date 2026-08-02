namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Segments that were dropped from a partition but whose files could not be deleted yet, kept so the
/// deletion is retried instead of forgotten.
///
/// <para><b>Why deletion can fail.</b> A fetch serves its bytes straight out of the segment, and with
/// the File engine that read can hold a memory-mapped view. Windows refuses to delete a file that has
/// a live mapping (<c>ERROR_USER_MAPPED_FILE</c>) — so a retention pass that happens to overlap an
/// in-flight read cannot remove the file at that moment. On Linux the unlink succeeds and the file
/// disappears once the last mapping closes, which is why this only bites on Windows.</para>
///
/// <para><b>Why forgetting is not an option.</b> The partition has already removed the segment from
/// its list and moved <c>LogStartOffset</c> past it, so the data is gone as far as the running broker
/// is concerned. The file, however, is still on disk — and <c>LoadExistingSegments</c> picks it up on
/// the next start. Records that retention deleted would come back, which is a data-retention
/// violation, not a leak. Retrying keeps runtime state and disk in agreement.</para>
/// </summary>
internal sealed class PendingSegmentDeletions
{
    private readonly Lock _gate = new();
    private readonly List<ILogSegment> _segments = [];

    /// <summary>Segments still waiting to be deleted.</summary>
    public int Count
    {
        get { lock (_gate) return _segments.Count; }
    }

    /// <summary>
    /// Deletes the segment's files, or remembers it for a later retry if they are still pinned.
    /// </summary>
    /// <returns><see langword="true"/> if the files are gone.</returns>
    public bool DeleteOrDefer(ILogSegment segment)
    {
        if (TryDelete(segment))
            return true;

        lock (_gate)
        {
            _segments.Add(segment);
        }

        return false;
    }

    /// <summary>
    /// Retries every deferred deletion. Called at the start of each retention pass, so a reader that
    /// has since finished releases the file without any extra scheduling.
    /// </summary>
    /// <returns>How many segments were finally deleted.</returns>
    public int RetryPending()
    {
        List<ILogSegment> pending;
        lock (_gate)
        {
            if (_segments.Count == 0)
                return 0;

            pending = [.. _segments];
            _segments.Clear();
        }

        var deleted = 0;
        List<ILogSegment>? stillPinned = null;

        foreach (var segment in pending)
        {
            if (TryDelete(segment))
                deleted++;
            else
                (stillPinned ??= []).Add(segment);
        }

        if (stillPinned is not null)
        {
            lock (_gate)
            {
                _segments.AddRange(stillPinned);
            }
        }

        return deleted;
    }

    private static bool TryDelete(ILogSegment segment)
    {
        try
        {
            segment.DeleteFiles();
            return true;
        }
        catch (IOException)
        {
            // The file is pinned right now — a mapped view from an in-flight read. Retry later.
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            // Windows surfaces a pinned file this way too, depending on the handle that holds it.
            return false;
        }
    }
}
