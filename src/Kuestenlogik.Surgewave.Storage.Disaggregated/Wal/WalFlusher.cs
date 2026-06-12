using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Tiering;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Wal;

/// <summary>
/// AutoMQ-style WAL-to-object-store flusher (ADR-014, P2b). One
/// instance watches an arbitrary set of disaggregated-wal partitions.
/// Per scan it asks the <see cref="IWalSegmentSource"/> for sealed
/// segments, filters out anything already past the manifest's tail,
/// then for each newly sealed segment uploads the bytes via the
/// configured <see cref="IRemoteStorageProvider"/> and appends a
/// <see cref="StreamObjectRef"/> to the partition manifest.
///
/// Designed to be testable without spinning a broker: every dependency
/// is injected, the per-scan work is exposed on
/// <see cref="RunOnceAsync"/>. The background loop is a thin wrapper
/// in <see cref="RunForeverAsync"/>.
///
/// WAL trimming (deleting flushed segments from disk) is intentionally
/// NOT done here — that belongs in P2d, after the read path knows how
/// to fall through to the manifest. Until then segments stay on disk
/// after flush; the cost is duplicated storage during the overlap
/// window, the win is a trivial rollback if the manifest commit fails.
/// </summary>
public sealed class WalFlusher
{
    private readonly IWalSegmentSource _segments;
    private readonly IPartitionManifestStore _manifests;
    private readonly IRemoteStorageProvider _remote;
    private readonly WalFlusherOptions _options;
    private readonly ILogger<WalFlusher> _logger;

    public WalFlusher(
        IWalSegmentSource segments,
        IPartitionManifestStore manifests,
        IRemoteStorageProvider remote,
        WalFlusherOptions? options = null,
        ILogger<WalFlusher>? logger = null)
    {
        _segments = segments;
        _manifests = manifests;
        _remote = remote;
        _options = options ?? new WalFlusherOptions();
        _logger = logger ?? NullLogger<WalFlusher>.Instance;
    }

    /// <summary>
    /// Flush every sealed segment that is not yet in the manifest for
    /// <paramref name="partition"/>. Returns the number of segments
    /// flushed in this call (0 when there is nothing new). Errors on
    /// any one segment bubble — the caller decides whether to retry
    /// the partition on the next scan.
    /// </summary>
    public async Task<int> RunOnceAsync(TopicPartition partition, CancellationToken cancellationToken = default)
    {
        var sealedSegments = await _segments.ListSealedAsync(partition, cancellationToken).ConfigureAwait(false);
        if (sealedSegments.Count == 0) return 0;

        var manifest = await _manifests.GetAsync(partition, cancellationToken).ConfigureAwait(false);
        var lastFlushedOffset = manifest.LastOffset ?? -1;

        var pending = sealedSegments
            .Where(s => s.BaseOffset > lastFlushedOffset)
            .OrderBy(s => s.BaseOffset)
            .Take(_options.MaxSegmentsPerScan)
            .ToList();

        var flushed = 0;
        foreach (var segment in pending)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await FlushOneAsync(segment, cancellationToken).ConfigureAwait(false);
            flushed++;
        }
        return flushed;
    }

    /// <summary>
    /// Background loop. Calls <see cref="RunOnceAsync"/> for every
    /// partition in <paramref name="partitions"/> every
    /// <see cref="WalFlusherOptions.PollInterval"/>. The set of
    /// partitions can be mutated externally between scans — the loop
    /// re-reads it on every iteration.
    /// </summary>
    public async Task RunForeverAsync(
        Func<IReadOnlyList<TopicPartition>> partitions,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var p in partitions())
            {
                if (cancellationToken.IsCancellationRequested) return;
                try
                {
                    var n = await RunOnceAsync(p, cancellationToken).ConfigureAwait(false);
                    if (n > 0)
                    {
                        _logger.LogInformation("WAL-flush: {Topic}/{Partition} → {Count} stream object(s)",
                            p.Topic, p.Partition, n);
                    }
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "WAL-flush failed for {Topic}/{Partition}; will retry next scan",
                        p.Topic, p.Partition);
                }
            }

            try
            {
                await Task.Delay(_options.PollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task FlushOneAsync(WalSealedSegment segment, CancellationToken cancellationToken)
    {
        var logBytes = await segment.ReadLogBytesAsync(cancellationToken).ConfigureAwait(false);
        var indexBytes = await segment.ReadIndexBytesAsync(cancellationToken).ConfigureAwait(false);
        var timeIndexBytes = await segment.ReadTimeIndexBytesAsync(cancellationToken).ConfigureAwait(false);

        await _remote.UploadSegmentAsync(
            segment.Partition.Topic,
            segment.Partition.Partition,
            segment.BaseOffset,
            logBytes,
            indexBytes,
            timeIndexBytes,
            cancellationToken).ConfigureAwait(false);

        var manifestRef = new StreamObjectRef(
            ObjectKey: StreamObjectKeyConvention.Build(segment.Partition, segment.BaseOffset),
            FirstOffset: segment.BaseOffset,
            LastOffset: segment.LastOffset,
            BytesOnDisk: segment.SizeBytes,
            CreatedAt: segment.CreatedAt);

        await _manifests.AppendObjectAsync(segment.Partition, manifestRef, cancellationToken).ConfigureAwait(false);

        // Trim runs strictly AFTER the manifest commit succeeds. A failed
        // upload or commit above throws and we never reach here, so the
        // local segment file is preserved for the next scan to retry.
        // Trim itself is allowed to fail (logged + swallowed): the
        // manifest is the source of truth; a stray local file just costs
        // disk until the retention sweeper picks it up.
        if (_options.TrimAfterFlush && segment.TrimAsync is not null)
        {
            try
            {
                await segment.TrimAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "WAL-trim failed for {Topic}/{Partition} offset {Offset}; segment stays local",
                    segment.Partition.Topic, segment.Partition.Partition, segment.BaseOffset);
            }
        }
    }
}
