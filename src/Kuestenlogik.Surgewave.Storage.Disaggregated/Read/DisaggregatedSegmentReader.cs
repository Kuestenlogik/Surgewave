using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Storage.Tiering;

namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Read;

/// <summary>
/// Read-side counterpart of <see cref="Wal.WalFlusher"/>. Given a
/// fetch request <c>(partition, startOffset)</c>, consults the
/// partition manifest and — if the offset lives in a flushed stream
/// object — downloads the object's bytes via
/// <see cref="IRemoteStorageProvider"/>.
///
/// Returns <see cref="DisaggregatedReadResult.MissedManifest"/> when
/// the offset is past the manifest's tail; the caller (P2d's broker
/// integration) then serves from the local WAL as normal. This keeps
/// the reader free of any WAL/log dependency — it only knows
/// "manifest + remote", which is what makes the disaggregated read
/// path testable in isolation.
/// </summary>
public sealed class DisaggregatedSegmentReader : IDisaggregatedSegmentReader
{
    private readonly IPartitionManifestStore _manifests;
    private readonly IRemoteStorageProvider _remote;

    public DisaggregatedSegmentReader(
        IPartitionManifestStore manifests,
        IRemoteStorageProvider remote)
    {
        _manifests = manifests;
        _remote = remote;
    }

    /// <summary>
    /// Try to fetch the stream-object covering <paramref name="startOffset"/>
    /// from the remote store. The remote payload is returned whole — slicing
    /// to <paramref name="maxBytes"/> is the caller's job (record-batch
    /// boundaries live in the decode layer, not here).
    /// </summary>
    public async Task<DisaggregatedReadResult> TryReadAsync(
        TopicPartition partition,
        long startOffset,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        // maxBytes is currently informational: this read returns the whole
        // stream-object payload because record-batch alignment is a
        // higher-layer concern. It stays in the signature so a future
        // version can implement range-fetch via
        // IRemoteStorageProvider.FetchLogSegmentAsync without breaking the
        // caller.
        _ = maxBytes;

        var manifest = await _manifests.GetAsync(partition, cancellationToken).ConfigureAwait(false);
        var entry = manifest.Locate(startOffset);
        if (entry is null)
        {
            return DisaggregatedReadResult.MissedManifest();
        }

        var (logBytes, _, _) = await _remote.DownloadSegmentAsync(
            partition.Topic,
            partition.Partition,
            entry.Value.FirstOffset,
            cancellationToken).ConfigureAwait(false);

        return new DisaggregatedReadResult(
            LogBytes: logBytes,
            HitManifest: true,
            NextOffset: entry.Value.LastOffset + 1);
    }
}
