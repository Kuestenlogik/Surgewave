namespace Kuestenlogik.Surgewave.Storage.Disaggregated.Read;

/// <summary>
/// What an <see cref="IDisaggregatedSegmentReader"/> returns for one
/// fetch call. The bytes are the raw RecordBatch payload from the
/// stream object that contains <c>StartOffset</c>; the caller is
/// responsible for slicing/decoding records — this layer is byte-in,
/// byte-out.
///
/// Lives in Broker.Abstractions (namespace kept as
/// <c>Kuestenlogik.Surgewave.Storage.Disaggregated.Read</c>) so protocol plugins can consume
/// the neutral read seam without referencing the storage engine (#59 b4-tier2).
/// </summary>
/// <param name="LogBytes">The contents of the stream object. Empty when <see cref="HitManifest"/> is false.</param>
/// <param name="HitManifest">
/// True when the requested offset was found in the manifest and bytes
/// were fetched from the remote store. False means the offset is past
/// the manifest tail (a freshly produced batch that has not been
/// flushed yet) — the caller should serve from the local WAL.
/// </param>
/// <param name="NextOffset">
/// The offset after the last record in <see cref="LogBytes"/>. Equal
/// to the StreamObject's <c>LastOffset + 1</c>. Callers use this to
/// chain subsequent reads. <c>null</c> when <see cref="HitManifest"/>
/// is false.
/// </param>
public readonly record struct DisaggregatedReadResult(
    ReadOnlyMemory<byte> LogBytes,
    bool HitManifest,
    long? NextOffset)
{
    public static DisaggregatedReadResult MissedManifest() =>
        new(ReadOnlyMemory<byte>.Empty, HitManifest: false, NextOffset: null);
}
