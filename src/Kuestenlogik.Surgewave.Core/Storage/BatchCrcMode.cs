namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// How an append treats the CRC-32C field of an incoming RecordBatch (#85).
/// The CRC covers bytes 21.. (attributes onward); baseOffset, length, epoch and magic lie outside
/// the covered region, so stamping the offset never invalidates a producer's CRC — which is what
/// makes validating it possible at all.
/// </summary>
public enum BatchCrcMode
{
    /// <summary>
    /// Legacy: recompute the CRC and overwrite the field. Heals foreign bytes silently — including
    /// genuinely corrupt ones. The default, so <c>default(BatchCrcMode)</c> is the old behaviour.
    /// </summary>
    Recompute = 0,

    /// <summary>
    /// Compute once and compare against the stored CRC; a mismatch rejects the batch with
    /// <see cref="Exceptions.DataCorruptionException"/> (Kafka wire: CorruptMessage). Costs the
    /// same single pass as <see cref="Recompute"/> plus a four-byte compare.
    /// </summary>
    Validate = 1,

    /// <summary>
    /// Skip the CRC pass entirely. ONLY for batches that just came out of
    /// <see cref="RecordBatchSerializer"/>, whose CRC was written moments ago — anything else
    /// persists a CRC nobody checked, which read-side validation would later report as corruption.
    /// </summary>
    Trusted = 2,
}
