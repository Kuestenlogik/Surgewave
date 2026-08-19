using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral content-deduplication seam consumed by the data-plane handler. Optional:
/// injected as <c>null</c> when deduplication is disabled (#59 b4-tier2). The concrete
/// <c>DeduplicationManager</c> lives in the broker engine.
/// </summary>
public interface IDeduplicationManager
{
    /// <summary>
    /// Check if a record batch is a duplicate. Does NOT register the hash — call
    /// <see cref="Register"/> after a successful write.
    /// </summary>
    DeduplicationResult CheckDuplicate(TopicPartition partition, ReadOnlySpan<byte> recordBatch);

    /// <summary>
    /// Register the hash from a preceding <see cref="CheckDuplicate"/> after a successful write.
    /// </summary>
    /// <remarks>
    /// Takes the hash rather than the bytes on purpose. The check runs before the record transform
    /// (so a duplicate costs no transform) while the registration runs after the append (so an
    /// offset exists), and passing bytes across that gap meant the caller had to keep the
    /// PRE-transform buffer alive and pick it correctly — it did not, and dedup was silently inert
    /// on every transform-bound topic. A hash cannot be picked wrongly. It also drops a second
    /// XxHash64 pass over the payload from the produce path.
    /// </remarks>
    void Register(TopicPartition partition, ulong contentHash, long offset);
}
