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

    /// <summary>Register a record batch hash after a successful write.</summary>
    void Register(TopicPartition partition, ReadOnlySpan<byte> recordBatch, long offset);
}
