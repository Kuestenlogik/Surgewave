using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral TTL index seam consumed by the data-plane handler and the
/// <see cref="TtlFilter"/> fetch filter. Optional: injected as <c>null</c> when TTL is disabled
/// (#59 b4-tier2). The concrete <c>TtlIndex</c> lives in the broker engine.
/// </summary>
public interface ITtlIndex
{
    /// <summary>Record a batch with TTL in the index.</summary>
    void RecordTtlBatch(TopicPartition partition, long offset, long expiryMs);

    /// <summary>Fast O(1) check whether a partition has any TTL-tracked records.</summary>
    bool HasTtlRecords(TopicPartition partition);

    /// <summary>Check if a specific offset has expired (expiry time is in the past).</summary>
    bool IsExpired(TopicPartition partition, long offset, long currentTimeMs);
}
