using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral delayed-delivery index seam consumed by the data-plane handler and the
/// <see cref="DelayFilter"/> fetch filter. Optional: injected as <c>null</c> when delayed
/// delivery is disabled (#59 b4-tier2). The concrete <c>DelayIndex</c> lives in the broker engine.
/// </summary>
public interface IDelayIndex
{
    /// <summary>Record a delayed batch in the index.</summary>
    void RecordDelayedBatch(TopicPartition partition, long offset, long deliverAtMs);

    /// <summary>Fast O(1) check whether a partition has any delayed records.</summary>
    bool HasDelayedRecords(TopicPartition partition);

    /// <summary>Check if a specific offset is delayed (delivery time is in the future).</summary>
    bool IsDelayed(TopicPartition partition, long offset, long currentTimeMs);
}
