namespace Kuestenlogik.Surgewave.Broker.AutoTuning;

/// <summary>
/// Protocol-neutral cold-start workload seam consumed by the data-plane handler's produce path.
/// Optional: injected as <c>null</c> when the profiler is not running (#59 b4-tier2). The
/// concrete <c>ColdStartWorkloadProfiler</c> lives in the broker engine.
/// </summary>
public interface IColdStartProfiler
{
    /// <summary>Record one Produce event (record count + byte count) for the given topic.</summary>
    void RecordProduce(string topic, long recordCount, long byteCount);
}
