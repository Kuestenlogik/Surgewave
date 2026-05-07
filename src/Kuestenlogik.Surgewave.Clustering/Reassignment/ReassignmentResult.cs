namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Final result of executing a reassignment plan.
/// </summary>
public sealed record ReassignmentResult(
    string PlanId,
    ReassignmentPlanStatus Status,
    int TotalPartitions,
    int Completed,
    int Failed,
    TimeSpan Duration,
    long TotalBytesCopied);
