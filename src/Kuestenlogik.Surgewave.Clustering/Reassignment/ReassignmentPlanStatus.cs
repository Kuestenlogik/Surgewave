namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Status of a reassignment plan lifecycle.
/// </summary>
public enum ReassignmentPlanStatus
{
    /// <summary>
    /// Plan has been proposed but not yet started.
    /// </summary>
    Proposed,

    /// <summary>
    /// Plan is currently executing.
    /// </summary>
    Executing,

    /// <summary>
    /// All assignments in the plan completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// One or more assignments failed.
    /// </summary>
    Failed,

    /// <summary>
    /// Plan was cancelled by the operator.
    /// </summary>
    Cancelled
}
