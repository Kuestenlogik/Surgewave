namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Result of validating a reassignment plan before execution.
/// Contains lists of errors (blocking) and warnings (informational).
/// </summary>
public sealed record ReassignmentValidation(
    bool IsValid,
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> Warnings);
