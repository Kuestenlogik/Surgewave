namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Result of a version compatibility check between a local broker and cluster members.
/// </summary>
public sealed record CompatibilityResult(
    bool IsCompatible,
    string? Reason,
    IReadOnlyList<string> Warnings);
