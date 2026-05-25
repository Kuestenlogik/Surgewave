namespace Kuestenlogik.Surgewave.Clustering.Upgrades;

/// <summary>
/// Result of a graceful shutdown operation.
/// </summary>
public sealed record ShutdownResult(
    bool Success,
    int PartitionsTransferred,
    int ConnectionsClosed,
    TimeSpan Duration,
    IReadOnlyList<string> Warnings);
