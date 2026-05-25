namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Retry policy configuration for a pipeline node (frontend model).
/// </summary>
public sealed record RetryPolicy(
    int MaxRetries = 3,
    long BackoffMs = 1000,
    double BackoffMultiplier = 2.0,
    long MaxBackoffMs = 30000,
    bool Enabled = true);
