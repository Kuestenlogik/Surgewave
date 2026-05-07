namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Retry policy configuration for a pipeline node.
/// </summary>
public sealed record RetryPolicy(
    int MaxRetries = 3,
    long BackoffMs = 1000,
    double BackoffMultiplier = 2.0,
    long MaxBackoffMs = 30000,
    bool Enabled = true);
