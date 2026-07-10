namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Quota statistics for a client.
/// </summary>
public sealed record ClientQuotaStats
{
    public long TotalProducedBytes { get; init; }
    public long TotalFetchedBytes { get; init; }
    public int ProduceThrottleCount { get; init; }
    public int FetchThrottleCount { get; init; }
    public long AvailableProduceTokens { get; init; }
    public long AvailableFetchTokens { get; init; }
    public DateTime LastActivity { get; init; }
}
