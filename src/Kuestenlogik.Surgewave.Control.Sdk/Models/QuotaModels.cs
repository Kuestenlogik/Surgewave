namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Response representing quota configuration.
/// </summary>
public sealed record QuotaConfigModel(
    bool Enabled,
    long ProducerBytesPerSecond,
    long ProducerBurstBytes,
    long ConsumerBytesPerSecond,
    long ConsumerBurstBytes,
    int MaxThrottleTimeMs,
    int ClientInactivityTimeoutMinutes);

/// <summary>
/// Request to update quota configuration.
/// </summary>
public sealed record UpdateQuotaConfigRequest(
    bool? Enabled = null,
    long? ProducerBytesPerSecond = null,
    long? ProducerBurstBytes = null,
    long? ConsumerBytesPerSecond = null,
    long? ConsumerBurstBytes = null,
    int? MaxThrottleTimeMs = null,
    int? ClientInactivityTimeoutMinutes = null);

/// <summary>
/// Response representing client quota statistics.
/// </summary>
public sealed record ClientQuotaStatsModel(
    string ClientId,
    long TotalProducedBytes,
    long TotalFetchedBytes,
    int ProduceThrottleCount,
    int FetchThrottleCount,
    long AvailableProduceTokens,
    long AvailableFetchTokens,
    DateTime LastActivity);
