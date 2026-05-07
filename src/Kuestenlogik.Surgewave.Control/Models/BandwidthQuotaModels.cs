namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Bandwidth quota configuration response model.
/// </summary>
public sealed record BandwidthQuotaConfigModel(
    bool Enabled,
    long DefaultProduceBytesPerSec,
    long DefaultConsumeBytesPerSec,
    int EnforcementWindowMs,
    double ThrottleDelayFactor,
    Dictionary<string, BandwidthQuotaOverrideModel> ClientOverrides,
    Dictionary<string, BandwidthQuotaOverrideModel> UserOverrides);

/// <summary>
/// Bandwidth quota override for a client or user.
/// </summary>
public sealed record BandwidthQuotaOverrideModel(long ProduceBytesPerSec, long ConsumeBytesPerSec);

/// <summary>
/// Bandwidth usage for a single client.
/// </summary>
public sealed record BandwidthUsageModel(
    string ClientId,
    long ProduceBytesPerSec,
    long ConsumeBytesPerSec,
    long ProduceLimitBytesPerSec,
    long ConsumeLimitBytesPerSec,
    double ProduceUtilizationPercent,
    double ConsumeUtilizationPercent,
    bool IsThrottled,
    DateTimeOffset LastActivityAt);

/// <summary>
/// Bandwidth quota metrics response model.
/// </summary>
public sealed record BandwidthQuotaMetricsModel(
    long TotalClientsTracked,
    long TotalClientsThrottled,
    long TotalBytesThrottled,
    long TotalThrottleEvents);

/// <summary>
/// Wrapper for listing all bandwidth quota usage.
/// </summary>
public sealed record BandwidthQuotaListModel(IReadOnlyList<BandwidthUsageModel> Clients);
