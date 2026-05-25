namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Quota configuration.
/// </summary>
public record QuotaConfig(
    long ProduceRateLimit,
    long FetchRateLimit,
    long RequestRateLimit,
    bool Enabled);
