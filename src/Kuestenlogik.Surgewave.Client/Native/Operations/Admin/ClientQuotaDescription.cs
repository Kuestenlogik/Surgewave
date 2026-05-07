namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Client quota description.
/// </summary>
public record ClientQuotaDescription(
    string ClientId,
    long ProduceRate,
    long FetchRate,
    long ProduceTokensAvailable,
    long FetchTokensAvailable,
    bool IsThrottled,
    long LastActivityMs);
