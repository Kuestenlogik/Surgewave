namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Client quota information.
/// </summary>
public record ClientQuotaInfo(
    string ClientId,
    long ProduceRate,
    long FetchRate,
    bool IsThrottled);
