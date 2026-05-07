namespace Kuestenlogik.Surgewave.Broker.Tenancy;

public enum TenantQuotaCheckResult
{
    Allowed,
    Throttled,  // Temporarily over quota, should retry after delay
    Rejected    // Hard limit exceeded, reject request
}
