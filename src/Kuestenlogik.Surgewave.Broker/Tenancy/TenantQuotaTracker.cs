using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// Per-tenant quota enforcement using token bucket algorithm.
/// Tracks produce and fetch byte rates per tenant and enforces limits defined in TenantPolicy.
/// </summary>
public sealed partial class TenantQuotaTracker
{
    private readonly ConcurrentDictionary<TenantId, TenantQuotaState> _states = new();
    private readonly ILogger<TenantQuotaTracker> _logger;

    public TenantQuotaTracker(ILogger<TenantQuotaTracker> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Checks whether a produce operation is allowed for the given tenant.
    /// Returns Allowed, Throttled, or Rejected based on the tenant's policy.
    /// </summary>
    public TenantQuotaCheckResult CheckProduceQuota(TenantId tenant, TenantPolicy policy, long bytes)
    {
        if (policy.MaxProduceBytesPerSecond < 0)
            return TenantQuotaCheckResult.Allowed;

        var state = GetOrCreateState(tenant);
        lock (state)
        {
            state.Refill(policy.MaxProduceBytesPerSecond, policy.MaxFetchBytesPerSecond);

            if (state.ProduceTokens >= bytes)
            {
                state.ProduceTokens -= bytes;
                return TenantQuotaCheckResult.Allowed;
            }

            LogProduceThrottled(tenant.Value, bytes, state.ProduceTokens);
            return TenantQuotaCheckResult.Throttled;
        }
    }

    /// <summary>
    /// Checks whether a fetch operation is allowed for the given tenant.
    /// Returns Allowed, Throttled, or Rejected based on the tenant's policy.
    /// </summary>
    public TenantQuotaCheckResult CheckFetchQuota(TenantId tenant, TenantPolicy policy, long bytes)
    {
        if (policy.MaxFetchBytesPerSecond < 0)
            return TenantQuotaCheckResult.Allowed;

        var state = GetOrCreateState(tenant);
        lock (state)
        {
            state.Refill(policy.MaxProduceBytesPerSecond, policy.MaxFetchBytesPerSecond);

            if (state.FetchTokens >= bytes)
            {
                state.FetchTokens -= bytes;
                return TenantQuotaCheckResult.Allowed;
            }

            LogFetchThrottled(tenant.Value, bytes, state.FetchTokens);
            return TenantQuotaCheckResult.Throttled;
        }
    }

    /// <summary>
    /// Records bytes produced by a tenant for tracking purposes.
    /// </summary>
    public void RecordProducedBytes(TenantId tenant, long bytes)
    {
        var state = GetOrCreateState(tenant);
        lock (state)
        {
            state.ProduceBytesTotal += bytes;
        }
    }

    /// <summary>
    /// Records bytes fetched by a tenant for tracking purposes.
    /// </summary>
    public void RecordFetchedBytes(TenantId tenant, long bytes)
    {
        var state = GetOrCreateState(tenant);
        lock (state)
        {
            state.FetchBytesTotal += bytes;
        }
    }

    /// <summary>
    /// Gets current resource usage statistics for a tenant.
    /// </summary>
    public TenantResourceUsage GetUsage(TenantId tenant)
    {
        if (!_states.TryGetValue(tenant, out var state))
        {
            return new TenantResourceUsage(tenant, 0, 0, 0, 0, 0, 0, 0);
        }

        lock (state)
        {
            return new TenantResourceUsage(
                TenantId: tenant,
                TopicCount: 0,
                PartitionCount: 0,
                ConsumerGroupCount: 0,
                StorageBytesEstimate: 0,
                ProduceBytesPerSecond: state.ProduceBytesTotal,
                FetchBytesPerSecond: state.FetchBytesTotal,
                ActiveConnections: 0);
        }
    }

    private TenantQuotaState GetOrCreateState(TenantId tenant) =>
        _states.GetOrAdd(tenant, _ => new TenantQuotaState());

    [LoggerMessage(Level = LogLevel.Debug, Message = "Produce throttled for tenant '{TenantId}': requested {Bytes} bytes, available {Available}")]
    private partial void LogProduceThrottled(string tenantId, long bytes, long available);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fetch throttled for tenant '{TenantId}': requested {Bytes} bytes, available {Available}")]
    private partial void LogFetchThrottled(string tenantId, long bytes, long available);
}
