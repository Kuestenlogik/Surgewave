using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Broker.Tenancy;

/// <summary>
/// OTEL metrics for multi-tenancy. Follows the BrokerMetrics pattern using System.Diagnostics.Metrics.
/// </summary>
public sealed class TenantMetrics : IDisposable
{
    public const string MeterName = "Kuestenlogik.Surgewave.Tenancy";

    private readonly Meter _meter;

    // Counters
    private readonly Counter<long> _topicsCreatedTotal;
    private readonly Counter<long> _topicsDeletedTotal;
    private readonly Counter<long> _quotaThrottledTotal;
    private readonly Counter<long> _quotaRejectedTotal;

    // Observable gauges
    private readonly ObservableGauge<int> _tenantCount;
    private readonly ObservableGauge<int> _topicsPerTenant;
    private readonly ObservableGauge<int> _connectionsPerTenant;
    private readonly ObservableGauge<long> _storageBytes;

    // Histograms
    private readonly Histogram<long> _produceBytes;
    private readonly Histogram<long> _fetchBytes;

    // State accessors for observable gauges
    private Func<int>? _getTenantCount;
    private Func<IEnumerable<Measurement<int>>>? _getTopicsPerTenant;
    private Func<IEnumerable<Measurement<int>>>? _getConnectionsPerTenant;
    private Func<IEnumerable<Measurement<long>>>? _getStorageBytes;

    public TenantMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");

        _topicsCreatedTotal = _meter.CreateCounter<long>(
            "surgewave_tenancy_topics_created_total",
            description: "Total number of topics created per tenant");

        _topicsDeletedTotal = _meter.CreateCounter<long>(
            "surgewave_tenancy_topics_deleted_total",
            description: "Total number of topics deleted per tenant");

        _quotaThrottledTotal = _meter.CreateCounter<long>(
            "surgewave_tenancy_quota_throttled_total",
            description: "Total number of requests throttled due to tenant quota");

        _quotaRejectedTotal = _meter.CreateCounter<long>(
            "surgewave_tenancy_quota_rejected_total",
            description: "Total number of requests rejected due to tenant quota");

        _tenantCount = _meter.CreateObservableGauge(
            "surgewave_tenancy_tenant_count",
            () => _getTenantCount?.Invoke() ?? 0,
            description: "Total number of tenants");

        _topicsPerTenant = _meter.CreateObservableGauge(
            "surgewave_tenancy_topics_per_tenant",
            () => _getTopicsPerTenant?.Invoke() ?? [],
            description: "Number of topics per tenant");

        _connectionsPerTenant = _meter.CreateObservableGauge(
            "surgewave_tenancy_connections_per_tenant",
            () => _getConnectionsPerTenant?.Invoke() ?? [],
            description: "Number of active connections per tenant");

        _storageBytes = _meter.CreateObservableGauge(
            "surgewave_tenancy_storage_bytes",
            () => _getStorageBytes?.Invoke() ?? [],
            unit: "By",
            description: "Storage bytes used per tenant");

        _produceBytes = _meter.CreateHistogram<long>(
            "surgewave_tenancy_produce_bytes",
            unit: "By",
            description: "Bytes produced per tenant");

        _fetchBytes = _meter.CreateHistogram<long>(
            "surgewave_tenancy_fetch_bytes",
            unit: "By",
            description: "Bytes fetched per tenant");
    }

    /// <summary>
    /// Register callbacks for observable gauges (pull-based metrics).
    /// </summary>
    public void RegisterStateAccessors(
        Func<int> getTenantCount,
        Func<IEnumerable<Measurement<int>>>? getTopicsPerTenant = null,
        Func<IEnumerable<Measurement<int>>>? getConnectionsPerTenant = null,
        Func<IEnumerable<Measurement<long>>>? getStorageBytes = null)
    {
        _getTenantCount = getTenantCount;
        _getTopicsPerTenant = getTopicsPerTenant;
        _getConnectionsPerTenant = getConnectionsPerTenant;
        _getStorageBytes = getStorageBytes;
    }

    public void RecordTopicCreated(string tenant)
    {
        _topicsCreatedTotal.Add(1, new TagList { { "tenant", tenant } });
    }

    public void RecordTopicDeleted(string tenant)
    {
        _topicsDeletedTotal.Add(1, new TagList { { "tenant", tenant } });
    }

    public void RecordQuotaThrottled(string tenant)
    {
        _quotaThrottledTotal.Add(1, new TagList { { "tenant", tenant } });
    }

    public void RecordQuotaRejected(string tenant)
    {
        _quotaRejectedTotal.Add(1, new TagList { { "tenant", tenant } });
    }

    public void RecordProduceBytes(string tenant, long bytes)
    {
        _produceBytes.Record(bytes, new TagList { { "tenant", tenant } });
    }

    public void RecordFetchBytes(string tenant, long bytes)
    {
        _fetchBytes.Record(bytes, new TagList { { "tenant", tenant } });
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}
