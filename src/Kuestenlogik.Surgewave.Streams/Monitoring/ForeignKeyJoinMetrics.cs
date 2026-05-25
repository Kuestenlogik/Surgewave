using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// OTEL metrics for foreign key join operations.
/// Tracks subscriptions, unsubscriptions, lookups, fan-out, and current subscription count.
/// </summary>
public sealed class ForeignKeyJoinMetrics
{
    private readonly Counter<long> _subscriptions;
    private readonly Counter<long> _unsubscriptions;
    private readonly Histogram<double> _lookupLatency;
    private readonly Counter<long> _fanOut;
    private readonly KeyValuePair<string, object?> _joinTag;

    private long _subscriptionCount;
    private long _totalSubscriptions;
    private long _totalUnsubscriptions;
    private long _totalLookups;
    private long _totalFanOut;

    /// <summary>Gets the name of the join node these metrics belong to.</summary>
    public string JoinName { get; }

    /// <summary>Gets the total number of subscribe operations recorded.</summary>
    public long TotalSubscriptions => Interlocked.Read(ref _totalSubscriptions);

    /// <summary>Gets the total number of unsubscribe operations recorded.</summary>
    public long TotalUnsubscriptions => Interlocked.Read(ref _totalUnsubscriptions);

    /// <summary>Gets the total number of FK lookup operations recorded.</summary>
    public long TotalLookups => Interlocked.Read(ref _totalLookups);

    /// <summary>Gets the total fan-out recorded (PKs matched per FK update, summed).</summary>
    public long TotalFanOut => Interlocked.Read(ref _totalFanOut);

    /// <summary>Gets the current active subscription count (subscriptions minus unsubscriptions).</summary>
    public long CurrentSubscriptionCount => Interlocked.Read(ref _subscriptionCount);

    /// <summary>
    /// Initializes FK join metrics on the given meter.
    /// </summary>
    public ForeignKeyJoinMetrics(Meter meter, string joinName)
    {
        JoinName = joinName ?? throw new ArgumentNullException(nameof(joinName));
        _joinTag = new KeyValuePair<string, object?>("join.name", joinName);

        _subscriptions = meter.CreateCounter<long>(
            "surgewave_streams_fk_join_subscriptions_total",
            description: "Total FK join subscription operations");

        _unsubscriptions = meter.CreateCounter<long>(
            "surgewave_streams_fk_join_unsubscriptions_total",
            description: "Total FK join unsubscription operations");

        _lookupLatency = meter.CreateHistogram<double>(
            "surgewave_streams_fk_join_lookup_latency_ms",
            unit: "ms",
            description: "Latency of FK join subscriber lookups");

        _fanOut = meter.CreateCounter<long>(
            "surgewave_streams_fk_join_fanout_total",
            description: "Total number of PKs matched per FK update (fan-out)");

        meter.CreateObservableGauge(
            "surgewave_streams_fk_join_subscription_count",
            () => Interlocked.Read(ref _subscriptionCount),
            description: "Current number of active FK join subscriptions");
    }

    /// <summary>Records a subscribe operation.</summary>
    public void RecordSubscription()
    {
        Interlocked.Increment(ref _totalSubscriptions);
        Interlocked.Increment(ref _subscriptionCount);
        _subscriptions.Add(1, _joinTag);
    }

    /// <summary>Records an unsubscribe operation.</summary>
    public void RecordUnsubscription()
    {
        Interlocked.Increment(ref _totalUnsubscriptions);
        Interlocked.Decrement(ref _subscriptionCount);
        _unsubscriptions.Add(1, _joinTag);
    }

    /// <summary>Records a FK subscriber lookup with its latency in milliseconds.</summary>
    public void RecordLookup(double latencyMs)
    {
        Interlocked.Increment(ref _totalLookups);
        _lookupLatency.Record(latencyMs, _joinTag);
    }

    /// <summary>
    /// Records the fan-out for a single FK update — i.e., how many PKs were matched.
    /// </summary>
    public void RecordFanOut(long matchedPks)
    {
        if (matchedPks <= 0) return;
        Interlocked.Add(ref _totalFanOut, matchedPks);
        _fanOut.Add(matchedPks, _joinTag);
    }

    /// <summary>
    /// Explicitly sets the current subscription count (e.g. after a restore).
    /// </summary>
    public void SetSubscriptionCount(long count)
    {
        Interlocked.Exchange(ref _subscriptionCount, count);
    }
}
