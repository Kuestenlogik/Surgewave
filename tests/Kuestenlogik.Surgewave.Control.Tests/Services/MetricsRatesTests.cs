using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Control.Services.Alerting;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Tests for deriving per-second rates from two cumulative broker snapshots
/// (#38). The broker exposes only monotonic _total counters, so throughput and
/// error rate must be diffed over elapsed time.
/// </summary>
public sealed class MetricsRatesTests
{
    private static readonly DateTime Base = new(2026, 7, 6, 12, 0, 0, DateTimeKind.Utc);

    private static MetricsSnapshot Snapshot(DateTime at, double produced = 0, long errors = 0, long produceErrors = 0)
        => new()
        {
            Timestamp = at,
            MessagesProducedTotal = produced,
            ErrorsTotal = errors,
            ProduceErrorsTotal = produceErrors,
        };

    [Fact]
    public void Between_NoPreviousSnapshot_IsUnavailable()
    {
        var rates = MetricsRates.Between(null, Snapshot(Base, produced: 100), brokerReachable: true);

        Assert.False(rates.Available);
    }

    [Fact]
    public void Between_UnreachableBroker_IsUnavailable()
    {
        var previous = Snapshot(Base, produced: 100);
        var current = Snapshot(Base.AddSeconds(30), produced: 200);

        var rates = MetricsRates.Between(previous, current, brokerReachable: false);

        Assert.False(rates.Available);
    }

    [Fact]
    public void Between_ComputesMessagesAndErrorsPerSecond()
    {
        var previous = Snapshot(Base, produced: 1000, errors: 5, produceErrors: 5);
        var current = Snapshot(Base.AddSeconds(10), produced: 4000, errors: 15, produceErrors: 25);

        var rates = MetricsRates.Between(previous, current, brokerReachable: true);

        Assert.True(rates.Available);
        Assert.Equal(300, rates.MessagesProducedPerSecond); // (4000-1000)/10
        Assert.Equal(3, rates.ErrorsPerSecond);             // ((15+25)-(5+5))/10
    }

    [Fact]
    public void Between_IdleBroker_YieldsZeroRate()
    {
        var previous = Snapshot(Base, produced: 1_000_000);
        var current = Snapshot(Base.AddSeconds(30), produced: 1_000_000);

        var rates = MetricsRates.Between(previous, current, brokerReachable: true);

        Assert.True(rates.Available);
        Assert.Equal(0, rates.MessagesProducedPerSecond);
    }

    [Fact]
    public void Between_CounterReset_ClampsToZeroInsteadOfNegative()
    {
        // A broker restart resets the monotonic totals; the diff must not produce
        // a spurious negative rate.
        var previous = Snapshot(Base, produced: 5000, errors: 100, produceErrors: 100);
        var current = Snapshot(Base.AddSeconds(10), produced: 50, errors: 0, produceErrors: 0);

        var rates = MetricsRates.Between(previous, current, brokerReachable: true);

        Assert.True(rates.Available);
        Assert.Equal(0, rates.MessagesProducedPerSecond);
        Assert.Equal(0, rates.ErrorsPerSecond);
    }

    [Fact]
    public void Between_NonPositiveElapsed_IsUnavailable()
    {
        var previous = Snapshot(Base, produced: 100);
        var sameInstant = Snapshot(Base, produced: 200);

        var rates = MetricsRates.Between(previous, sameInstant, brokerReachable: true);

        Assert.False(rates.Available);
    }
}
