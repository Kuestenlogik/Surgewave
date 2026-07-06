namespace Kuestenlogik.Surgewave.Control.Services.Alerting;

/// <summary>
/// Per-second rates derived by diffing two consecutive broker metrics
/// snapshots. The broker only exposes cumulative <c>_total</c> counters, so
/// rate-based rules (throughput, error rate) must diff over elapsed time rather
/// than compare an absolute lifetime total. <see cref="Available"/> is false on
/// the first evaluation cycle (no prior snapshot to diff) and whenever the
/// broker is unreachable, in which case rate rules must not fire.
/// </summary>
public readonly record struct MetricsRates(
    double MessagesProducedPerSecond,
    double ErrorsPerSecond,
    bool Available)
{
    public static readonly MetricsRates Unavailable = new(0, 0, Available: false);

    /// <summary>
    /// Compute rates between <paramref name="previous"/> and <paramref name="current"/>.
    /// Returns <see cref="Unavailable"/> when there is no usable prior snapshot,
    /// no positive elapsed time, or the broker is unreachable. Counter decreases
    /// (a broker restart resets the monotonic totals to zero) are clamped to a
    /// zero delta rather than producing a spurious negative rate.
    /// </summary>
    public static MetricsRates Between(MetricsSnapshot? previous, MetricsSnapshot current, bool brokerReachable)
    {
        if (!brokerReachable || previous is null)
            return Unavailable;

        var elapsedSeconds = (current.Timestamp - previous.Timestamp).TotalSeconds;
        if (elapsedSeconds <= 0)
            return Unavailable;

        var messages = Math.Max(0, current.MessagesProducedTotal - previous.MessagesProducedTotal) / elapsedSeconds;

        var currentErrors = current.ErrorsTotal + current.ProduceErrorsTotal;
        var previousErrors = previous.ErrorsTotal + previous.ProduceErrorsTotal;
        var errors = Math.Max(0, currentErrors - previousErrors) / elapsedSeconds;

        return new MetricsRates(messages, errors, Available: true);
    }
}
