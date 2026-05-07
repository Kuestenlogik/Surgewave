namespace Kuestenlogik.Surgewave.Core.Observability;

/// <summary>
/// Configuration for the broker-side observability hook. Bound from
/// <c>Surgewave:Observability</c> via <see cref="SurgewaveObservabilityExtensions.AddSurgewaveBrokerObservability"/>.
/// </summary>
public sealed class SurgewaveObservabilityOptions
{
    public const string ConfigSection = "Surgewave:Observability";

    /// <summary>
    /// Whether the broker hooks up <see cref="SurgewaveBrokerObservability"/> at all. When
    /// <c>false</c>, no <see cref="ISurgewaveBrokerObservability"/> is registered in DI —
    /// pipeline code sees <c>_observability == null</c> and skips event construction
    /// entirely. Defaults to <c>true</c>; operators who are certain nothing will ever
    /// subscribe (embedded test runs, static benchmarks) can flip this off to shave the
    /// null-conditional check from each produce/consume call.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Per-subscriber bounded-channel capacity. Overrides
    /// <see cref="SurgewaveBrokerObservability.DefaultSubscriberCapacity"/> when set.
    /// Higher values tolerate longer GC pauses in observers at the cost of more memory
    /// per subscriber; lower values drop events faster.
    /// </summary>
    public int SubscriberCapacity { get; set; } = SurgewaveBrokerObservability.DefaultSubscriberCapacity;
}
