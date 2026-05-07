namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Represents a currently active fault in the chaos engine.
/// </summary>
/// <param name="Id">Unique identifier for this fault activation.</param>
/// <param name="FaultType">The type of fault.</param>
/// <param name="Scope">The scope of the fault.</param>
/// <param name="ActivatedAt">When the fault was activated.</param>
/// <param name="InjectedLatency">Optional latency to inject for SlowNetwork faults.</param>
/// <param name="Description">Human-readable description of the fault.</param>
public sealed record ActiveFault(
    string Id,
    FaultType FaultType,
    FaultScope Scope,
    DateTimeOffset ActivatedAt,
    TimeSpan? InjectedLatency,
    string Description
);
