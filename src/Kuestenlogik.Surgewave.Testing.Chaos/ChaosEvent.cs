namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// The type of a chaos event.
/// </summary>
public enum ChaosEventType
{
    /// <summary>A fault was activated.</summary>
    Activated,

    /// <summary>A fault was deactivated.</summary>
    Deactivated,

    /// <summary>A fault was triggered during an operation.</summary>
    Triggered
}

/// <summary>
/// Records an event in the chaos timeline for diagnostics and assertions.
/// </summary>
/// <param name="EventType">Whether the fault was activated, deactivated, or triggered.</param>
/// <param name="FaultType">The type of fault involved.</param>
/// <param name="Scope">The scope of the fault, if any.</param>
/// <param name="Timestamp">When the event occurred.</param>
/// <param name="Description">Human-readable description of the event.</param>
public sealed record ChaosEvent(
    ChaosEventType EventType,
    FaultType FaultType,
    FaultScope? Scope,
    DateTimeOffset Timestamp,
    string Description
);
