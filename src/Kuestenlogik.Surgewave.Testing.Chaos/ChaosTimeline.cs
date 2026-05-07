using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Thread-safe timeline of chaos events for diagnostics and test assertions.
/// </summary>
public sealed class ChaosTimeline
{
    private readonly ConcurrentQueue<ChaosEvent> _events = new();

    /// <summary>
    /// Records a new event in the timeline.
    /// </summary>
    public void Record(ChaosEvent chaosEvent)
    {
        ArgumentNullException.ThrowIfNull(chaosEvent);
        _events.Enqueue(chaosEvent);
    }

    /// <summary>
    /// Records a new event in the timeline with the specified parameters.
    /// </summary>
    public void Record(ChaosEventType eventType, FaultType faultType, FaultScope? scope, string description)
    {
        _events.Enqueue(new ChaosEvent(eventType, faultType, scope, DateTimeOffset.UtcNow, description));
    }

    /// <summary>
    /// Returns all events recorded so far, in order.
    /// </summary>
    public IReadOnlyList<ChaosEvent> GetEvents() => [.. _events];

    /// <summary>
    /// Returns events filtered by fault type.
    /// </summary>
    public IReadOnlyList<ChaosEvent> GetEvents(FaultType faultType)
        => _events.Where(e => e.FaultType == faultType).ToList();

    /// <summary>
    /// Returns events filtered by event type.
    /// </summary>
    public IReadOnlyList<ChaosEvent> GetEvents(ChaosEventType eventType)
        => _events.Where(e => e.EventType == eventType).ToList();

    /// <summary>
    /// Returns the total number of recorded events.
    /// </summary>
    public int Count => _events.Count;

    /// <summary>
    /// Dumps the timeline to the provided output action.
    /// </summary>
    public void DumpToOutput(Action<string> output)
    {
        ArgumentNullException.ThrowIfNull(output);

        output($"=== Chaos Timeline ({_events.Count} events) ===");
        foreach (var e in _events)
        {
            output($"[{e.Timestamp:HH:mm:ss.fff}] {e.EventType} | {e.FaultType} | {e.Description}");
        }
        output("=== End Timeline ===");
    }
}
