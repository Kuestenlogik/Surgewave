using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Central orchestrator for chaos testing. Manages fault activation, deactivation,
/// and querying. Thread-safe for concurrent access from multiple components.
/// </summary>
public sealed class ChaosEngine
{
    private readonly ConcurrentDictionary<string, ActiveFault> _activeFaults = new();
    private readonly ChaosTimeline _timeline = new();

    [ThreadStatic]
    private static Random? t_random;

    private static Random Random => t_random ??= new Random();

    /// <summary>
    /// The timeline of all chaos events recorded by this engine.
    /// </summary>
    public ChaosTimeline Timeline => _timeline;

    /// <summary>
    /// A snapshot of all currently active faults.
    /// </summary>
    public IReadOnlyCollection<ActiveFault> ActiveFaults => _activeFaults.Values.ToList();

    /// <summary>
    /// Activates a fault with the specified type, scope, and optional latency.
    /// </summary>
    /// <param name="type">The type of fault to activate.</param>
    /// <param name="scope">The target scope for the fault. If null, applies globally.</param>
    /// <param name="latency">Optional latency to inject for SlowNetwork faults.</param>
    /// <returns>A unique fault ID that can be used to deactivate this specific fault.</returns>
    public string ActivateFault(FaultType type, FaultScope? scope = null, TimeSpan? latency = null)
    {
        scope ??= new FaultScope();
        var id = Guid.NewGuid().ToString("N");
        var description = $"{type} fault on broker={scope.BrokerId?.ToString() ?? "*"}, " +
                          $"peer={scope.TargetPeerId?.ToString() ?? "*"}, " +
                          $"topic={scope.Topic ?? "*"}, " +
                          $"probability={scope.Probability:P0}" +
                          (latency.HasValue ? $", latency={latency.Value.TotalMilliseconds}ms" : "");

        var fault = new ActiveFault(id, type, scope, DateTimeOffset.UtcNow, latency, description);
        _activeFaults[id] = fault;
        _timeline.Record(ChaosEventType.Activated, type, scope, description);
        return id;
    }

    /// <summary>
    /// Deactivates a specific fault by its ID.
    /// </summary>
    /// <param name="faultId">The fault ID returned by <see cref="ActivateFault"/>.</param>
    public void DeactivateFault(string faultId)
    {
        ArgumentNullException.ThrowIfNull(faultId);

        if (_activeFaults.TryRemove(faultId, out var fault))
        {
            _timeline.Record(ChaosEventType.Deactivated, fault.FaultType, fault.Scope, $"Deactivated: {fault.Description}");
        }
    }

    /// <summary>
    /// Deactivates all currently active faults.
    /// </summary>
    public void DeactivateAll()
    {
        foreach (var kvp in _activeFaults)
        {
            if (_activeFaults.TryRemove(kvp.Key, out var fault))
            {
                _timeline.Record(ChaosEventType.Deactivated, fault.FaultType, fault.Scope, $"Deactivated (all): {fault.Description}");
            }
        }
    }

    /// <summary>
    /// Checks whether a fault of the given type is active and matches the specified broker and peer.
    /// Also evaluates probability and records triggered events.
    /// </summary>
    /// <param name="type">The fault type to check.</param>
    /// <param name="brokerId">The broker ID to match against. Use -1 for any.</param>
    /// <param name="peerId">The peer ID to match against. Use -1 for any.</param>
    /// <returns>True if the fault should be triggered.</returns>
    public bool IsFaultActive(FaultType type, int brokerId = -1, int peerId = -1)
    {
        foreach (var kvp in _activeFaults)
        {
            var fault = kvp.Value;
            if (fault.FaultType != type)
                continue;

            if (brokerId >= 0 && !fault.Scope.Matches(brokerId))
                continue;

            if (peerId >= 0 && !fault.Scope.MatchesPeer(peerId))
                continue;

            // Evaluate probability
            if (fault.Scope.Probability < 1.0 && Random.NextDouble() > fault.Scope.Probability)
                continue;

            _timeline.Record(ChaosEventType.Triggered, type, fault.Scope,
                $"Triggered: {type} on broker={brokerId}, peer={peerId}");
            return true;
        }

        return false;
    }

    /// <summary>
    /// Gets the injected latency for a fault of the given type matching the specified broker.
    /// </summary>
    /// <param name="type">The fault type to check.</param>
    /// <param name="brokerId">The broker ID to match against. Use -1 for any.</param>
    /// <returns>The injected latency, or null if no matching fault with latency is active.</returns>
    public TimeSpan? GetInjectedLatency(FaultType type, int brokerId = -1)
    {
        foreach (var kvp in _activeFaults)
        {
            var fault = kvp.Value;
            if (fault.FaultType != type)
                continue;

            if (brokerId >= 0 && !fault.Scope.Matches(brokerId))
                continue;

            if (fault.InjectedLatency.HasValue)
                return fault.InjectedLatency;
        }

        return null;
    }
}
