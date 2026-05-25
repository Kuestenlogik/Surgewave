namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Injects configurable latency into operations on a specific broker.
/// All storage and transport operations will be delayed by the specified amount.
/// Disposing or calling <see cref="Remove"/> removes the latency injection.
/// </summary>
public sealed class LatencyInjectionScenario : IDisposable
{
    private readonly ChaosEngine _engine;
    private string? _faultId;
    private bool _disposed;

    private LatencyInjectionScenario(ChaosEngine engine)
    {
        _engine = engine;
    }

    /// <summary>
    /// Creates a latency injection scenario on the specified broker.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="brokerId">The broker ID to add latency to.</param>
    /// <param name="latency">The amount of latency to inject per operation.</param>
    /// <param name="probability">Probability of triggering (0.0 to 1.0). Defaults to 1.0.</param>
    /// <returns>A scenario that can be removed or disposed.</returns>
    public static LatencyInjectionScenario Create(ChaosEngine engine, int brokerId, TimeSpan latency, double probability = 1.0)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var scenario = new LatencyInjectionScenario(engine);
        scenario._faultId = engine.ActivateFault(FaultType.SlowNetwork, new FaultScope
        {
            BrokerId = brokerId,
            Probability = probability
        }, latency);
        return scenario;
    }

    /// <summary>
    /// Creates a latency injection scenario targeting a specific peer from a broker.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="brokerId">The source broker ID.</param>
    /// <param name="targetPeerId">The target peer ID to slow down communication with.</param>
    /// <param name="latency">The amount of latency to inject per operation.</param>
    /// <returns>A scenario that can be removed or disposed.</returns>
    public static LatencyInjectionScenario CreateForPeer(ChaosEngine engine, int brokerId, int targetPeerId, TimeSpan latency)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var scenario = new LatencyInjectionScenario(engine);
        scenario._faultId = engine.ActivateFault(FaultType.SlowNetwork, new FaultScope
        {
            BrokerId = brokerId,
            TargetPeerId = targetPeerId
        }, latency);
        return scenario;
    }

    /// <summary>
    /// Removes the latency injection, restoring normal operation speeds.
    /// </summary>
    public void Remove()
    {
        if (_faultId != null)
        {
            _engine.DeactivateFault(_faultId);
            _faultId = null;
        }
    }

    /// <summary>
    /// Removes the latency and disposes the scenario.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Remove();
    }
}
