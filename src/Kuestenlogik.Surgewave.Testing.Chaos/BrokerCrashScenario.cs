namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Simulates a complete broker crash by injecting NodeCrash faults
/// that cause all operations on the specified broker to fail.
/// Disposing or calling <see cref="Recover"/> removes the crash fault.
/// </summary>
public sealed class BrokerCrashScenario : IDisposable
{
    private readonly ChaosEngine _engine;
    private string? _faultId;
    private bool _disposed;

    private BrokerCrashScenario(ChaosEngine engine)
    {
        _engine = engine;
    }

    /// <summary>
    /// Creates a broker crash scenario that makes all operations on the specified broker fail.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="brokerId">The broker ID to crash.</param>
    /// <returns>A scenario that can be recovered or disposed.</returns>
    public static BrokerCrashScenario Create(ChaosEngine engine, int brokerId)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var scenario = new BrokerCrashScenario(engine);
        scenario._faultId = engine.ActivateFault(FaultType.NodeCrash, new FaultScope
        {
            BrokerId = brokerId
        });
        return scenario;
    }

    /// <summary>
    /// Recovers the broker from the simulated crash.
    /// </summary>
    public void Recover()
    {
        if (_faultId != null)
        {
            _engine.DeactivateFault(_faultId);
            _faultId = null;
        }
    }

    /// <summary>
    /// Recovers the broker and disposes the scenario.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Recover();
    }
}
