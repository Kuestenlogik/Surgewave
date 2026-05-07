namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Simulates disk I/O failures on a specific broker by injecting DiskIoError faults.
/// All storage read/write operations on the target broker will throw IOException.
/// Disposing or calling <see cref="Repair"/> removes the disk failure.
/// </summary>
public sealed class DiskFailureScenario : IDisposable
{
    private readonly ChaosEngine _engine;
    private string? _faultId;
    private bool _disposed;

    private DiskFailureScenario(ChaosEngine engine)
    {
        _engine = engine;
    }

    /// <summary>
    /// Creates a disk failure scenario on the specified broker.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="brokerId">The broker ID whose disk should fail.</param>
    /// <returns>A scenario that can be repaired or disposed.</returns>
    public static DiskFailureScenario Create(ChaosEngine engine, int brokerId)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var scenario = new DiskFailureScenario(engine);
        scenario._faultId = engine.ActivateFault(FaultType.DiskIoError, new FaultScope
        {
            BrokerId = brokerId
        });
        return scenario;
    }

    /// <summary>
    /// Creates a disk-full scenario on the specified broker.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="brokerId">The broker ID whose disk should appear full.</param>
    /// <returns>A scenario that can be repaired or disposed.</returns>
    public static DiskFailureScenario CreateStorageFull(ChaosEngine engine, int brokerId)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var scenario = new DiskFailureScenario(engine);
        scenario._faultId = engine.ActivateFault(FaultType.StorageFullError, new FaultScope
        {
            BrokerId = brokerId
        });
        return scenario;
    }

    /// <summary>
    /// Repairs the disk failure, restoring normal storage operations.
    /// </summary>
    public void Repair()
    {
        if (_faultId != null)
        {
            _engine.DeactivateFault(_faultId);
            _faultId = null;
        }
    }

    /// <summary>
    /// Repairs the failure and disposes the scenario.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Repair();
    }
}
