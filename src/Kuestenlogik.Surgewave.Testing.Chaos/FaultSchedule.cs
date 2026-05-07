namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Schedules a fault to be activated after a delay and optionally deactivated after a duration.
/// Disposing the schedule cancels any pending activation or deactivation.
/// </summary>
public sealed class FaultSchedule : IDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private string? _faultId;
    private bool _disposed;

    private FaultSchedule() { }

    /// <summary>
    /// Creates a scheduled fault that activates after the specified delay
    /// and optionally deactivates after the specified duration.
    /// </summary>
    /// <param name="engine">The chaos engine to activate the fault on.</param>
    /// <param name="type">The type of fault to activate.</param>
    /// <param name="scope">The target scope for the fault.</param>
    /// <param name="activateAfter">Delay before activating the fault.</param>
    /// <param name="duration">Optional duration after which the fault is automatically deactivated.</param>
    /// <returns>A FaultSchedule that can be disposed to cancel the schedule.</returns>
    public static FaultSchedule Create(ChaosEngine engine, FaultType type, FaultScope? scope,
        TimeSpan activateAfter, TimeSpan? duration = null)
    {
        ArgumentNullException.ThrowIfNull(engine);

        var schedule = new FaultSchedule();
        schedule.StartAsync(engine, type, scope, activateAfter, duration);
        return schedule;
    }

    private async void StartAsync(ChaosEngine engine, FaultType type, FaultScope? scope,
        TimeSpan activateAfter, TimeSpan? duration)
    {
        try
        {
            await Task.Delay(activateAfter, _cts.Token);

            _faultId = engine.ActivateFault(type, scope);

            if (duration.HasValue)
            {
                await Task.Delay(duration.Value, _cts.Token);
                engine.DeactivateFault(_faultId);
                _faultId = null;
            }
        }
        catch (OperationCanceledException)
        {
            // Schedule was cancelled via Dispose
        }
    }

    /// <summary>
    /// Cancels the schedule and deactivates the fault if it was already activated.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _cts.Cancel();
        _cts.Dispose();
    }
}
