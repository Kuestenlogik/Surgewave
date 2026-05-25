using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Orchestrates graceful broker shutdown for serverless scale-down.
/// Transitions through Draining -> ReadyToTerminate, flushing all write buffers
/// to remote object storage before allowing the instance to terminate.
/// Thread-safe state transitions via Interlocked.CompareExchange.
/// </summary>
public sealed class DrainCoordinator
{
    private readonly ILogger<DrainCoordinator> _logger;
    private readonly ServerlessConfig _config;
    private readonly Func<CancellationToken, Task> _flushCallback;

    private int _state = (int)ServerlessLifecycleState.Active;
    private int _drainStarted;

    /// <summary>
    /// Current lifecycle state of the broker.
    /// </summary>
    public ServerlessLifecycleState CurrentState =>
        (ServerlessLifecycleState)Volatile.Read(ref _state);

    /// <summary>
    /// Creates a new <see cref="DrainCoordinator"/>.
    /// </summary>
    /// <param name="logger">Logger instance.</param>
    /// <param name="config">Serverless configuration with drain timeout.</param>
    /// <param name="flushCallback">
    /// Callback that flushes all active partition write buffers to remote storage.
    /// Called during drain to persist in-flight data.
    /// </param>
    public DrainCoordinator(
        ILogger<DrainCoordinator> logger,
        ServerlessConfig config,
        Func<CancellationToken, Task> flushCallback)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _flushCallback = flushCallback ?? throw new ArgumentNullException(nameof(flushCallback));
    }

    /// <summary>
    /// Begin the drain process. Transitions state to <see cref="ServerlessLifecycleState.Draining"/>
    /// and signals that the broker should stop accepting new writes.
    /// Concurrent calls are idempotent; only the first call triggers the transition.
    /// </summary>
    public Task StartDrainAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _drainStarted, 1, 0) != 0)
        {
            _logger.LogWarning("Drain already in progress, ignoring duplicate StartDrainAsync call");
            return Task.CompletedTask;
        }

        var previous = Interlocked.CompareExchange(
            ref _state,
            (int)ServerlessLifecycleState.Draining,
            (int)ServerlessLifecycleState.Active);

        if (previous != (int)ServerlessLifecycleState.Active)
        {
            _logger.LogWarning(
                "Cannot start drain from state {CurrentState}, expected Active",
                (ServerlessLifecycleState)previous);
            return Task.CompletedTask;
        }

        _logger.LogInformation("Broker drain started, transitioning to Draining state");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Wait for all write buffers to flush to remote storage, then transition to
    /// <see cref="ServerlessLifecycleState.ReadyToTerminate"/>.
    /// If the drain exceeds <see cref="ServerlessConfig.DrainTimeout"/>, the broker
    /// is force-transitioned to ReadyToTerminate with a warning.
    /// </summary>
    public async Task WaitForDrainCompleteAsync(CancellationToken cancellationToken = default)
    {
        if (CurrentState != ServerlessLifecycleState.Draining)
        {
            _logger.LogWarning(
                "WaitForDrainCompleteAsync called but state is {State}, not Draining",
                CurrentState);
            return;
        }

        var stopwatch = Stopwatch.StartNew();

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_config.DrainTimeout);

        try
        {
            _logger.LogInformation("Flushing all write buffers to remote storage...");
            await _flushCallback(timeoutCts.Token);
            stopwatch.Stop();

            _logger.LogInformation(
                "Drain completed successfully in {ElapsedMs}ms",
                stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            stopwatch.Stop();
            _logger.LogWarning(
                "Drain timed out after {TimeoutMs}ms (limit: {DrainTimeout}ms), forcing ReadyToTerminate",
                stopwatch.ElapsedMilliseconds,
                _config.DrainTimeout.TotalMilliseconds);
        }

        Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.ReadyToTerminate);
        _logger.LogInformation("Broker transitioned to ReadyToTerminate");
    }

    /// <summary>
    /// Allows setting the initial state for testing or cold-start scenarios.
    /// </summary>
    internal void SetState(ServerlessLifecycleState newState)
    {
        Interlocked.Exchange(ref _state, (int)newState);
    }
}
