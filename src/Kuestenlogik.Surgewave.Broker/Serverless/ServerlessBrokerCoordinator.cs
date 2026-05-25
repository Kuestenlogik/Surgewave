using System.Diagnostics;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Top-level coordinator that ties together all serverless scaling components.
/// Manages the broker lifecycle from cold start through active operation to graceful drain.
/// </summary>
public sealed class ServerlessBrokerCoordinator : IDisposable
{
    private readonly ILogger<ServerlessBrokerCoordinator> _logger;
    private readonly ServerlessConfig _config;
    private readonly DrainCoordinator _drainCoordinator;
    private readonly ColdStartOptimizer _coldStartOptimizer;
    private readonly ScaleDecisionEngine _scaleDecisionEngine;
    private readonly ServerlessMetrics _metrics;

    private int _state = (int)ServerlessLifecycleState.ColdStarting;
    private bool _disposed;

    /// <summary>
    /// Current lifecycle state of the broker.
    /// </summary>
    public ServerlessLifecycleState State =>
        (ServerlessLifecycleState)Volatile.Read(ref _state);

    public ServerlessBrokerCoordinator(
        ILogger<ServerlessBrokerCoordinator> logger,
        ServerlessConfig config,
        DrainCoordinator drainCoordinator,
        ColdStartOptimizer coldStartOptimizer,
        ScaleDecisionEngine scaleDecisionEngine,
        ServerlessMetrics metrics)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _drainCoordinator = drainCoordinator ?? throw new ArgumentNullException(nameof(drainCoordinator));
        _coldStartOptimizer = coldStartOptimizer ?? throw new ArgumentNullException(nameof(coldStartOptimizer));
        _scaleDecisionEngine = scaleDecisionEngine ?? throw new ArgumentNullException(nameof(scaleDecisionEngine));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    /// <summary>
    /// Initialize the broker from cold start. Transitions through ColdStarting -> Warming -> Active.
    /// </summary>
    /// <param name="clusterState">Cluster state for cold start optimization.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InitializeAsync(ClusterState clusterState, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(clusterState);

        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordColdStart();

        _logger.LogInformation("Serverless broker initialization starting (ColdStarting)");

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_config.ColdStartTimeout);

        try
        {
            // Transition to Warming
            Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.Warming);
            _logger.LogInformation("Broker transitioning to Warming state");

            // Run cold start optimization
            var report = await _coldStartOptimizer.OptimizeColdStartAsync(
                clusterState, timeoutCts.Token);

            // Transition to Active
            Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.Active);
            _drainCoordinator.SetState(ServerlessLifecycleState.Active);

            stopwatch.Stop();
            _metrics.RecordColdStartDuration(stopwatch.Elapsed.TotalMilliseconds);

            _logger.LogInformation(
                "Broker is now Active. Cold start took {ElapsedMs}ms " +
                "(partitions loaded: {Loaded}, pre-warmed: {Warmed})",
                stopwatch.ElapsedMilliseconds,
                report.PartitionsLoaded,
                report.PartitionsPreWarmed);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            stopwatch.Stop();
            _logger.LogWarning(
                "Cold start timed out after {ElapsedMs}ms (limit: {Timeout}ms), transitioning to Active anyway",
                stopwatch.ElapsedMilliseconds,
                _config.ColdStartTimeout.TotalMilliseconds);

            Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.Active);
            _drainCoordinator.SetState(ServerlessLifecycleState.Active);
            _metrics.RecordColdStartDuration(stopwatch.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>
    /// Evaluate current metrics and produce a scaling decision.
    /// Called periodically by the scaling loop.
    /// </summary>
    public Task<ScaleDecision> EvaluateScaleAsync(ScaleMetrics metrics, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var decision = _scaleDecisionEngine.Evaluate(metrics);

        switch (decision.Action)
        {
            case ScaleAction.ScaleUp:
                _metrics.RecordScaleUp();
                break;
            case ScaleAction.ScaleDown:
                _metrics.RecordScaleDown();
                break;
        }

        return Task.FromResult(decision);
    }

    /// <summary>
    /// Initiate the drain process for graceful shutdown.
    /// Transitions to Draining, flushes buffers, then ReadyToTerminate.
    /// </summary>
    public async Task InitiateDrainAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State != ServerlessLifecycleState.Active)
        {
            _logger.LogWarning("Cannot drain from state {State}, expected Active", State);
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordDrain();

        Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.Draining);
        _logger.LogInformation("Broker transitioning to Draining state");

        await _drainCoordinator.StartDrainAsync(cancellationToken);
        await _drainCoordinator.WaitForDrainCompleteAsync(cancellationToken);

        Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.ReadyToTerminate);

        stopwatch.Stop();
        _metrics.RecordDrainDuration(stopwatch.Elapsed.TotalMilliseconds);

        _logger.LogInformation(
            "Broker drain complete in {ElapsedMs}ms, now ReadyToTerminate",
            stopwatch.ElapsedMilliseconds);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        Interlocked.Exchange(ref _state, (int)ServerlessLifecycleState.Terminated);
        _metrics.Dispose();
    }
}
