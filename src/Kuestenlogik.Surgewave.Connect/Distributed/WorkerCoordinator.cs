using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Distributed;

/// <summary>
/// Coordinates distributed Connect workers using Surgewave/Kafka for group membership
/// and task assignment. Supports both Surgewave native and Kafka protocols via ISurgewaveClient.
/// </summary>
public sealed class WorkerCoordinator : IAsyncDisposable
{
    private readonly ConnectWorkerConfig _config;
    private readonly string _workerId;
    private readonly string _restUrl;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, WorkerInfo> _workers = new();
    private readonly ConcurrentDictionary<string, ConnectorTaskAssignment> _assignments = new();
    private readonly SemaphoreSlim _rebalanceLock = new(1, 1);
    private readonly PluginDiscovery? _pluginDiscovery;
    private readonly AggregatedConnectorRegistry? _aggregatedRegistry;

    private ISurgewaveClient? _surgewaveClient;
    private IProducer<string, string>? _producer;
    private IConsumer<string, string>? _consumer;
    private CancellationTokenSource? _heartbeatCts;
    private Task? _heartbeatTask;
    private Task? _assignmentTask;
    private bool _isLeader;
    private int _generation;
    private bool _disposed;

    public string WorkerId => _workerId;
    public bool IsLeader => _isLeader;
    public string? LeaderId { get; private set; }
    public int Generation => _generation;
    public IReadOnlyCollection<WorkerInfo> Workers => _workers.Values.ToList();

    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings", Justification = "REST URL stored as string for simplicity")]
    public string RestUrl => _restUrl;

    public event EventHandler<TasksAssignedEventArgs>? TasksAssigned;
    public event EventHandler<TasksRevokedEventArgs>? TasksRevoked;

    /// <summary>
    /// Raised when a worker is considered disconnected (missed heartbeats past session timeout).
    /// </summary>
    public event EventHandler<WorkerDisconnectedEventArgs>? WorkerDisconnected;

    [SuppressMessage("Design", "CA1054:URI-like parameters should not be strings", Justification = "REST URL stored as string for simplicity")]
    public WorkerCoordinator(
        ConnectWorkerConfig config,
        string restUrl,
        ILogger logger,
        PluginDiscovery? pluginDiscovery = null,
        AggregatedConnectorRegistry? aggregatedRegistry = null)
    {
        _config = config;
        _restUrl = restUrl;
        _workerId = $"worker-{Environment.MachineName}-{Guid.NewGuid():N}".ToLowerInvariant();
        _logger = logger;
        _pluginDiscovery = pluginDiscovery;
        _aggregatedRegistry = aggregatedRegistry;
    }

    /// <summary>
    /// Sets the Surgewave client for communication.
    /// </summary>
    public void SetSurgewaveClient(ISurgewaveClient client)
    {
        _surgewaveClient = client;
    }

    /// <summary>
    /// Start the coordinator and join the worker group.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_surgewaveClient == null)
            throw new InvalidOperationException("Surgewave client not set. Call SetSurgewaveClient first.");

        _logger.LogInformation("Starting worker coordinator as {WorkerId} using {Protocol} protocol",
            _workerId, _surgewaveClient.Protocol);

        // Create producer for heartbeats and assignments
        _producer = _surgewaveClient.CreateProducer<string, string>();

        // Create consumer for assignment topic
        _consumer = _surgewaveClient.CreateConsumer<string, string>(opts =>
        {
            opts.GroupId = $"{_config.GroupId}-coordinator";
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
        });

        // Subscribe to status topic for heartbeats
        _consumer.Subscribe(_config.StatusTopic);

        // Initialize aggregated registry with local plugins
        if (_pluginDiscovery != null && _aggregatedRegistry != null)
        {
            _aggregatedRegistry.UpdateFromLocalPlugins(_pluginDiscovery.GetAllPlugins());
        }

        // Start heartbeat task
        _heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _heartbeatTask = HeartbeatLoopAsync(_heartbeatCts.Token);

        // Start assignment listener
        _assignmentTask = AssignmentLoopAsync(_heartbeatCts.Token);

        // Send initial join
        await SendHeartbeatAsync();

        _logger.LogInformation("Worker coordinator started with REST URL: {RestUrl}", _restUrl);
    }

    /// <summary>
    /// Send heartbeat to register this worker.
    /// </summary>
    public async Task SendHeartbeatAsync()
    {
        if (_producer == null) return;

        var heartbeat = new WorkerHeartbeat
        {
            WorkerId = _workerId,
            RestUrl = _restUrl,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Generation = _generation,
            AssignedConnectors = _assignments.Values
                .Where(a => a.WorkerId == _workerId)
                .Select(a => a.ConnectorName)
                .ToList(),
            AvailableTypes = BuildAvailableTypes()
        };

        var value = JsonSerializer.Serialize(heartbeat);
        await _producer.ProduceAsync(_config.StatusTopic, _workerId, value);
    }

    private IReadOnlyList<ConnectorCapability> BuildAvailableTypes()
    {
        if (_pluginDiscovery == null)
            return [];

        return _pluginDiscovery.GetAllPlugins()
            .Select(p => new ConnectorCapability(
                p.Class,
                p.Type,
                p.DisplayName ?? p.Class.Split('.')[^1],
                p.Version))
            .ToList();
    }

    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        var heartbeatInterval = TimeSpan.FromSeconds(3);
        var sessionTimeout = TimeSpan.FromSeconds(30);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await SendHeartbeatAsync();

                // Check for expired workers
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var expiredWorkers = _workers.Values
                    .Where(w => now - w.LastHeartbeat > sessionTimeout.TotalMilliseconds)
                    .ToList();

                foreach (var worker in expiredWorkers)
                {
                    if (_workers.TryRemove(worker.WorkerId, out _))
                    {
                        _logger.LogWarning("Worker {WorkerId} expired", worker.WorkerId);
                        _aggregatedRegistry?.RemoveWorker(worker.WorkerId);
                        WorkerDisconnected?.Invoke(this, new WorkerDisconnectedEventArgs(
                            worker.WorkerId, worker.RestUrl));
                        await TriggerRebalanceAsync();
                    }
                }

                await Task.Delay(heartbeatInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in heartbeat loop");
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }

    private async Task AssignmentLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_consumer == null) break;

                var result = await _consumer.ConsumeAsync(TimeSpan.FromSeconds(1), cancellationToken);
                if (result == null || result.Value == null) continue;

                // Process heartbeat from another worker
                var heartbeat = JsonSerializer.Deserialize<WorkerHeartbeat>(result.Value);
                if (heartbeat == null) continue;

                ProcessHeartbeat(heartbeat);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in assignment loop");
                await Task.Delay(1000, cancellationToken);
            }
        }
    }

    private void ProcessHeartbeat(WorkerHeartbeat heartbeat)
    {
        var isNew = !_workers.ContainsKey(heartbeat.WorkerId);

        _workers.AddOrUpdate(
            heartbeat.WorkerId,
            _ => new WorkerInfo
            {
                WorkerId = heartbeat.WorkerId,
                RestUrl = heartbeat.RestUrl,
                LastHeartbeat = heartbeat.Timestamp,
                AssignedConnectors = heartbeat.AssignedConnectors,
                AvailableTypes = heartbeat.AvailableTypes
            },
            (_, existing) =>
            {
                existing.LastHeartbeat = heartbeat.Timestamp;
                existing.RestUrl = heartbeat.RestUrl;
                existing.AssignedConnectors = heartbeat.AssignedConnectors;
                existing.AvailableTypes = heartbeat.AvailableTypes;
                return existing;
            });

        // Update the aggregated registry with remote capabilities
        _aggregatedRegistry?.UpdateFromHeartbeat(heartbeat.WorkerId, heartbeat.AvailableTypes);

        if (isNew)
        {
            _logger.LogInformation("New worker joined: {WorkerId} at {RestUrl}", heartbeat.WorkerId, heartbeat.RestUrl);
            _ = TriggerRebalanceAsync();
        }

        // Determine leader (first worker alphabetically)
        var leader = _workers.Keys.OrderBy(w => w).FirstOrDefault();
        var wasLeader = _isLeader;
        _isLeader = leader == _workerId;
        LeaderId = leader;

        if (_isLeader && !wasLeader)
        {
            _logger.LogInformation("This worker ({WorkerId}) is now the leader", _workerId);
        }
    }

    /// <summary>
    /// Trigger a rebalance of connector tasks across workers.
    /// </summary>
    public async Task TriggerRebalanceAsync()
    {
        if (!_isLeader)
        {
            _logger.LogDebug("Not leader, skipping rebalance");
            return;
        }

        await _rebalanceLock.WaitAsync();
        try
        {
            _generation++;
            _logger.LogInformation("Starting rebalance, generation {Generation}", _generation);

            // Collect all connectors that need tasks
            var allAssignments = _assignments.Values.ToList();
            var workerList = _workers.Values.ToList();

            if (workerList.Count == 0)
            {
                _logger.LogWarning("No workers available for task assignment");
                return;
            }

            // Round-robin assignment
            var workerIndex = 0;
            var newAssignments = new Dictionary<string, ConnectorTaskAssignment>();

            foreach (var assignment in allAssignments)
            {
                var worker = workerList[workerIndex % workerList.Count];
                newAssignments[assignment.ConnectorName] = new ConnectorTaskAssignment
                {
                    ConnectorName = assignment.ConnectorName,
                    WorkerId = worker.WorkerId,
                    TaskIds = assignment.TaskIds,
                    Generation = _generation
                };
                workerIndex++;
            }

            // Publish new assignments
            foreach (var (connectorName, assignment) in newAssignments)
            {
                _assignments[connectorName] = assignment;

                if (_producer != null)
                {
                    var key = $"assignment-{connectorName}";
                    var value = JsonSerializer.Serialize(assignment);
                    await _producer.ProduceAsync(_config.StatusTopic, key, value);
                }

                // Notify if assigned to this worker
                if (assignment.WorkerId == _workerId)
                {
                    TasksAssigned?.Invoke(this, new TasksAssignedEventArgs(assignment));
                }
            }

            _logger.LogInformation("Rebalance complete, {Count} connectors assigned", newAssignments.Count);
        }
        finally
        {
            _rebalanceLock.Release();
        }
    }

    /// <summary>
    /// Register a connector for task distribution.
    /// </summary>
    public async Task RegisterConnectorAsync(string connectorName, int taskCount)
    {
        var taskIds = Enumerable.Range(0, taskCount).ToList();

        _assignments[connectorName] = new ConnectorTaskAssignment
        {
            ConnectorName = connectorName,
            WorkerId = _workerId,
            TaskIds = taskIds,
            Generation = _generation
        };

        _logger.LogInformation("Registered connector {Connector} with {TaskCount} tasks",
            connectorName, taskCount);

        await TriggerRebalanceAsync();
    }

    /// <summary>
    /// Unregister a connector.
    /// </summary>
    public Task UnregisterConnectorAsync(string connectorName)
    {
        if (_assignments.TryRemove(connectorName, out var assignment))
        {
            _logger.LogInformation("Unregistered connector {Connector}", connectorName);

            var taskIds = assignment.TaskIds.Select(t => $"{connectorName}-{t}").ToList();
            TasksRevoked?.Invoke(this, new TasksRevokedEventArgs(taskIds));
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Leave the worker group gracefully.
    /// </summary>
    public async Task LeaveGroupAsync()
    {
        _logger.LogInformation("Leaving worker group");

        // Send leaving heartbeat
        if (_producer != null)
        {
            var heartbeat = new WorkerHeartbeat
            {
                WorkerId = _workerId,
                RestUrl = _restUrl,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Generation = _generation,
                IsLeaving = true,
                AssignedConnectors = [],
                AvailableTypes = []
            };
            var value = JsonSerializer.Serialize(heartbeat);
            await _producer.ProduceAsync(_config.StatusTopic, _workerId, value);
        }

        _workers.TryRemove(_workerId, out _);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;

        _heartbeatCts?.Cancel();

        if (_heartbeatTask != null)
        {
            try
            {
                await _heartbeatTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            catch { }
        }

        if (_assignmentTask != null)
        {
            try
            {
                await _assignmentTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            catch { }
        }

        if (_consumer != null)
            await _consumer.DisposeAsync();
        if (_producer != null)
            await _producer.DisposeAsync();
        _heartbeatCts?.Dispose();
        _rebalanceLock.Dispose();

        _disposed = true;
    }
}

/// <summary>
/// Event args for tasks assigned event.
/// </summary>
public sealed class TasksAssignedEventArgs : EventArgs
{
    public ConnectorTaskAssignment Assignment { get; }

    public TasksAssignedEventArgs(ConnectorTaskAssignment assignment)
    {
        Assignment = assignment;
    }
}

/// <summary>
/// Event args for tasks revoked event.
/// </summary>
public sealed class TasksRevokedEventArgs : EventArgs
{
    public IReadOnlyList<string> TaskIds { get; }

    public TasksRevokedEventArgs(IEnumerable<string> taskIds)
    {
        TaskIds = taskIds.ToList();
    }
}

/// <summary>
/// Event args raised when a worker is considered disconnected.
/// </summary>
public sealed class WorkerDisconnectedEventArgs : EventArgs
{
    /// <summary>
    /// ID of the disconnected worker.
    /// </summary>
    public string WorkerId { get; }

    /// <summary>
    /// Last known REST URL of the disconnected worker.
    /// </summary>
    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings", Justification = "REST URL stored as string for simplicity")]
    public string RestUrl { get; }

    public WorkerDisconnectedEventArgs(string workerId, string restUrl)
    {
        WorkerId = workerId;
        RestUrl = restUrl;
    }
}

/// <summary>
/// Information about a Connect worker.
/// </summary>
public sealed class WorkerInfo
{
    public required string WorkerId { get; init; }

    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings", Justification = "REST URL stored as string for JSON serialization")]
    public string RestUrl { get; set; } = "";

    public long LastHeartbeat { get; set; }
    public IList<string> AssignedConnectors { get; set; } = [];

    /// <summary>
    /// Connector types this worker can instantiate.
    /// </summary>
    public IReadOnlyList<ConnectorCapability> AvailableTypes { get; set; } = [];

    /// <summary>
    /// Role tags for placement decisions (e.g., "edge", "gpu", "high-memory").
    /// </summary>
    public IReadOnlyList<string> Tags { get; set; } = [];

    /// <summary>
    /// Whether this worker allows on-demand plugin auto-installation.
    /// </summary>
    public bool AllowAutoInstall { get; set; }
}

/// <summary>
/// Heartbeat message sent by workers.
/// </summary>
public sealed class WorkerHeartbeat
{
    public required string WorkerId { get; init; }

    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings", Justification = "REST URL stored as string for JSON serialization")]
    public required string RestUrl { get; init; }

    public long Timestamp { get; init; }
    public int Generation { get; init; }
    public bool IsLeaving { get; init; }
    public IList<string> AssignedConnectors { get; init; } = [];

    /// <summary>
    /// Connector types this worker can instantiate (from its local plugin discovery).
    /// </summary>
    public IReadOnlyList<ConnectorCapability> AvailableTypes { get; init; } = [];

    /// <summary>
    /// Role tags for placement decisions.
    /// </summary>
    public IReadOnlyList<string> Tags { get; init; } = [];

    /// <summary>
    /// Whether this worker allows on-demand plugin auto-installation.
    /// </summary>
    public bool AllowAutoInstall { get; init; }
}
