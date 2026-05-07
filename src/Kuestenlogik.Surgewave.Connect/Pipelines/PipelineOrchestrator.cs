using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Orchestrates pipeline lifecycle - starting, stopping, and monitoring pipelines.
/// Supports both local and distributed execution modes, routing connector tasks
/// to remote workers when distributed mode is enabled and workers are available.
/// </summary>
public sealed class PipelineOrchestrator : IAsyncDisposable
{
    private readonly PipelineStore _store;
    private readonly PipelineTopicManager _topicManager;
    private readonly ConnectWorker _worker;
    private readonly PluginDiscovery _pluginDiscovery;
    private readonly ILogger<PipelineOrchestrator> _logger;
    private readonly ConcurrentDictionary<string, PipelineRunState> _runningPipelines = new();
    private readonly PipelineMetricsCollector _metricsCollector;
    private readonly PipelineVersionStore _versionStore = new();
    private readonly TaskAssignmentTracker _assignmentTracker = new();

    private AggregatedConnectorRegistry? _aggregatedRegistry;
    private WorkerCoordinator? _workerCoordinator;
    private ConnectWorkerConfig? _workerConfig;
    private IProducer<string, string>? _configTopicProducer;

    /// <summary>
    /// Gets the task assignment tracker for querying worker assignments.
    /// </summary>
    public TaskAssignmentTracker AssignmentTracker => _assignmentTracker;

    public PipelineOrchestrator(
        PipelineStore store,
        PipelineTopicManager topicManager,
        ConnectWorker worker,
        PluginDiscovery pluginDiscovery,
        ILogger<PipelineOrchestrator> logger,
        PipelineMetricsCollector? metricsCollector = null)
    {
        _metricsCollector = metricsCollector ?? new PipelineMetricsCollector();
        _store = store;
        _topicManager = topicManager;
        _worker = worker;
        _pluginDiscovery = pluginDiscovery;
        _logger = logger;
    }

    /// <summary>
    /// Configures distributed mode support with the aggregated registry and worker coordinator.
    /// </summary>
    public void ConfigureDistributedMode(
        ConnectWorkerConfig workerConfig,
        AggregatedConnectorRegistry aggregatedRegistry,
        WorkerCoordinator workerCoordinator)
    {
        _workerConfig = workerConfig;
        _aggregatedRegistry = aggregatedRegistry;
        _workerCoordinator = workerCoordinator;

        // Subscribe to worker disconnect events for task reassignment
        workerCoordinator.WorkerDisconnected += OnWorkerDisconnected;
    }

    /// <summary>
    /// Sets the producer for publishing task assignments to the config topic.
    /// </summary>
    public void SetConfigTopicProducer(IProducer<string, string> producer)
    {
        _configTopicProducer = producer;
    }

    /// <summary>
    /// Gets the aggregated connector registry (for validation).
    /// </summary>
    public AggregatedConnectorRegistry? GetAggregatedRegistry() => _aggregatedRegistry;

    /// <summary>
    /// Gets the current worker snapshot as a dictionary (for validation).
    /// </summary>
    public IReadOnlyDictionary<string, WorkerInfo>? GetWorkers()
    {
        var workers = _workerCoordinator?.Workers;
        if (workers == null) return null;
        return workers.ToDictionary(w => w.WorkerId, w => w);
    }

    /// <summary>
    /// Whether distributed mode is enabled and configured.
    /// </summary>
    public bool IsDistributedModeEnabled =>
        _workerConfig?.DistributedMode == true
        && _aggregatedRegistry != null
        && _workerCoordinator != null;

    /// <summary>
    /// Initialize the orchestrator and restore running pipelines.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _store.LoadAsync(cancellationToken);

        // Restore running pipelines
        foreach (var pipeline in _store.GetAll())
        {
            if (pipeline.Status == PipelineStatus.Running)
            {
                try
                {
                    await StartPipelineInternalAsync(pipeline, cancellationToken);
                    _logger.LogInformation("Restored running pipeline: {Id}", pipeline.Id);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to restore pipeline: {Id}", pipeline.Id);
                    await _store.UpdateStatusAsync(pipeline.Id, PipelineStatus.Failed, ex.Message, cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// Get all pipelines.
    /// </summary>
    public IReadOnlyList<PipelineDefinition> GetAll()
    {
        return _store.GetAll();
    }

    /// <summary>
    /// Get a pipeline by ID.
    /// </summary>
    public PipelineDefinition? Get(string id)
    {
        return _store.Get(id);
    }

    /// <summary>
    /// Create a new pipeline.
    /// </summary>
    public async Task<PipelineDefinition> CreateAsync(
        string name,
        string? description,
        List<PipelineNode> nodes,
        List<PipelineConnection> connections,
        Dictionary<string, string>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var id = Guid.NewGuid().ToString("N");

        // Assign internal topic names to connections
        var connectionsWithTopics = PipelineTopicManager.AssignTopicNames(id, connections);

        var pipeline = new PipelineDefinition
        {
            Id = id,
            Name = name,
            Description = description,
            Nodes = nodes,
            Connections = connectionsWithTopics,
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow,
            Parameters = parameters
        };

        await _store.SaveAsync(pipeline, cancellationToken);
        _versionStore.SaveVersion(id, pipeline, "Created");
        _logger.LogInformation("Created pipeline: {Id} ({Name})", id, name);

        return pipeline;
    }

    /// <summary>
    /// Update a pipeline definition.
    /// </summary>
    public async Task<PipelineDefinition> UpdateAsync(
        string id,
        string name,
        string? description,
        List<PipelineNode> nodes,
        List<PipelineConnection> connections,
        Dictionary<string, string>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var existing = _store.Get(id)
            ?? throw new InvalidOperationException($"Pipeline '{id}' not found");

        if (existing.Status == PipelineStatus.Running)
        {
            throw new InvalidOperationException("Cannot update a running pipeline. Stop it first.");
        }

        // Assign internal topic names to connections
        var connectionsWithTopics = PipelineTopicManager.AssignTopicNames(id, connections);

        var updated = existing with
        {
            Name = name,
            Description = description,
            Nodes = nodes,
            Connections = connectionsWithTopics,
            UpdatedAt = DateTimeOffset.UtcNow,
            Parameters = parameters ?? existing.Parameters
        };

        await _store.SaveAsync(updated, cancellationToken);
        _versionStore.SaveVersion(id, updated, "Updated");
        _logger.LogInformation("Updated pipeline: {Id} ({Name})", id, name);

        return updated;
    }

    /// <summary>
    /// Delete a pipeline.
    /// </summary>
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        var pipeline = _store.Get(id)
            ?? throw new InvalidOperationException($"Pipeline '{id}' not found");

        if (pipeline.Status == PipelineStatus.Running)
        {
            await StopAsync(id, cancellationToken);
        }

        // Delete internal topics
        await _topicManager.DeleteTopicsForPipelineAsync(pipeline, cancellationToken);

        await _store.DeleteAsync(id, cancellationToken);
        _logger.LogInformation("Deleted pipeline: {Id}", id);
    }

    /// <summary>
    /// Start a pipeline.
    /// </summary>
    public async Task StartAsync(string id, Dictionary<string, string>? parameterOverrides = null, CancellationToken cancellationToken = default)
    {
        var pipeline = _store.Get(id)
            ?? throw new InvalidOperationException($"Pipeline '{id}' not found");

        if (pipeline.Status == PipelineStatus.Running)
        {
            _logger.LogDebug("Pipeline {Id} is already running", id);
            return;
        }

        // Merge parameters: definition defaults + runtime overrides
        if (parameterOverrides is { Count: > 0 })
        {
            var mergedParams = new Dictionary<string, string>(pipeline.Parameters ?? []);
            foreach (var (key, value) in parameterOverrides)
            {
                mergedParams[key] = value;
            }

            pipeline = pipeline with { Parameters = mergedParams };
        }

        await StartPipelineInternalAsync(pipeline, cancellationToken);
        await _store.UpdateStatusAsync(id, PipelineStatus.Running, null, cancellationToken);
    }

    /// <summary>
    /// Stop a pipeline.
    /// </summary>
    public async Task StopAsync(string id, CancellationToken cancellationToken = default)
    {
        var pipeline = _store.Get(id)
            ?? throw new InvalidOperationException($"Pipeline '{id}' not found");

        if (pipeline.Status != PipelineStatus.Running)
        {
            _logger.LogDebug("Pipeline {Id} is not running", id);
            return;
        }

        await StopPipelineInternalAsync(pipeline, cancellationToken);
        await _store.UpdateStatusAsync(id, PipelineStatus.Stopped, null, cancellationToken);
    }

    /// <summary>
    /// Update pipeline schedule.
    /// </summary>
    public async Task UpdateScheduleAsync(string id, ScheduleConfig schedule, CancellationToken cancellationToken = default)
    {
        await _store.UpdateScheduleAsync(id, schedule, cancellationToken);
    }

    /// <summary>
    /// Hot-deploy config changes to a running pipeline. Returns true if successful.
    /// </summary>
    public async Task<bool> HotDeployAsync(string id, PipelineDefinition proposed, CancellationToken cancellationToken = default)
    {
        var current = _store.Get(id)
            ?? throw new InvalidOperationException($"Pipeline '{id}' not found");

        if (current.Status != PipelineStatus.Running)
            return false;

        var analysis = HotDeployAnalyzer.Analyze(current, proposed);
        if (!analysis.IsHotDeployable)
            return false;

        // Apply config changes to running connectors
        foreach (var change in analysis.HotDeployConfigChangedNodes)
        {
            if (!_runningPipelines.TryGetValue(id, out var runState))
                return false;

            if (!runState.NodeConnectors.TryGetValue(change.NodeId, out var connectorName))
                continue;

            var newConfig = new Dictionary<string, string>();
            var node = proposed.Nodes.FirstOrDefault(n => n.Id == change.NodeId);
            if (node != null)
            {
                foreach (var (key, value) in node.Config)
                    newConfig[key] = value;
            }

            await _worker.PauseConnectorAsync(connectorName);
            await _worker.UpdateConnectorConfigAsync(connectorName, newConfig);
            await _worker.ResumeConnectorAsync(connectorName);
        }

        // Persist the updated definition
        await _store.SaveAsync(proposed with { Status = PipelineStatus.Running, UpdatedAt = DateTimeOffset.UtcNow }, cancellationToken);
        _logger.LogInformation("Hot-deployed changes to pipeline {Id}", id);
        return true;
    }

    /// <summary>
    /// Get the runtime status of a pipeline.
    /// </summary>
    public PipelineRuntimeStatus? GetStatus(string id)
    {
        var pipeline = _store.Get(id);
        if (pipeline == null)
        {
            return null;
        }

        var nodeStatuses = new List<NodeStatus>();

        if (_runningPipelines.TryGetValue(id, out var runState))
        {
            foreach (var (nodeId, connectorName) in runState.NodeConnectors)
            {
                // Check if this node is running on a remote worker
                var isRemote = runState.RemoteNodes.TryGetValue(nodeId, out var remoteWorkerId);

                var connectorInfo = isRemote ? null : _worker.GetConnectorStatus(connectorName);
                nodeStatuses.Add(new NodeStatus
                {
                    NodeId = nodeId,
                    ConnectorName = connectorName,
                    State = connectorInfo?.State ?? (isRemote ? "Running" : "Unknown"),
                    TaskCount = connectorInfo?.Tasks.Count ?? (isRemote ? 1 : 0),
                    Error = connectorInfo?.Tasks.FirstOrDefault(t => !string.IsNullOrEmpty(t.Trace))?.Trace,
                    WorkerId = remoteWorkerId
                });
            }
        }

        return new PipelineRuntimeStatus
        {
            PipelineId = id,
            Status = pipeline.Status,
            Nodes = nodeStatuses
        };
    }

    /// <summary>
    /// Get pipeline metrics.
    /// </summary>
    public PipelineMetrics? GetMetrics(string pipelineId) => _metricsCollector.GetMetrics(pipelineId);

    /// <summary>
    /// Get version history for a pipeline.
    /// </summary>
    public List<PipelineVersionEntry> GetVersions(string pipelineId) => _versionStore.GetVersions(pipelineId);

    /// <summary>
    /// Get a specific version.
    /// </summary>
    public PipelineVersionEntry? GetVersion(string pipelineId, int version) => _versionStore.GetVersion(pipelineId, version);

    /// <summary>
    /// Get diff between two versions.
    /// </summary>
    public PipelineVersionDiff? GetVersionDiff(string pipelineId, int fromVersion, int toVersion)
        => _versionStore.GetDiff(pipelineId, fromVersion, toVersion);

    /// <summary>
    /// Rollback a pipeline to a previous version.
    /// </summary>
    public async Task<PipelineDefinition> RollbackAsync(string pipelineId, int version, CancellationToken cancellationToken = default)
    {
        var versionEntry = _versionStore.GetVersion(pipelineId, version)
            ?? throw new InvalidOperationException($"Version {version} not found for pipeline '{pipelineId}'");

        var def = versionEntry.Definition;
        return await UpdateAsync(pipelineId, def.Name, def.Description, def.Nodes, def.Connections, def.Parameters, cancellationToken);
    }

    /// <summary>
    /// Run a pipeline in dry-run mode with sample data.
    /// </summary>
    public async Task<DryRunResult> DryRunAsync(string pipelineId, List<DryRunInput> inputs, CancellationToken cancellationToken = default)
    {
        var pipeline = _store.Get(pipelineId)
            ?? throw new InvalidOperationException($"Pipeline '{pipelineId}' not found");

        var runner = new PipelineDryRunner();
        return await runner.RunAsync(pipeline, inputs, cancellationToken);
    }

    private async Task StartPipelineInternalAsync(PipelineDefinition pipeline, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting pipeline: {Id} ({Name})", pipeline.Id, pipeline.Name);

        // Pipeline services are now passed per-instance via TaskContext (no more static state)

        // Create internal topics
        await _topicManager.CreateTopicsForPipelineAsync(pipeline, cancellationToken);

        var runState = new PipelineRunState();

        // Build variable resolution context
        var variableContext = new PipelineVariableContext
        {
            PipelineId = pipeline.Id,
            PipelineName = pipeline.Name,
            Parameters = pipeline.Parameters ?? new Dictionary<string, string>()
        };

        // Build node dependency graph
        var nodeOutputTopics = new Dictionary<string, string>();
        var nodeInputTopics = new Dictionary<string, List<string>>();
        var nodeErrorOutputTopics = new Dictionary<string, string>();

        foreach (var node in pipeline.Nodes)
        {
            nodeInputTopics[node.Id] = [];
        }

        foreach (var connection in pipeline.Connections)
        {
            var topic = connection.InternalTopic!;

            if (connection.Type == PipelineConnectionType.Error)
            {
                nodeErrorOutputTopics[connection.SourceNodeId] = topic;
            }
            else
            {
                nodeOutputTopics[connection.SourceNodeId] = topic;
            }

            nodeInputTopics[connection.TargetNodeId].Add(topic);
        }

        // Start each node as a connector
        foreach (var node in pipeline.Nodes)
        {
            var connectorName = $"pipeline-{pipeline.Id}-{node.Id}";

            var config = new Dictionary<string, string>(node.Config)
            {
                ["connector.class"] = node.ConnectorType,
                ["name"] = connectorName,
                ["node.id"] = node.Id,
                ["pipeline.id"] = pipeline.Id
            };

            // Set error topic if error connection exists
            if (nodeErrorOutputTopics.TryGetValue(node.Id, out var errorTopic))
            {
                config["error.topic"] = errorTopic;
            }

            // For source nodes that output to internal topics
            if (nodeOutputTopics.TryGetValue(node.Id, out var outputTopic))
            {
                // Check if this is a source connector
                var connectorType = Type.GetType(node.ConnectorType);
                if (connectorType != null && typeof(SourceConnector).IsAssignableFrom(connectorType))
                {
                    config["topic"] = outputTopic;
                }
            }

            // For sink nodes that read from internal topics
            if (nodeInputTopics.TryGetValue(node.Id, out var inputTopics) && inputTopics.Count > 0)
            {
                var connectorType = Type.GetType(node.ConnectorType);
                if (connectorType != null && typeof(SinkConnector).IsAssignableFrom(connectorType))
                {
                    config["topics"] = string.Join(",", inputTopics);
                }
            }

            // Inject retry policy config keys
            if (node.RetryPolicy is { Enabled: true } retry)
            {
                config["_retry.enabled"] = "true";
                config["_retry.max.attempts"] = retry.MaxRetries.ToString();
                config["_retry.backoff.ms"] = retry.BackoffMs.ToString();
                config["_retry.backoff.multiplier"] = retry.BackoffMultiplier.ToString(System.Globalization.CultureInfo.InvariantCulture);
                config["_retry.max.backoff.ms"] = retry.MaxBackoffMs.ToString();
            }

            // Resolve pipeline variables in config
            config = PipelineVariableResolver.Resolve(config, variableContext with { NodeId = node.Id }, _logger);

            try
            {
                // Check if this node should be assigned to a remote worker
                var targetWorkerId = ResolveTargetWorker(node);

                if (targetWorkerId != null)
                {
                    // Remote assignment: publish task to config topic for the remote worker
                    await AssignTaskToWorkerAsync(targetWorkerId, connectorName, node, config);
                    runState.NodeConnectors[node.Id] = connectorName;
                    runState.RemoteNodes[node.Id] = targetWorkerId;
                    _assignmentTracker.TrackAssignment(connectorName, targetWorkerId, pipeline.Id, node.Id);
                    _logger.LogInformation("Assigned node {NodeId} to remote worker {WorkerId} as connector {Connector}",
                        node.Id, targetWorkerId, connectorName);
                }
                else
                {
                    // Local execution (existing behavior)
                    await _worker.CreateConnectorAsync(connectorName, node.ConnectorType, config);
                    runState.NodeConnectors[node.Id] = connectorName;
                    _logger.LogDebug("Started node {NodeId} as connector {Connector}", node.Id, connectorName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start node {NodeId}", node.Id);
                // Stop already started nodes
                await StopPipelineInternalAsync(pipeline, cancellationToken);
                throw;
            }
        }

        _runningPipelines[pipeline.Id] = runState;
        _logger.LogInformation("Pipeline {Id} started with {NodeCount} nodes", pipeline.Id, pipeline.Nodes.Count);
    }

    /// <summary>
    /// Determines which worker should execute a pipeline node.
    /// Returns null if the node should run locally.
    /// Uses <see cref="PlacementConfig"/> from the node for placement decisions.
    /// </summary>
    private string? ResolveTargetWorker(PipelineNode node)
    {
        if (!IsDistributedModeEnabled)
            return null;

        var placement = node.Placement ?? new PlacementConfig();

        return placement.Strategy switch
        {
            PlacementStrategy.Manual => ResolveManualPlacement(node, placement),
            PlacementStrategy.TagBased => ResolveTagBasedPlacement(node, placement),
            _ => ResolveAutoPlacement(node, placement)
        };
    }

    private string? ResolveManualPlacement(PipelineNode node, PlacementConfig placement)
    {
        var workerId = placement.WorkerId;

        // Fall back to legacy _worker config key
        if (string.IsNullOrEmpty(workerId) &&
            node.Config.TryGetValue("_worker", out var legacyWorker) &&
            !string.IsNullOrEmpty(legacyWorker) &&
            !legacyWorker.Equals("auto", StringComparison.OrdinalIgnoreCase))
        {
            workerId = legacyWorker;
        }

        if (string.IsNullOrEmpty(workerId))
        {
            _logger.LogWarning("Manual placement for node {NodeId} has no WorkerId, falling back to auto",
                node.Id);
            return ResolveAutoPlacement(node, placement);
        }

        var workers = _workerCoordinator!.Workers;
        if (workers.Any(w => w.WorkerId.Equals(workerId, StringComparison.Ordinal)))
        {
            return workerId;
        }

        _logger.LogWarning("Manual placement: worker {WorkerId} not available for node {NodeId}",
            workerId, node.Id);
        return null;
    }

    private string? ResolveTagBasedPlacement(PipelineNode node, PlacementConfig placement)
    {
        if (placement.RequiredTags.Length == 0)
        {
            _logger.LogWarning("TagBased placement for node {NodeId} has no tags, falling back to auto",
                node.Id);
            return ResolveAutoPlacement(node, placement);
        }

        // Find workers with required tags AND the connector type
        var candidates = _aggregatedRegistry!.GetWorkersForTypeWithTags(
            node.ConnectorType, placement.RequiredTags);

        if (candidates.Count == 0)
        {
            // Try workers with tags but without the plugin (for auto-install)
            var taggedWorkers = _aggregatedRegistry.GetWorkersWithTags(placement.RequiredTags);
            if (taggedWorkers.Count > 0 && placement.AllowAutoInstall)
            {
                // Pick a tagged worker that allows auto-install
                var autoInstallWorker = taggedWorkers.FirstOrDefault(wId =>
                    _aggregatedRegistry.GetWorkerMetadata(wId)?.AllowAutoInstall == true);

                if (autoInstallWorker != null)
                {
                    _logger.LogInformation(
                        "TagBased placement: worker {WorkerId} has tags [{Tags}] but missing plugin '{ConnectorType}' â€” auto-install expected",
                        autoInstallWorker, string.Join(", ", placement.RequiredTags), node.ConnectorType);
                    return autoInstallWorker;
                }
            }

            _logger.LogWarning(
                "No worker with tags [{Tags}] can handle '{ConnectorType}' for node {NodeId}",
                string.Join(", ", placement.RequiredTags), node.ConnectorType, node.Id);
            return null;
        }

        return PickLeastLoadedWorker(candidates);
    }

    private string? ResolveAutoPlacement(PipelineNode node, PlacementConfig placement)
    {
        // Legacy _worker config key support
        if (node.Config.TryGetValue("_worker", out var preferredWorker)
            && !string.IsNullOrEmpty(preferredWorker)
            && !preferredWorker.Equals("auto", StringComparison.OrdinalIgnoreCase))
        {
            var workers = _workerCoordinator!.Workers;
            if (workers.Any(w => w.WorkerId.Equals(preferredWorker, StringComparison.Ordinal)))
            {
                return preferredWorker;
            }

            _logger.LogWarning("Preferred worker {WorkerId} not available for node {NodeId}, falling back",
                preferredWorker, node.Id);
        }

        // Check remote workers with the connector type
        var availableWorkers = _aggregatedRegistry!.GetWorkersForType(node.ConnectorType);
        if (availableWorkers.Count == 0)
        {
            return null; // Run locally
        }

        // Prefer local if available
        var localPlugins = _pluginDiscovery.GetAllPlugins();
        var isLocallyAvailable = localPlugins.Any(p =>
            p.Class.Equals(node.ConnectorType, StringComparison.Ordinal));
        if (isLocallyAvailable)
        {
            return null; // Local is preferred
        }

        return PickLeastLoadedWorker(availableWorkers);
    }

    private string? PickLeastLoadedWorker(IReadOnlyList<string> candidates)
    {
        var activeWorkers = _workerCoordinator!.Workers;
        string? bestWorker = null;
        var minAssignments = int.MaxValue;

        foreach (var workerId in candidates)
        {
            if (!activeWorkers.Any(w => w.WorkerId.Equals(workerId, StringComparison.Ordinal)))
                continue;

            var assignmentCount = _assignmentTracker.GetAssignmentsForWorker(workerId).Count;
            if (assignmentCount < minAssignments)
            {
                minAssignments = assignmentCount;
                bestWorker = workerId;
            }
        }

        return bestWorker;
    }

    /// <summary>
    /// Publishes a task assignment to the config topic for a remote worker to pick up.
    /// </summary>
    private async Task AssignTaskToWorkerAsync(
        string workerId,
        string connectorName,
        PipelineNode node,
        Dictionary<string, string> config)
    {
        if (_configTopicProducer == null || _workerConfig == null)
        {
            throw new InvalidOperationException(
                "Config topic producer not configured. Call SetConfigTopicProducer before starting distributed pipelines.");
        }

        var assignment = new RemoteTaskAssignment
        {
            ConnectorName = connectorName,
            ConnectorType = node.ConnectorType,
            WorkerId = workerId,
            Config = config,
            PipelineId = config.GetValueOrDefault("pipeline.id") ?? "",
            NodeId = node.Id,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var key = $"task-assignment-{connectorName}";
        var value = JsonSerializer.Serialize(assignment);
        await _configTopicProducer.ProduceAsync(_workerConfig.ConfigTopic, key, value);

        _logger.LogDebug("Published task assignment for {Connector} to worker {WorkerId} on topic {Topic}",
            connectorName, workerId, _workerConfig.ConfigTopic);
    }

    /// <summary>
    /// Handles worker disconnect events by reassigning tasks to other workers or local execution.
    /// </summary>
    private async void OnWorkerDisconnected(object? sender, WorkerDisconnectedEventArgs e)
    {
        var orphanedAssignments = _assignmentTracker.RemoveWorkerAssignments(e.WorkerId);
        if (orphanedAssignments.Count == 0)
            return;

        _logger.LogWarning("Worker {WorkerId} disconnected, reassigning {Count} task(s)",
            e.WorkerId, orphanedAssignments.Count);

        foreach (var assignment in orphanedAssignments)
        {
            try
            {
                // Try to find the pipeline and node for this assignment
                var pipeline = _store.Get(assignment.PipelineId);
                if (pipeline == null || pipeline.Status != PipelineStatus.Running)
                    continue;

                var node = pipeline.Nodes.FirstOrDefault(n => n.Id == assignment.NodeId);
                if (node == null)
                    continue;

                if (!_runningPipelines.TryGetValue(assignment.PipelineId, out var runState))
                    continue;

                // Try to find an alternative remote worker
                var newWorkerId = ResolveTargetWorker(node);

                if (newWorkerId != null)
                {
                    // Reassign to another remote worker
                    var config = new Dictionary<string, string>(node.Config)
                    {
                        ["connector.class"] = node.ConnectorType,
                        ["name"] = assignment.ConnectorName,
                        ["node.id"] = node.Id,
                        ["pipeline.id"] = assignment.PipelineId
                    };

                    await AssignTaskToWorkerAsync(newWorkerId, assignment.ConnectorName, node, config);
                    runState.RemoteNodes[node.Id] = newWorkerId;
                    _assignmentTracker.TrackAssignment(
                        assignment.ConnectorName, newWorkerId, assignment.PipelineId, assignment.NodeId);

                    _logger.LogInformation(
                        "Reassigned node {NodeId} from worker {OldWorker} to {NewWorker}",
                        node.Id, e.WorkerId, newWorkerId);
                }
                else
                {
                    // Fall back to local execution
                    await _worker.CreateConnectorAsync(
                        assignment.ConnectorName, node.ConnectorType,
                        new Dictionary<string, string>(node.Config)
                        {
                            ["connector.class"] = node.ConnectorType,
                            ["name"] = assignment.ConnectorName,
                            ["node.id"] = node.Id,
                            ["pipeline.id"] = assignment.PipelineId
                        });

                    runState.RemoteNodes.TryRemove(node.Id, out _);
                    _logger.LogInformation(
                        "Reassigned node {NodeId} from worker {OldWorker} to local execution",
                        node.Id, e.WorkerId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to reassign connector {Connector} from disconnected worker {WorkerId}",
                    assignment.ConnectorName, e.WorkerId);

                // Update pipeline status to indicate failure
                await _store.UpdateStatusAsync(assignment.PipelineId, PipelineStatus.Failed,
                    $"Worker {e.WorkerId} disconnected and task reassignment failed: {ex.Message}");
            }
        }
    }

    private async Task StopPipelineInternalAsync(PipelineDefinition pipeline, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping pipeline: {Id}", pipeline.Id);

        _metricsCollector.Reset(pipeline.Id);

        if (!_runningPipelines.TryRemove(pipeline.Id, out var runState))
        {
            return;
        }

        foreach (var (nodeId, connectorName) in runState.NodeConnectors)
        {
            try
            {
                if (runState.RemoteNodes.TryGetValue(nodeId, out var remoteWorkerId))
                {
                    // For remote nodes, publish a stop command on the config topic
                    await PublishStopCommandAsync(connectorName, remoteWorkerId);
                    _assignmentTracker.RemoveAssignment(connectorName);
                    _logger.LogDebug("Sent stop command for remote node {NodeId} on worker {WorkerId}", nodeId, remoteWorkerId);
                }
                else
                {
                    await _worker.StopConnectorAsync(connectorName);
                    _logger.LogDebug("Stopped node {NodeId} connector {Connector}", nodeId, connectorName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to stop node {NodeId}", nodeId);
            }
        }

        _logger.LogInformation("Pipeline {Id} stopped", pipeline.Id);
    }

    /// <summary>
    /// Publishes a stop command on the config topic for a remote worker.
    /// </summary>
    private async Task PublishStopCommandAsync(string connectorName, string workerId)
    {
        if (_configTopicProducer == null || _workerConfig == null)
            return;

        var command = new RemoteTaskCommand
        {
            ConnectorName = connectorName,
            WorkerId = workerId,
            Command = "stop",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var key = $"task-command-{connectorName}";
        var value = JsonSerializer.Serialize(command);
        await _configTopicProducer.ProduceAsync(_workerConfig.ConfigTopic, key, value);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workerCoordinator != null)
        {
            _workerCoordinator.WorkerDisconnected -= OnWorkerDisconnected;
        }

        foreach (var (pipelineId, _) in _runningPipelines)
        {
            var pipeline = _store.Get(pipelineId);
            if (pipeline != null)
            {
                await StopPipelineInternalAsync(pipeline, CancellationToken.None);
            }
        }

        if (_configTopicProducer != null)
        {
            await _configTopicProducer.DisposeAsync();
        }
    }
}

/// <summary>
/// Internal state for a running pipeline.
/// </summary>
internal sealed class PipelineRunState
{
    public Dictionary<string, string> NodeConnectors { get; } = new();

    /// <summary>
    /// Maps node IDs to their remote worker IDs. Empty for locally-running nodes.
    /// </summary>
    public ConcurrentDictionary<string, string> RemoteNodes { get; } = new();
}

/// <summary>
/// Runtime status of a pipeline.
/// </summary>
public record PipelineRuntimeStatus
{
    public required string PipelineId { get; init; }
    public PipelineStatus Status { get; init; }
    public required List<NodeStatus> Nodes { get; init; }
}

/// <summary>
/// Status of a single node in a pipeline.
/// </summary>
public record NodeStatus
{
    public required string NodeId { get; init; }
    public required string ConnectorName { get; init; }
    public required string State { get; init; }
    public int TaskCount { get; init; }
    public string? Error { get; init; }

    /// <summary>
    /// ID of the worker executing this node. Null if running locally.
    /// </summary>
    public string? WorkerId { get; init; }
}
