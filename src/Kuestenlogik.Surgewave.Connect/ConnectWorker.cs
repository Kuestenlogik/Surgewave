using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Connect.Dlq;
using Kuestenlogik.Surgewave.Connect.Eos;
using Kuestenlogik.Surgewave.Core.Dlq;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// The Connect worker manages connectors and their tasks.
/// Requires an <see cref="ISurgewaveClient"/> — call <see cref="SetSurgewaveClient"/> before starting.
/// </summary>
public sealed class ConnectWorker : IAsyncDisposable
{
    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<ConnectWorker> _logger;
    private readonly ConnectWorkerServices _services;
    private readonly ConcurrentDictionary<string, ConnectorState> _connectors = new();
    private readonly ConcurrentDictionary<string, List<ITaskRunner>> _taskRunners = new();
    private readonly CancellationTokenSource _cts = new();
    private Func<string, Type?>? _typeResolver;
    private readonly ISurgewaveClient _surgewaveClient;
    private readonly bool _ownsClient;
    private SurgewaveSourceOffsetStore? _eosOffsetStore;

    public ConnectWorker(
        ConnectWorkerConfig config,
        ISurgewaveClient surgewaveClient,
        ILogger<ConnectWorker> logger,
        ConnectWorkerServices? services = null,
        bool ownsClient = false)
    {
        _config = config;
        _surgewaveClient = surgewaveClient;
        _ownsClient = ownsClient;
        _logger = logger;
        _services = services ?? ConnectWorkerServices.None;
    }

    /// <summary>
    /// Sets a custom type resolver for loading connector classes.
    /// </summary>
    public void SetTypeResolver(Func<string, Type?> resolver)
    {
        _typeResolver = resolver;
    }

    /// <summary>
    /// Gets or creates the exactly-once source offset store.
    /// </summary>
    internal SurgewaveSourceOffsetStore GetOrCreateEosOffsetStore()
    {
        return _eosOffsetStore ??= new SurgewaveSourceOffsetStore(
            _surgewaveClient, _config.OffsetsTopic, _logger);
    }

    /// <summary>
    /// Gets the exactly-once source offsets for a connector.
    /// Returns null if EOS offset store is not available.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, Dictionary<string, string>>?> GetSourceOffsetsAsync(
        string connectorName, CancellationToken ct = default)
    {
        var store = GetOrCreateEosOffsetStore();
        if (store == null) return null;
        await store.LoadAsync(ct);
        return await store.GetAllOffsetsAsync(connectorName, ct);
    }

    /// <summary>
    /// Deletes all exactly-once source offsets for a connector (for reprocessing).
    /// </summary>
    public async Task DeleteSourceOffsetsAsync(string connectorName, CancellationToken ct = default)
    {
        var store = GetOrCreateEosOffsetStore();
        if (store == null) return;
        await store.LoadAsync(ct);
        await store.DeleteOffsetsAsync(connectorName, ct);
    }

    /// <summary>
    /// Resets exactly-once source offsets for a connector to specific values.
    /// </summary>
    public async Task ResetSourceOffsetsAsync(
        string connectorName,
        Dictionary<string, Dictionary<string, string>> offsets,
        CancellationToken ct = default)
    {
        var store = GetOrCreateEosOffsetStore();
        if (store == null) return;
        await store.LoadAsync(ct);

        // Delete existing offsets
        await store.DeleteOffsetsAsync(connectorName, ct);

        // Write new offsets
        foreach (var (partition, offset) in offsets)
        {
            await store.CommitOffsetAsync(connectorName, partition, offset, ct);
        }
    }

    /// <summary>
    /// Gets the schema registry operations from the underlying Surgewave client, if available.
    /// </summary>
    internal Kuestenlogik.Surgewave.Client.Native.Operations.Schema.ISchemaRegistryOperations? SchemaRegistryOperations =>
        _surgewaveClient?.NativeClient?.Schema;

    /// <summary>
    /// Start the Connect worker.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting Surgewave Connect worker");
        _logger.LogInformation("Bootstrap servers: {Servers}", _config.BootstrapServers);

        // Load persisted connector configs from config topic
        await LoadPersistedConnectorsAsync(cancellationToken);

        _logger.LogInformation("Surgewave Connect worker started");
    }

    /// <summary>
    /// Load connector configurations persisted in the config topic.
    /// </summary>
    private async Task LoadPersistedConnectorsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var connectorConfigs = await LoadPersistedConnectorsViaSurgewaveClientAsync(cancellationToken);

            // Now start all the connectors we found
            _logger.LogInformation("Loading {Count} persisted connector(s)", connectorConfigs.Count);

            foreach (var (name, config) in connectorConfigs)
            {
                if (!config.TryGetValue("connector.class", out var connectorClass))
                {
                    _logger.LogWarning("Skipping connector '{Name}': missing connector.class", name);
                    continue;
                }

                try
                {
                    await CreateConnectorAsync(name, connectorClass, config);
                    _logger.LogInformation("Restored connector: {Name}", name);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to restore connector: {Name}", name);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load persisted connectors, starting fresh");
        }
    }

    private async Task<Dictionary<string, Dictionary<string, string>>> LoadPersistedConnectorsViaSurgewaveClientAsync(CancellationToken cancellationToken)
    {
        var connectorConfigs = new Dictionary<string, Dictionary<string, string>>();

        await using var consumer = _surgewaveClient.CreateConsumer<string, string>(opts =>
        {
            opts.GroupId = $"{_config.GroupId}-config-loader-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = Client.Consumer.AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        consumer.Subscribe(_config.ConfigTopic);

        var startTime = DateTime.UtcNow;
        var timeout = TimeSpan.FromSeconds(5);

        while (!cancellationToken.IsCancellationRequested && DateTime.UtcNow - startTime < timeout)
        {
            var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(1), cancellationToken);
            if (result == null)
            {
                break;
            }

            var connectorName = result.Key;
            if (result.Value == null)
            {
                connectorConfigs.Remove(connectorName ?? "");
                _logger.LogDebug("Found tombstone for connector: {Name}", connectorName);
            }
            else if (connectorName != null)
            {
                try
                {
                    var config = JsonSerializer.Deserialize<Dictionary<string, string>>(result.Value);
                    if (config != null)
                    {
                        connectorConfigs[connectorName] = config;
                        _logger.LogDebug("Found config for connector: {Name}", connectorName);
                    }
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Invalid config JSON for connector: {Name}", connectorName);
                }
            }
        }

        return connectorConfigs;
    }

    /// <summary>
    /// Persist connector configuration to the config topic.
    /// </summary>
    private async Task PersistConnectorConfigAsync(string name, IDictionary<string, string> config)
    {
        try
        {
            var value = JsonSerializer.Serialize(config);

            await using var producer = _surgewaveClient.CreateProducer<string, string>();
            await producer.ProduceAsync(_config.ConfigTopic, name, value);

            _logger.LogDebug("Persisted config for connector: {Name}", name);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist config for connector: {Name}", name);
        }
    }

    /// <summary>
    /// Delete connector configuration from the config topic (tombstone).
    /// </summary>
    private async Task DeleteConnectorConfigAsync(string name)
    {
        try
        {
            await using var producer = _surgewaveClient.CreateProducer<string, string?>();
            await producer.ProduceAsync(_config.ConfigTopic, name, null);

            _logger.LogDebug("Deleted config for connector: {Name}", name);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete config for connector: {Name}", name);
        }
    }

    /// <summary>
    /// Stop the Connect worker.
    /// </summary>
    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping Surgewave Connect worker");

        await _cts.CancelAsync();

        // Stop all connectors
        foreach (var kvp in _connectors)
        {
            await StopConnectorAsync(kvp.Key);
        }

        if (_ownsClient)
            await _surgewaveClient.DisposeAsync();

        _logger.LogInformation("Surgewave Connect worker stopped");
    }

    /// <summary>
    /// Create and start a new connector.
    /// </summary>
    public async Task<string> CreateConnectorAsync(string name, string connectorClass, IDictionary<string, string> config)
    {
        if (_connectors.ContainsKey(name))
        {
            throw new InvalidOperationException($"Connector '{name}' already exists");
        }

        _logger.LogInformation("Creating connector: {Name} ({Class})", name, connectorClass);

        // Load connector type using type resolver if available, otherwise fall back to Type.GetType
        Type? connectorType = null;
        if (_typeResolver != null)
        {
            connectorType = _typeResolver(connectorClass);
        }

        connectorType ??= Type.GetType(connectorClass);

        if (connectorType == null)
        {
            throw new ArgumentException($"Connector class not found: {connectorClass}");
        }

        if (!typeof(IConnector).IsAssignableFrom(connectorType))
        {
            throw new ArgumentException($"Class {connectorClass} does not implement IConnector");
        }

        // Create connector instance
        var connector = (IConnector)Activator.CreateInstance(connectorType)!;

        // Initialize connector
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => RequestTaskReconfiguration(name),
            RaiseError = ex => HandleConnectorError(name, ex),
            Logger = _logger
        };
        connector.Initialize(context);

        // Merge config with name
        var fullConfig = new Dictionary<string, string>(config)
        {
            ["name"] = name,
            ["connector.class"] = connectorClass
        };

        // Start connector
        connector.Start(fullConfig);

        // Get task configs
        var maxTasks = config.TryGetValue("tasks.max", out var maxTasksStr)
            ? int.Parse(maxTasksStr)
            : 1;
        var taskConfigs = connector.TaskConfigs(maxTasks);

        // Create connector state
        var state = new ConnectorState
        {
            Name = name,
            Connector = connector,
            Config = fullConfig,
            Status = ConnectorStatus.Running,
            TaskCount = taskConfigs.Count
        };
        _connectors[name] = state;

        // Start tasks (use EOS runners if enabled)
        var useEos = ShouldUseEos(fullConfig);
        var eosConnector = connector as ExactlyOnceSourceConnector;
        var runners = new List<ITaskRunner>();
        for (int i = 0; i < taskConfigs.Count; i++)
        {
            var runner = await CreateTaskRunnerAsync(name, connector.TaskClass, taskConfigs[i], i, useEos, eosConnector);
            runners.Add(runner);
        }
        _taskRunners[name] = runners;

        _logger.LogInformation("Connector {Name} started with {TaskCount} tasks", name, taskConfigs.Count);

        // Persist connector config
        await PersistConnectorConfigAsync(name, fullConfig);

        return name;
    }

    /// <summary>
    /// Pause a connector (stops tasks but keeps connector registered).
    /// </summary>
    public async Task PauseConnectorAsync(string name)
    {
        if (!_connectors.TryGetValue(name, out var state))
        {
            throw new InvalidOperationException($"Connector '{name}' not found");
        }

        if (state.Status == ConnectorStatus.Paused)
        {
            _logger.LogDebug("Connector {Name} is already paused", name);
            return;
        }

        _logger.LogInformation("Pausing connector: {Name}", name);

        // Pause all tasks
        if (_taskRunners.TryGetValue(name, out var runners))
        {
            foreach (var runner in runners)
            {
                await runner.PauseAsync();
            }
        }

        state.Status = ConnectorStatus.Paused;
        _logger.LogInformation("Connector {Name} paused", name);
    }

    /// <summary>
    /// Resume a paused connector.
    /// </summary>
    public async Task ResumeConnectorAsync(string name)
    {
        if (!_connectors.TryGetValue(name, out var state))
        {
            throw new InvalidOperationException($"Connector '{name}' not found");
        }

        if (state.Status != ConnectorStatus.Paused)
        {
            _logger.LogDebug("Connector {Name} is not paused (state: {State})", name, state.Status);
            return;
        }

        _logger.LogInformation("Resuming connector: {Name}", name);

        // Resume all tasks
        if (_taskRunners.TryGetValue(name, out var runners))
        {
            foreach (var runner in runners)
            {
                await runner.ResumeAsync();
            }
        }

        state.Status = ConnectorStatus.Running;
        _logger.LogInformation("Connector {Name} resumed", name);
    }

    /// <summary>
    /// Restart a connector (stop and start with same config).
    /// </summary>
    public async Task RestartConnectorAsync(string name)
    {
        if (!_connectors.TryGetValue(name, out var state))
        {
            throw new InvalidOperationException($"Connector '{name}' not found");
        }

        _logger.LogInformation("Restarting connector: {Name}", name);

        // Save config before stopping
        var config = new Dictionary<string, string>(state.Config);
        var connectorClass = config["connector.class"];

        // Stop the connector
        await StopConnectorAsync(name);

        // Remove the class and name from config (they'll be re-added)
        config.Remove("connector.class");
        config.Remove("name");

        // Recreate the connector
        await CreateConnectorAsync(name, connectorClass, config);

        _logger.LogInformation("Connector {Name} restarted", name);
    }

    /// <summary>
    /// Restart a specific task within a connector.
    /// </summary>
    public async Task RestartTaskAsync(string connectorName, int taskId)
    {
        if (!_connectors.TryGetValue(connectorName, out var state))
        {
            throw new InvalidOperationException($"Connector '{connectorName}' not found");
        }

        if (!_taskRunners.TryGetValue(connectorName, out var runners))
        {
            throw new InvalidOperationException($"No tasks found for connector '{connectorName}'");
        }

        var runner = runners.FirstOrDefault(r => r.TaskId == taskId)
            ?? throw new InvalidOperationException($"Task {taskId} not found in connector '{connectorName}'");

        _logger.LogInformation("Restarting task {Connector}/{TaskId}", connectorName, taskId);

        // Get task config
        var maxTasks = state.Config.TryGetValue("tasks.max", out var maxTasksStr)
            ? int.Parse(maxTasksStr)
            : 1;
        var taskConfigs = state.Connector.TaskConfigs(maxTasks);

        if (taskId >= taskConfigs.Count)
        {
            throw new InvalidOperationException($"Task {taskId} configuration not available");
        }

        // Stop and restart the task (use EOS if enabled)
        await runner.StopAsync();
        var index = runners.IndexOf(runner);
        var useEos = ShouldUseEos(state.Config);
        var eosConnector = state.Connector as ExactlyOnceSourceConnector;
        var newRunner = await CreateTaskRunnerAsync(connectorName, state.Connector.TaskClass, taskConfigs[taskId], taskId, useEos, eosConnector);
        runners[index] = newRunner;

        _logger.LogInformation("Task {Connector}/{TaskId} restarted", connectorName, taskId);
    }

    /// <summary>
    /// Stop and remove a connector.
    /// </summary>
    public async Task StopConnectorAsync(string name)
    {
        if (!_connectors.TryRemove(name, out var state))
        {
            return;
        }

        _logger.LogInformation("Stopping connector: {Name}", name);

        // Stop tasks
        if (_taskRunners.TryRemove(name, out var runners))
        {
            foreach (var runner in runners)
            {
                await runner.StopAsync();
                runner.Dispose();
            }
        }

        // Stop connector
        state.Connector.Stop();
        state.Connector.Dispose();

        // Delete persisted config
        await DeleteConnectorConfigAsync(name);

        _logger.LogInformation("Connector {Name} stopped", name);
    }

    /// <summary>
    /// Update the config for a running connector's tasks.
    /// </summary>
    public Task UpdateConnectorConfigAsync(string name, Dictionary<string, string> config)
    {
        if (!_connectors.TryGetValue(name, out var state)) return Task.CompletedTask;
        foreach (var (key, value) in config)
        {
            state.Config[key] = value;
        }
        _logger.LogInformation("Connector {Name} config updated ({Count} keys)", name, config.Count);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Get the status of a connector.
    /// </summary>
    public ConnectorInfo? GetConnectorStatus(string name)
    {
        if (!_connectors.TryGetValue(name, out var state))
        {
            return null;
        }

        var taskStatuses = new List<TaskStatus>();
        if (_taskRunners.TryGetValue(name, out var runners))
        {
            taskStatuses.AddRange(runners.Select(r => new TaskStatus
            {
                Id = r.TaskId,
                State = r.State.ToString(),
                WorkerId = Environment.MachineName
            }));
        }

        return new ConnectorInfo
        {
            Name = name,
            Type = state.Connector is SourceConnector ? "source" : "sink",
            State = state.Status.ToString(),
            WorkerId = Environment.MachineName,
            Config = state.Config,
            Tasks = taskStatuses
        };
    }

    /// <summary>
    /// List all connectors.
    /// </summary>
    public IReadOnlyList<string> ListConnectors()
    {
        return _connectors.Keys.ToList();
    }

    private async Task<ITaskRunner> CreateTaskRunnerAsync(
        string connectorName,
        Type taskClass,
        IDictionary<string, string> taskConfig,
        int taskId,
        bool useEos = false,
        ExactlyOnceSourceConnector? eosConnector = null)
    {
        var task = (IConnectorTask)Activator.CreateInstance(taskClass)!;

        ITaskRunner runner;

        // Check for exactly-once source connector pipeline
        if (task is ExactlyOnceSourceTask eosTask && eosConnector != null
            && eosConnector.ExactlyOnceEnabled)
        {
            var offsetStore = GetOrCreateEosOffsetStore();
            runner = new ExactlyOnceSourcePipeline(
                connectorName,
                taskId,
                eosTask,
                taskConfig,
                eosConnector.ExactlyOnceConfig,
                _surgewaveClient,
                offsetStore,
                _services,
                _logger);
            _logger.LogInformation(
                "Created exactly-once source pipeline for {Connector}/{TaskId}", connectorName, taskId);
        }
        else if (useEos)
        {
            // Use transactional runners for exactly-once semantics
            if (task is SourceTask sourceTask)
            {
                runner = new TransactionalSourceTaskRunner(
                    connectorName,
                    taskId,
                    sourceTask,
                    taskConfig,
                    _config,
                    _logger,
                    _surgewaveClient, _services);
                _logger.LogInformation("Created EOS source task runner for {Connector}/{TaskId}", connectorName, taskId);
            }
            else if (task is SinkTask sinkTask)
            {
                runner = new TransactionalSinkTaskRunner(
                    connectorName,
                    taskId,
                    sinkTask,
                    taskConfig,
                    _config,
                    _logger,
                    _surgewaveClient, _services);
                _logger.LogInformation("Created EOS sink task runner for {Connector}/{TaskId}", connectorName, taskId);
            }
            else
            {
                _logger.LogWarning("EOS not supported for task type {Type}, falling back to at-least-once", task.GetType().Name);
                runner = new TaskRunner(
                    connectorName,
                    taskId,
                    task,
                    taskConfig,
                    _config,
                    _logger,
                    _services,
                    surgewaveClient: _surgewaveClient);
            }
        }
        else
        {
            // Use standard at-least-once runner
            runner = new TaskRunner(
                connectorName,
                taskId,
                task,
                taskConfig,
                _config,
                _logger,
                _services,
                    surgewaveClient: _surgewaveClient);
        }

        await runner.StartAsync(_cts.Token);
        return runner;
    }

    /// <summary>
    /// Determines if exactly-once semantics should be used for a connector.
    /// </summary>
    private bool ShouldUseEos(IDictionary<string, string> config)
    {
        // Check per-connector override first
        if (config.TryGetValue("exactly.once", out var eosStr))
        {
            if (bool.TryParse(eosStr, out var eos))
            {
                return eos;
            }
        }

        // Fall back to worker-level setting
        return _config.ExactlyOnceSupport;
    }

    private async void RequestTaskReconfiguration(string connectorName)
    {
        _logger.LogInformation("Task reconfiguration requested for connector: {Name}", connectorName);

        if (!_connectors.TryGetValue(connectorName, out var state))
        {
            _logger.LogWarning("Cannot reconfigure tasks: connector '{Name}' not found", connectorName);
            return;
        }

        try
        {
            // Get new task configs from connector
            var maxTasks = state.Config.TryGetValue("tasks.max", out var maxTasksStr)
                ? int.Parse(maxTasksStr)
                : 1;
            var newTaskConfigs = state.Connector.TaskConfigs(maxTasks);

            // Stop existing tasks
            if (_taskRunners.TryRemove(connectorName, out var oldRunners))
            {
                foreach (var runner in oldRunners)
                {
                    _logger.LogInformation("Stopping task {Connector}/{TaskId} for reconfiguration", connectorName, runner.TaskId);
                    await runner.StopAsync();
                    runner.Dispose();
                }
            }

            // Start new tasks (use EOS if enabled)
            var useEos = ShouldUseEos(state.Config);
            var eosConnector = state.Connector as ExactlyOnceSourceConnector;
            var newRunners = new List<ITaskRunner>();
            for (int i = 0; i < newTaskConfigs.Count; i++)
            {
                var runner = await CreateTaskRunnerAsync(connectorName, state.Connector.TaskClass, newTaskConfigs[i], i, useEos, eosConnector);
                newRunners.Add(runner);
            }
            _taskRunners[connectorName] = newRunners;

            state.TaskCount = newTaskConfigs.Count;
            _logger.LogInformation("Task reconfiguration complete for connector {Name}: {TaskCount} tasks", connectorName, newTaskConfigs.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to reconfigure tasks for connector: {Name}", connectorName);
            if (_connectors.TryGetValue(connectorName, out var s))
            {
                s.Status = ConnectorStatus.Failed;
                s.Error = $"Task reconfiguration failed: {ex.Message}";
            }
        }
    }

    private void HandleConnectorError(string connectorName, Exception ex)
    {
        _logger.LogError(ex, "Connector error: {Name}", connectorName);
        if (_connectors.TryGetValue(connectorName, out var state))
        {
            state.Status = ConnectorStatus.Failed;
            state.Error = ex.Message;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts.Dispose();
    }
}

/// <summary>
/// Internal state for a connector.
/// </summary>
internal sealed class ConnectorState
{
    public required string Name { get; init; }
    public required IConnector Connector { get; init; }
    public required IDictionary<string, string> Config { get; init; }
    public ConnectorStatus Status { get; set; }
    public int TaskCount { get; set; }
    public string? Error { get; set; }
}

/// <summary>
/// Runs a single connector task with at-least-once delivery semantics.
/// Supports both Kafka protocol (via Confluent.Kafka) and Surgewave Native protocol (via ISurgewaveClient).
/// </summary>
internal sealed class TaskRunner : ITaskRunner
{
    private bool _disposed;
    private readonly string _connectorName;
    private readonly IConnectorTask _task;
    private readonly IDictionary<string, string> _config;
    private readonly ConnectWorkerConfig _workerConfig;
    private readonly ILogger _logger;
    private readonly ConnectDlqHandler? _dlqHandler;
    private readonly ISurgewaveClient _surgewaveClient;
    private readonly ConnectWorkerServices _services;
    private Task? _runLoop;
    private CancellationTokenSource? _cts;
    private volatile bool _isPaused;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public int TaskId { get; }
    public TaskRunnerState State { get; private set; } = TaskRunnerState.Unassigned;

    public TaskRunner(
        string connectorName,
        int taskId,
        IConnectorTask task,
        IDictionary<string, string> config,
        ConnectWorkerConfig workerConfig,
        ILogger logger,
        ConnectWorkerServices services,
        ISurgewaveClient surgewaveClient,
        ConnectDlqHandler? dlqHandler = null)
    {
        _connectorName = connectorName;
        TaskId = taskId;
        _task = task;
        _config = config;
        _workerConfig = workerConfig;
        _logger = logger;
        _services = services;
        _dlqHandler = dlqHandler;
        _surgewaveClient = surgewaveClient;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var context = new TaskContext
        {
            RaiseError = HandleError,
            SchemaRegistry = _services.SchemaRegistry ?? _surgewaveClient.NativeClient?.Schema,
            MetricsCollector = _services.MetricsCollector,
            Debugger = _services.Debugger
        };
        _task.Initialize(context);
        _task.Start(_config);

        State = TaskRunnerState.Running;

        if (_task is SourceTask sourceTask)
            _runLoop = RunSourceTaskViaSurgewaveClientAsync(sourceTask, _cts.Token);
        else if (_task is SinkTask sinkTask)
            _runLoop = RunSinkTaskViaSurgewaveClientAsync(sinkTask, _cts.Token);

        var protocol = _surgewaveClient.Protocol.ToString();
        _logger.LogInformation("Task {Connector}/{TaskId} started ({Protocol})", _connectorName, TaskId, protocol);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping task {Connector}/{TaskId}", _connectorName, TaskId);

        if (_cts != null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }

        if (_runLoop != null)
        {
            try
            {
                await _runLoop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _task.Stop();
        _task.Dispose();
        State = TaskRunnerState.Unassigned;

        _logger.LogInformation("Task {Connector}/{TaskId} stopped", _connectorName, TaskId);
    }

    public async Task PauseAsync()
    {
        if (_isPaused)
        {
            return;
        }

        _logger.LogInformation("Pausing task {Connector}/{TaskId}", _connectorName, TaskId);

        // Acquire the semaphore to block the run loop
        await _pauseSemaphore.WaitAsync();
        _isPaused = true;
        State = TaskRunnerState.Paused;

        _logger.LogInformation("Task {Connector}/{TaskId} paused", _connectorName, TaskId);
    }

    public Task ResumeAsync()
    {
        if (!_isPaused)
        {
            return Task.CompletedTask;
        }

        _logger.LogInformation("Resuming task {Connector}/{TaskId}", _connectorName, TaskId);

        _isPaused = false;
        State = TaskRunnerState.Running;
        _pauseSemaphore.Release();

        _logger.LogInformation("Task {Connector}/{TaskId} resumed", _connectorName, TaskId);
        return Task.CompletedTask;
    }

    private async Task CheckPauseAsync(CancellationToken cancellationToken)
    {
        if (_isPaused)
        {
            // Wait until resumed - acquire and immediately release
            await _pauseSemaphore.WaitAsync(cancellationToken);
            _pauseSemaphore.Release();
        }
    }



    private async Task RunSourceTaskViaSurgewaveClientAsync(SourceTask task, CancellationToken cancellationToken)
    {
        await using var producer = _surgewaveClient.CreateProducer<byte[]?, byte[]>();

        var maxRetries = _dlqHandler?.Config.MaxRetries ?? _workerConfig.DlqMaxRetries;
        var retryBackoffMs = _dlqHandler?.Config.RetryBackoffMs ?? _workerConfig.DlqRetryBackoffMs;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CheckPauseAsync(cancellationToken);

                var records = await task.PollAsync(cancellationToken);

                if (records == null || records.Count == 0)
                {
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                foreach (var record in records)
                {
                    var attemptCount = 0;
                    Exception? lastException = null;
                    ProduceResult? result = null;

                    while (attemptCount <= maxRetries)
                    {
                        attemptCount++;
                        try
                        {
                            var headers = record.Headers != null
                                ? new Dictionary<string, byte[]>(record.Headers) as IReadOnlyDictionary<string, byte[]>
                                : null;
                            result = await producer.ProduceAsync(
                                record.Topic,
                                record.Key,
                                record.Value,
                                headers,
                                cancellationToken);
                            break;
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception ex) when (attemptCount < maxRetries)
                        {
                            lastException = ex;
                            _logger.LogWarning(ex,
                                "Source task {Connector}/{TaskId} failed produce attempt {Attempt}/{Max}",
                                _connectorName, TaskId, attemptCount, maxRetries);
                            await Task.Delay(retryBackoffMs, cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            lastException = ex;
                            break;
                        }
                    }

                    if (lastException != null)
                    {
                        if (_dlqHandler != null)
                        {
                            await _dlqHandler.HandleSourceRecordErrorAsync(
                                record, lastException, attemptCount, cancellationToken);
                        }
                        else
                        {
                            _logger.LogError(lastException,
                                "Source task {Connector}/{TaskId} failed after {Attempts} attempts",
                                _connectorName, TaskId, attemptCount);
                        }
                        continue;
                    }

                    if (result != null)
                    {
                        var metadata = new RecordMetadata
                        {
                            Topic = result.Topic,
                            Partition = result.Partition,
                            Offset = result.Offset,
                            Timestamp = result.Timestamp
                        };

                        task.CommitRecord(record, metadata);
                    }
                }

                await task.CommitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in source task {Connector}/{TaskId}", _connectorName, TaskId);
                State = TaskRunnerState.Failed;
                await Task.Delay(5000, cancellationToken);
            }
        }
    }

    private async Task RunSinkTaskViaSurgewaveClientAsync(SinkTask task, CancellationToken cancellationToken)
    {
        var topics = _config.TryGetValue("topics", out var topicsStr)
            ? topicsStr.Split(',').Select(t => t.Trim()).ToArray()
            : throw new InvalidOperationException("Sink connector must specify 'topics'");

        await using var consumer = _surgewaveClient.CreateConsumer<byte[]?, byte[]>(opts =>
        {
            opts.GroupId = $"{_workerConfig.GroupId}-{_connectorName}";
            opts.AutoOffsetReset = Client.Consumer.AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        consumer.Subscribe(topics);

        var maxRetries = _dlqHandler?.Config.MaxRetries ?? _workerConfig.DlqMaxRetries;
        var retryBackoffMs = _dlqHandler?.Config.RetryBackoffMs ?? _workerConfig.DlqRetryBackoffMs;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CheckPauseAsync(cancellationToken);

                var consumeResult = await consumer.ConsumeAsync(TimeSpan.FromMilliseconds(100), cancellationToken);
                if (consumeResult == null)
                {
                    continue;
                }

                var record = new SinkRecord
                {
                    Topic = consumeResult.Topic,
                    Partition = consumeResult.Partition,
                    Offset = consumeResult.Offset,
                    Key = consumeResult.Key,
                    Value = consumeResult.Value ?? [],
                    Timestamp = consumeResult.Timestamp,
                    Headers = consumeResult.Headers
                };

                var attemptCount = 0;
                Exception? lastException = null;

                while (attemptCount <= maxRetries)
                {
                    attemptCount++;
                    try
                    {
                        await task.PutAsync([record], cancellationToken);

                        var offsets = new Dictionary<TopicPartition, long>
                        {
                            [new TopicPartition(record.Topic, record.Partition)] = record.Offset + 1
                        };
                        await task.FlushAsync(offsets, cancellationToken);

                        await consumer.CommitAsync(cancellationToken);
                        break;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex) when (attemptCount < maxRetries)
                    {
                        lastException = ex;
                        _logger.LogWarning(ex,
                            "Sink task {Connector}/{TaskId} failed attempt {Attempt}/{Max}",
                            _connectorName, TaskId, attemptCount, maxRetries);
                        await Task.Delay(retryBackoffMs, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        break;
                    }
                }

                if (lastException != null)
                {
                    if (_dlqHandler != null)
                    {
                        await _dlqHandler.HandleSinkRecordErrorAsync(
                            record, lastException, attemptCount, cancellationToken);
                    }
                    else
                    {
                        _logger.LogError(lastException,
                            "Sink task {Connector}/{TaskId} failed after {Attempts} attempts",
                            _connectorName, TaskId, attemptCount);
                    }
                    await consumer.CommitAsync(cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in sink task {Connector}/{TaskId}", _connectorName, TaskId);
                State = TaskRunnerState.Failed;
                await Task.Delay(5000, cancellationToken);
            }
        }
    }

    private void HandleError(Exception ex)
    {
        _logger.LogError(ex, "Task error: {Connector}/{TaskId}", _connectorName, TaskId);
        State = TaskRunnerState.Failed;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _cts?.Dispose();
        _pauseSemaphore.Dispose();
    }
}
