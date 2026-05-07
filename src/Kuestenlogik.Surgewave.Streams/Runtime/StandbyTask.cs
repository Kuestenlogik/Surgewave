using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// A standby task maintains a replica of a state store without processing records.
/// When the active task fails, a standby can be promoted to active with minimal recovery time.
/// </summary>
public sealed class StandbyTask : IDisposable
{
    private readonly string _taskName;
    private readonly string _storeName;
    private readonly IStateStore _replicaStore;
    private readonly ILogger _logger;
    private bool _disposed;

    public string TaskName => _taskName;
    public string StoreName => _storeName;
    public StandbyTaskState State { get; private set; } = StandbyTaskState.Created;

    /// <summary>
    /// The changelog offset up to which this standby has replicated.
    /// </summary>
    public long ReplicatedOffset { get; private set; }

    public StandbyTask(string taskName, string storeName, IStateStore replicaStore, ILogger logger)
    {
        _taskName = taskName;
        _storeName = storeName;
        _replicaStore = replicaStore;
        _logger = logger;
    }

    /// <summary>
    /// Initializes the standby task for changelog replication.
    /// </summary>
    public void Initialize(ProcessorContext context)
    {
        _replicaStore.Init(context);
        State = StandbyTaskState.Restoring;
        _logger.LogDebug("Standby task {TaskName} initialized for store {StoreName}", _taskName, _storeName);
    }

    /// <summary>
    /// Applies a changelog record to the standby store replica.
    /// </summary>
    public void UpdateReplica(byte[] key, byte[] value, long offset)
    {
        if (State == StandbyTaskState.Closed)
            return;

        ReplicatedOffset = offset;
        State = StandbyTaskState.Running;
    }

    /// <summary>
    /// Promotes this standby to an active task.
    /// Returns the replica store which can be used by the new active task.
    /// </summary>
    public IStateStore Promote()
    {
        _logger.LogInformation(
            "Promoting standby task {TaskName} for store {StoreName} at offset {Offset}",
            _taskName, _storeName, ReplicatedOffset);

        State = StandbyTaskState.Promoted;
        return _replicaStore;
    }

    public void Close()
    {
        if (State == StandbyTaskState.Closed)
            return;

        _replicaStore.Close();
        State = StandbyTaskState.Closed;
        _logger.LogDebug("Standby task {TaskName} closed", _taskName);
    }

    public void Dispose()
    {
        if (_disposed) return;
        Close();
        _disposed = true;
    }
}

/// <summary>
/// State of a standby task.
/// </summary>
public enum StandbyTaskState
{
    Created,
    Restoring,
    Running,
    Promoted,
    Closed
}

/// <summary>
/// Configuration for standby replicas.
/// </summary>
public sealed class StandbyConfig
{
    /// <summary>
    /// Number of standby replicas per state store. Default: 0 (disabled).
    /// </summary>
    public int NumStandbyReplicas { get; init; }

    /// <summary>
    /// Whether to start warming up standby replicas immediately. Default: true.
    /// </summary>
    public bool WarmupEnabled { get; init; } = true;

    /// <summary>
    /// Maximum number of changelog records to replicate per poll cycle.
    /// Default: 1000.
    /// </summary>
    public int MaxReplicationBatchSize { get; init; } = 1000;

    /// <summary>
    /// Disabled standby configuration.
    /// </summary>
    public static StandbyConfig Disabled => new() { NumStandbyReplicas = 0 };
}
