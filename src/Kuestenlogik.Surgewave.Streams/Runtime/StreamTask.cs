using System.Diagnostics;
using Kuestenlogik.Surgewave.Streams.Changelog;
using Kuestenlogik.Surgewave.Streams.Dlq;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Resilience;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Represents a task that processes records for a set of partitions.
/// Each StreamTask owns a subset of partitions and their associated state stores.
/// </summary>
internal sealed class StreamTask : IDisposable
{
    private readonly TaskId _taskId;
    private readonly Topology _topology;
    private readonly ProcessorContext _context;
    private readonly ILogger _logger;
    private readonly StreamsProducer? _producer;
    private readonly Dictionary<string, IStateStore> _localStores = new();
    private readonly Dictionary<TopicPartition, long> _currentOffsets = new();
    private readonly RateLimiter? _recordRateLimiter;
    private readonly RateLimiter? _byteRateLimiter;
    private readonly DeadLetterQueueWriter? _dlqWriter;
    private readonly StreamsRetryPolicy? _retryPolicy;
    private readonly Dictionary<string, ProcessorNode> _sourceByTopic = new();
    private bool _commitNeeded;
    private long _lastCommitTime;
    private bool _disposed;
    private bool _transactionInProgress;
    private CheckpointManager? _checkpointManager;

    public TaskId TaskId => _taskId;
    public StreamTaskState State { get; private set; } = StreamTaskState.Created;
    public IReadOnlyCollection<TopicPartition> Partitions => _taskId.Partitions;
    public IReadOnlyDictionary<TopicPartition, long> CurrentOffsets => _currentOffsets;
    private bool IsEosEnabled => _context.Config.ProcessingGuarantee == ProcessingGuarantee.ExactlyOnce;

    public StreamTask(
        TaskId taskId,
        Topology topology,
        ProcessorContext context,
        ILogger logger,
        StreamsProducer? producer = null)
    {
        _taskId = taskId;
        _topology = topology;
        _context = context;
        _logger = logger;
        _producer = producer;
        _lastCommitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Initialize rate limiters from config
        if (context.Config.MaxRecordsPerSecond > 0)
            _recordRateLimiter = new RateLimiter(context.Config.MaxRecordsPerSecond);
        if (context.Config.MaxBytesPerSecond > 0)
            _byteRateLimiter = new RateLimiter(context.Config.MaxBytesPerSecond);

        // Initialize DLQ writer if configured
        if (context.Config.DeadLetterQueue.Enabled && producer != null)
            _dlqWriter = new DeadLetterQueueWriter(producer, context.Config);

        // Initialize retry policy if configured
        if (context.Config.Retry.Enabled)
            _retryPolicy = new StreamsRetryPolicy(context.Config.Retry);
    }

    /// <summary>
    /// Initializes the task and its state stores.
    /// </summary>
    public void Initialize()
    {
        if (State != StreamTaskState.Created)
            throw new InvalidOperationException($"Cannot initialize task in state {State}");

        _logger.LogDebug("Initializing stream task {TaskId}", _taskId);

        // Create local state stores for this task's partitions
        foreach (var supplier in _topology.StateStoreSuppliers)
        {
            var getMethod = supplier.GetType().GetMethod("Get");
            if (getMethod != null)
            {
                var store = (IStateStore)getMethod.Invoke(supplier, null)!;
                _localStores[store.Name] = store;
                _context.RegisterStateStore(store);
            }
        }

        // Initialize all processor nodes and build source lookup cache
        foreach (var source in _topology.Sources)
        {
            InitializeNode(source);

            // Cache topic → source mapping (avoid reflection per-record)
            var topicProp = source.GetType().GetProperty("TopicPattern");
            if (topicProp?.GetValue(source)?.ToString() is string topicName)
            {
                _sourceByTopic[topicName] = source;
            }
        }

        // Initialize lifecycle hooks after state stores are ready
        var orchestrator = new ShutdownOrchestrator(_logger, _context.Config.ShutdownTimeout);
        orchestrator.InitializeLifecycles(_topology.Sources, _context);

        State = StreamTaskState.Running;
    }

    /// <summary>
    /// Initializes the task with changelog restoration.
    /// </summary>
    public async Task InitializeWithRestoreAsync(
        StreamsConsumer? restoreConsumer = null,
        CancellationToken cancellationToken = default)
    {
        Initialize();

        // Find all changelog-backed stores that need restoration
        var changelogStores = _localStores.Values
            .OfType<Changelog.IChangelogBacked>()
            .ToList();

        if (changelogStores.Count == 0)
        {
            _logger.LogDebug("No changelog-backed stores to restore for task {TaskId}", _taskId);
            return;
        }

        var restorer = new Changelog.ChangelogRestorer(
            restoreConsumer,
            _context.Config.StateRestoreListener,
            _logger);

        _logger.LogInformation("Restoring {Count} changelog-backed store(s) for task {TaskId}",
            changelogStores.Count, _taskId);

        foreach (var store in changelogStores)
        {
            await restorer.RestoreStoreAsync(store, cancellationToken);
        }

        _logger.LogInformation("Changelog restoration complete for task {TaskId}", _taskId);
    }

    private void InitializeNode(ProcessorNode node)
    {
        node.Init(_context);
        foreach (var child in node.Children)
        {
            InitializeNode(child);
        }
    }

    /// <summary>
    /// Processes a single record.
    /// </summary>
    public void Process(string topic, int partition, byte[] key, byte[] value, long timestamp, long offset)
    {
        if (State != StreamTaskState.Running)
            return;

        // Begin transaction on first record if EOS is enabled
        if (IsEosEnabled && !_transactionInProgress && _producer != null)
        {
            _producer.BeginTransaction();
            _transactionInProgress = true;
        }

        // Fast cached lookup (no reflection)
        if (!_sourceByTopic.TryGetValue(topic, out var sourceNode))
            return;

        _context.Topic = topic;
        _context.Partition = partition;
        _context.Offset = offset;
        _context.Timestamp = timestamp;

        // Rate limiting (global)
        if (_recordRateLimiter != null)
        {
            if (!_recordRateLimiter.TryConsume(1))
            {
                var waitMs = _recordRateLimiter.CalculateWaitTimeMs(1);
                if (waitMs > 0 && waitMs <= _context.Config.MaxRateLimitWaitMs)
                {
                    Thread.Sleep((int)waitMs);
                    _recordRateLimiter.Refill();
                    _recordRateLimiter.TryConsume(1);
                    _context.Metrics.RecordRateLimitThrottle((int)waitMs);
                }
            }
        }

        if (_byteRateLimiter != null)
        {
            var bytes = key.Length + value.Length;
            if (!_byteRateLimiter.TryConsume(bytes))
            {
                var waitMs = _byteRateLimiter.CalculateWaitTimeMs(bytes);
                if (waitMs > 0 && waitMs <= _context.Config.MaxRateLimitWaitMs)
                {
                    Thread.Sleep((int)waitMs);
                    _byteRateLimiter.Refill();
                    _byteRateLimiter.TryConsume(bytes);
                    _context.Metrics.RecordRateLimitThrottle((int)waitMs);
                }
            }
        }

        // Start OTEL activity for tracing
        using var activity = _context.Metrics.StartProcessRecordActivity(
            topic, partition, offset, _context.ApplicationId);
        _context.CurrentActivity = activity;
        var start = Stopwatch.GetTimestamp();

        try
        {
            if (_retryPolicy != null)
            {
                _retryPolicy.Execute(
                    () => sourceNode.Process(key, value, timestamp),
                    _context.Metrics);
            }
            else
            {
                sourceNode.Process(key, value, timestamp);
            }

            _commitNeeded = true;

            // Track offset for EOS commit (reuse struct, no separate allocation)
            _currentOffsets[new TopicPartition(topic, partition)] = offset + 1;

            _context.Metrics.RecordProcessed(key.Length + value.Length);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            var response = _context.Config.ProcessingExceptionHandler.Handle(
                topic, partition, offset, key, value, ex);

            if (response == ProcessingHandlerResponse.Fail)
            {
                if (IsEosEnabled && _transactionInProgress && _producer != null)
                {
                    _producer.AbortTransaction();
                    _transactionInProgress = false;
                }
                throw new ProcessingException(topic, partition, offset, "Processing failed", ex);
            }

            // Skip: write to DLQ if enabled
            if (_dlqWriter != null)
            {
                _dlqWriter.Write(topic, partition, offset, key, value, timestamp, ex);
                _context.Metrics.RecordDlqMessage();
            }

            _context.Metrics.RecordProcessingError();
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(start);
            _context.Metrics.RecordProcessingLatency(elapsed.TotalMilliseconds);
            _context.CurrentActivity = null;
        }
    }

    /// <summary>
    /// Sets the checkpoint manager for periodic state store snapshots.
    /// </summary>
    public void SetCheckpointManager(CheckpointManager manager)
    {
        _checkpointManager = manager;
    }

    /// <summary>
    /// Performs punctuation for time-based operations.
    /// </summary>
    public void MaybePunctuate(long currentTime)
    {
        _context.MaybeFireStreamTimePunctuations(currentTime);
        _context.MaybeFireWallClockTimePunctuations();
        _context.CleanupCancelledPunctuations();

        // Periodic checkpointing
        _checkpointManager?.MaybeCheckpoint(_localStores.Values);
    }

    /// <summary>
    /// Commits the task if needed.
    /// </summary>
    /// <returns>True if a commit was performed.</returns>
    public bool MaybeCommit(long commitIntervalMs)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (!_commitNeeded || now - _lastCommitTime < commitIntervalMs)
            return false;

        Commit();
        return true;
    }

    /// <summary>
    /// Forces a commit.
    /// </summary>
    public void Commit()
    {
        if (!_commitNeeded)
            return;

        _logger.LogDebug("Committing stream task {TaskId}", _taskId);

        // Flush state stores
        foreach (var store in _localStores.Values)
        {
            store.Flush();
        }

        if (IsEosEnabled && _transactionInProgress && _producer != null)
        {
            try
            {
                // Send consumer offsets as part of the transaction
                if (_currentOffsets.Count > 0)
                {
                    _producer.SendOffsetsToTransaction(
                        _currentOffsets,
                        _context.Config.ApplicationId);
                }

                // Commit the transaction atomically
                _producer.CommitTransaction();
                _transactionInProgress = false;

                _logger.LogDebug("EOS transaction committed for task {TaskId}", _taskId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to commit EOS transaction for task {TaskId}", _taskId);
                _producer.AbortTransaction();
                _transactionInProgress = false;
                throw;
            }
        }
        else
        {
            _context.Commit();
        }

        _currentOffsets.Clear();
        _commitNeeded = false;
        _lastCommitTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    /// <summary>
    /// Suspends the task.
    /// </summary>
    public void Suspend()
    {
        if (State == StreamTaskState.Running)
        {
            _logger.LogDebug("Suspending stream task {TaskId}", _taskId);
            Commit();
            State = StreamTaskState.Suspended;
        }
    }

    /// <summary>
    /// Resumes the task.
    /// </summary>
    public void Resume()
    {
        if (State == StreamTaskState.Suspended)
        {
            _logger.LogDebug("Resuming stream task {TaskId}", _taskId);
            State = StreamTaskState.Running;
        }
    }

    /// <summary>
    /// Closes the task.
    /// </summary>
    public void Close()
    {
        if (State == StreamTaskState.Closed)
            return;

        _logger.LogDebug("Closing stream task {TaskId}", _taskId);

        // Abort any in-progress transaction before closing
        if (IsEosEnabled && _transactionInProgress && _producer != null)
        {
            try
            {
                _producer.AbortTransaction();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to abort transaction during close");
            }
            _transactionInProgress = false;
        }

        Commit();

        // Use ShutdownOrchestrator for reverse-topological shutdown with lifecycle hooks
        var orchestrator = new ShutdownOrchestrator(_logger, _context.Config.ShutdownTimeout);
        orchestrator.Shutdown(_topology.Sources, _context);

        // Close state stores
        foreach (var store in _localStores.Values)
        {
            store.Close();
        }

        _localStores.Clear();
        _currentOffsets.Clear();
        State = StreamTaskState.Closed;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Close();
        _disposed = true;
    }
}

/// <summary>
/// Unique identifier for a stream task.
/// </summary>
internal readonly record struct TaskId(int SubtopologyId, int Partition)
{
    public IReadOnlyCollection<TopicPartition> Partitions { get; init; } = [];

    public override string ToString() => $"{SubtopologyId}_{Partition}";
}

/// <summary>
/// Represents a topic-partition pair.
/// </summary>
public readonly record struct TopicPartition(string Topic, int Partition);

/// <summary>
/// State of a stream task.
/// </summary>
internal enum StreamTaskState
{
    Created,
    Running,
    Suspended,
    Closed
}
