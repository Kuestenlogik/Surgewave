using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Orchestrates the exactly-once source pipeline for a single task.
/// Uses cross-topic transactions to atomically produce messages and commit source offsets.
///
/// Core loop:
/// 1. Get last committed offset from offset store
/// 2. Call task.PollWithOffsetAsync(lastOffset) to get records
/// 3. Begin cross-topic transaction
/// 4. Produce all records to target topic(s) within the transaction
/// 5. Produce offset update to the offset topic within the same transaction
/// 6. Commit transaction (all-or-nothing)
/// 7. Update in-memory offset cache
///
/// On crash/restart, the offset store reflects the last successfully committed
/// transaction, so the task resumes without duplicates.
/// </summary>
public sealed class ExactlyOnceSourcePipeline : ITaskRunner
{
    private readonly string _connectorName;
    private readonly ExactlyOnceSourceTask _task;
    private readonly IDictionary<string, string> _taskConfig;
    private readonly ExactlyOnceConfig _eosConfig;
    private readonly ISurgewaveClient _surgewaveClient;
    private readonly ConnectWorkerServices _services;
    private readonly SurgewaveSourceOffsetStore _offsetStore;
    private readonly ILogger _logger;
    private Task? _runLoop;
    private CancellationTokenSource? _cts;
    private volatile bool _isPaused;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);
    private bool _disposed;

    public int TaskId { get; }
    public TaskRunnerState State { get; private set; } = TaskRunnerState.Unassigned;

    public ExactlyOnceSourcePipeline(
        string connectorName,
        int taskId,
        ExactlyOnceSourceTask task,
        IDictionary<string, string> taskConfig,
        ExactlyOnceConfig eosConfig,
        ISurgewaveClient surgewaveClient,
        SurgewaveSourceOffsetStore offsetStore,
        ConnectWorkerServices services, ILogger logger)
    {
        _connectorName = connectorName;
        TaskId = taskId;
        _task = task;
        _taskConfig = taskConfig;
        _eosConfig = eosConfig;
        _surgewaveClient = surgewaveClient;
        _services = services;
        _offsetStore = offsetStore;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Inject offset store and connector name into the task
        _task.OffsetStore = _offsetStore;
        _task.ConnectorName = _connectorName;
        _task.SourcePartition = _taskConfig.TryGetValue("source.partition", out var sp) ? sp : $"task-{TaskId}";

        var context = new TaskContext
        {
            RaiseError = HandleError,
            SchemaRegistry = _services.SchemaRegistry,
            MetricsCollector = _services.MetricsCollector,
            Debugger = _services.Debugger
        };
        _task.Initialize(context);
        _task.Start(_taskConfig);

        // Load offset cache
        await _offsetStore.LoadAsync(cancellationToken);

        State = TaskRunnerState.Running;
        _runLoop = RunPipelineAsync(_cts.Token);

        _logger.LogInformation(
            "Exactly-once source pipeline {Connector}/{TaskId} started (partition: {Partition})",
            _connectorName, TaskId, _task.SourcePartition);
    }

    public async Task StopAsync()
    {
        _logger.LogInformation(
            "Stopping exactly-once source pipeline {Connector}/{TaskId}", _connectorName, TaskId);

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

        _logger.LogInformation(
            "Exactly-once source pipeline {Connector}/{TaskId} stopped", _connectorName, TaskId);
    }

    public async Task PauseAsync()
    {
        if (_isPaused) return;

        _logger.LogInformation(
            "Pausing exactly-once source pipeline {Connector}/{TaskId}", _connectorName, TaskId);
        await _pauseSemaphore.WaitAsync();
        _isPaused = true;
        State = TaskRunnerState.Paused;
    }

    public Task ResumeAsync()
    {
        if (!_isPaused) return Task.CompletedTask;

        _logger.LogInformation(
            "Resuming exactly-once source pipeline {Connector}/{TaskId}", _connectorName, TaskId);
        _isPaused = false;
        State = TaskRunnerState.Running;
        _pauseSemaphore.Release();
        return Task.CompletedTask;
    }

    private async Task RunPipelineAsync(CancellationToken ct)
    {
        var nativeClient = _surgewaveClient.NativeClient;
        if (nativeClient == null)
        {
            _logger.LogError(
                "Exactly-once source pipeline requires Surgewave native client. " +
                "Ensure the client is connected and using Surgewave protocol.");
            State = TaskRunnerState.Failed;
            return;
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await CheckPauseAsync(ct);

                // Step 1: Get last committed offset
                var lastOffset = await _offsetStore.GetOffsetAsync(
                    _connectorName, _task.SourcePartition, ct);

                // Step 2: Poll for records with offset context
                var records = await _task.PollWithOffsetAsync(lastOffset, ct);
                if (records.Count == 0)
                {
                    await Task.Delay(_eosConfig.PollInterval, ct);
                    continue;
                }

                // Limit batch size
                var batch = records.Count > _eosConfig.MaxBatchSize
                    ? records.Take(_eosConfig.MaxBatchSize).ToList()
                    : records;

                // Step 3: Begin cross-topic transaction
                await using var tx = await nativeClient.CrossTopicTransactions
                    .BeginAsync(_eosConfig.TransactionTimeout, ct);

                // Step 4: Produce all records within the transaction (concurrently)
                var produceTasks = new Task[batch.Count];
                for (var i = 0; i < batch.Count; i++)
                {
                    var record = batch[i];
                    var partition = record.Partition ?? 0;
                    produceTasks[i] = tx.ProduceAsync(record.Topic, record.Key, record.Value, partition, ct);
                }
                await Task.WhenAll(produceTasks);

                // Step 5: Produce offset update within the same transaction
                // Use the offset from the last record in the batch
                var lastRecord = batch[^1];
                var offsetKey = $"{_connectorName}:{lastRecord.SourcePartition}";
                var offsetValue = JsonSerializer.Serialize(lastRecord.SourceOffset);
                var offsetKeyBytes = Encoding.UTF8.GetBytes(offsetKey);
                var offsetValueBytes = Encoding.UTF8.GetBytes(offsetValue);

                await tx.ProduceAsync(_eosConfig.OffsetTopic, offsetKeyBytes, offsetValueBytes, 0, ct);

                // Step 6: Commit the transaction (all-or-nothing)
                var commitResult = await tx.CommitAsync(ct);
                if (commitResult.ErrorCode != Protocol.Native.SurgewaveErrorCode.None)
                {
                    throw new InvalidOperationException(
                        $"Failed to commit exactly-once transaction: {commitResult.ErrorCode} - {commitResult.Error}");
                }

                // Step 7: Update cache
                _offsetStore.UpdateCache(_connectorName, lastRecord.SourcePartition, lastRecord.SourceOffset);

                _logger.LogDebug(
                    "Committed {Count} records atomically for {Connector}/{TaskId}",
                    batch.Count, _connectorName, TaskId);

                // Notify task of successful commit
                await _task.CommitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Error in exactly-once source pipeline {Connector}/{TaskId}, retrying...",
                    _connectorName, TaskId);
                State = TaskRunnerState.Failed;

                try
                {
                    await Task.Delay(5000, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                State = TaskRunnerState.Running;
            }
        }
    }

    private async Task CheckPauseAsync(CancellationToken ct)
    {
        if (_isPaused)
        {
            await _pauseSemaphore.WaitAsync(ct);
            _pauseSemaphore.Release();
        }
    }

    private void HandleError(Exception ex)
    {
        _logger.LogError(ex, "Task error in {Connector}/{TaskId}", _connectorName, TaskId);
        State = TaskRunnerState.Failed;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Dispose();
        _pauseSemaphore.Dispose();
    }
}
