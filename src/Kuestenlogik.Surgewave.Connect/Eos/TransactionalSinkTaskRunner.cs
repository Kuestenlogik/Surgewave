using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Connect.Dlq;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Runs a sink connector task with exactly-once semantics (EOS).
/// Uses READ_COMMITTED consumers and transactional offset commits.
/// </summary>
public sealed class TransactionalSinkTaskRunner : ITaskRunner
{
    private bool _disposed;
    private readonly string _connectorName;
    private readonly SinkTask _task;
    private readonly IDictionary<string, string> _config;
    private readonly ConnectWorkerConfig _workerConfig;
    private readonly ILogger _logger;
    private readonly ConnectDlqHandler? _dlqHandler;
    private readonly ISurgewaveClient _surgewaveClient;
    private readonly ConnectWorkerServices _services;
    private readonly string _transactionalId;
    private readonly string _consumerGroupId;
    private Task? _runLoop;
    private CancellationTokenSource? _cts;
    private volatile bool _isPaused;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public int TaskId { get; }
    public TaskRunnerState State { get; private set; } = TaskRunnerState.Unassigned;

    public TransactionalSinkTaskRunner(
        string connectorName,
        int taskId,
        SinkTask task,
        IDictionary<string, string> config,
        ConnectWorkerConfig workerConfig,
        ILogger logger,
        ISurgewaveClient surgewaveClient,
        ConnectWorkerServices services, ConnectDlqHandler? dlqHandler = null)
    {
        _connectorName = connectorName;
        TaskId = taskId;
        _task = task;
        _config = config;
        _workerConfig = workerConfig;
        _logger = logger;
        _surgewaveClient = surgewaveClient;
        _services = services;
        _dlqHandler = dlqHandler;

        // Generate unique transactional ID and consumer group for this task
        _transactionalId = $"{workerConfig.TransactionIdPrefix}-{connectorName}-sink-{taskId}";
        _consumerGroupId = $"{workerConfig.GroupId}-{connectorName}";
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var context = new TaskContext
        {
            RaiseError = HandleError,
            SchemaRegistry = _services.SchemaRegistry,
            MetricsCollector = _services.MetricsCollector,
            Debugger = _services.Debugger
        };
        _task.Initialize(context);
        _task.Start(_config);

        State = TaskRunnerState.Running;
        _runLoop = RunTransactionalSinkTaskAsync(_cts.Token);

        _logger.LogInformation(
            "Transactional sink task {Connector}/{TaskId} started (TxnId: {TxnId}, Group: {Group})",
            _connectorName, TaskId, _transactionalId, _consumerGroupId);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping transactional sink task {Connector}/{TaskId}", _connectorName, TaskId);

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

        _logger.LogInformation("Transactional sink task {Connector}/{TaskId} stopped", _connectorName, TaskId);
    }

    public async Task PauseAsync()
    {
        if (_isPaused) return;

        _logger.LogInformation("Pausing transactional sink task {Connector}/{TaskId}", _connectorName, TaskId);
        await _pauseSemaphore.WaitAsync();
        _isPaused = true;
        State = TaskRunnerState.Paused;
        _logger.LogInformation("Transactional sink task {Connector}/{TaskId} paused", _connectorName, TaskId);
    }

    public Task ResumeAsync()
    {
        if (!_isPaused) return Task.CompletedTask;

        _logger.LogInformation("Resuming transactional sink task {Connector}/{TaskId}", _connectorName, TaskId);
        _isPaused = false;
        State = TaskRunnerState.Running;
        _pauseSemaphore.Release();
        _logger.LogInformation("Transactional sink task {Connector}/{TaskId} resumed", _connectorName, TaskId);
        return Task.CompletedTask;
    }

    private async Task CheckPauseAsync(CancellationToken cancellationToken)
    {
        if (_isPaused)
        {
            await _pauseSemaphore.WaitAsync(cancellationToken);
            _pauseSemaphore.Release();
        }
    }

    private async Task RunTransactionalSinkTaskAsync(CancellationToken cancellationToken)
    {
        // Get the native client for transaction support (exposed via ISurgewaveClient interface)
        var nativeClient = _surgewaveClient.NativeClient;
        if (nativeClient == null)
        {
            _logger.LogError(
                "Transactional sink task requires Surgewave native client. " +
                "Ensure the client is connected and using Surgewave protocol (not Kafka protocol).");
            State = TaskRunnerState.Failed;
            return;
        }

        // Get topics from config
        var topics = _config.TryGetValue("topics", out var topicsStr)
            ? topicsStr.Split(',').Select(t => t.Trim()).ToArray()
            : throw new InvalidOperationException("Sink connector must specify 'topics'");

        // Create consumer with READ_COMMITTED isolation for exactly-once
        await using var consumer = _surgewaveClient.CreateConsumer<byte[]?, byte[]>(opts =>
        {
            opts.GroupId = _consumerGroupId;
            opts.AutoOffsetReset = AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
            opts.IsolationLevel = IsolationLevel.ReadCommitted;  // Key for EOS
        });

        consumer.Subscribe(topics);

        // Notify task of assigned partitions
        var assignedPartitions = topics
            .SelectMany(t => Enumerable.Range(0, 1).Select(p => new TopicPartition(t, p)))
            .ToList();
        _task.Open(assignedPartitions);

        var maxRetries = _dlqHandler?.Config.MaxRetries ?? _workerConfig.DlqMaxRetries;
        var retryBackoffMs = _dlqHandler?.Config.RetryBackoffMs ?? _workerConfig.DlqRetryBackoffMs;

        // Batch processing for better throughput
        var batchSize = 100;
        var batchRecords = new List<SinkRecord>();
        var batchOffsets = new Dictionary<TopicPartition, long>();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CheckPauseAsync(cancellationToken);

                var consumeResult = await consumer.ConsumeAsync(TimeSpan.FromMilliseconds(100), cancellationToken);
                if (consumeResult == null)
                {
                    // Process any pending batch on timeout
                    if (batchRecords.Count > 0)
                    {
                        await ProcessBatchAsync(nativeClient, batchRecords, batchOffsets, maxRetries, retryBackoffMs, cancellationToken);
                        batchRecords.Clear();
                        batchOffsets.Clear();
                    }
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

                batchRecords.Add(record);
                var tp = new TopicPartition(record.Topic, record.Partition);
                batchOffsets[tp] = record.Offset + 1;  // Next offset to consume

                // Process batch when full
                if (batchRecords.Count >= batchSize)
                {
                    await ProcessBatchAsync(nativeClient, batchRecords, batchOffsets, maxRetries, retryBackoffMs, cancellationToken);
                    batchRecords.Clear();
                    batchOffsets.Clear();
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in transactional sink task {Connector}/{TaskId}", _connectorName, TaskId);
                State = TaskRunnerState.Failed;
                await Task.Delay(5000, cancellationToken);
            }
        }

        // Close partitions
        _task.Close(assignedPartitions);
    }

    private async Task ProcessBatchAsync(
        SurgewaveNativeClient nativeClient,
        List<SinkRecord> records,
        Dictionary<TopicPartition, long> offsets,
        int maxRetries,
        int retryBackoffMs,
        CancellationToken cancellationToken)
    {
        var attemptCount = 0;
        Exception? lastException = null;

        while (attemptCount <= maxRetries)
        {
            attemptCount++;
            try
            {
                // Process records
                await _task.PutAsync(records, cancellationToken);
                await _task.FlushAsync(offsets, cancellationToken);

                // Initialize transaction for transactional offset commit
                var txnBuilder = await nativeClient.Transactions
                    .BeginTransaction(_transactionalId)
                    .WithTimeout(TimeSpan.FromMilliseconds(_workerConfig.TransactionTimeoutMs))
                    .InitAsync(cancellationToken);

                // Create offset committer and commit offsets within the transaction
                var offsetCommitter = new SinkOffsetCommitter(nativeClient, _consumerGroupId, _logger);
                await offsetCommitter.CommitAsync(txnBuilder, records, cancellationToken);

                // Commit the transaction (offsets will be committed atomically)
                var commitResult = await txnBuilder.CommitAsync(cancellationToken);
                if (commitResult != SurgewaveErrorCode.None)
                {
                    throw new InvalidOperationException($"Failed to commit offset transaction: {commitResult}");
                }

                _logger.LogDebug(
                    "Committed batch of {Count} records for {Connector}/{TaskId}",
                    records.Count, _connectorName, TaskId);

                return; // Success
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (attemptCount < maxRetries)
            {
                lastException = ex;
                _logger.LogWarning(ex,
                    "Sink task {Connector}/{TaskId} failed process attempt {Attempt}/{Max}",
                    _connectorName, TaskId, attemptCount, maxRetries);
                await Task.Delay(retryBackoffMs, cancellationToken);
            }
            catch (Exception ex)
            {
                lastException = ex;
                break;
            }
        }

        // All retries failed
        if (lastException != null)
        {
            if (_dlqHandler != null)
            {
                foreach (var record in records)
                {
                    await _dlqHandler.HandleSinkRecordErrorAsync(record, lastException, attemptCount, cancellationToken);
                }
            }
            else
            {
                _logger.LogError(lastException,
                    "Sink task {Connector}/{TaskId} failed after {Attempts} attempts (DLQ disabled)",
                    _connectorName, TaskId, attemptCount);
            }
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
