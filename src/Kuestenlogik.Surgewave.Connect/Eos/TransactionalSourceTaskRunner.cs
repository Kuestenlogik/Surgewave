using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Connect.Dlq;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Runs a source connector task with exactly-once semantics (EOS).
/// Uses transactional producers to ensure each message is produced exactly once.
/// </summary>
public sealed class TransactionalSourceTaskRunner : ITaskRunner
{
    private bool _disposed;
    private readonly string _connectorName;
    private readonly SourceTask _task;
    private readonly IDictionary<string, string> _config;
    private readonly ConnectWorkerConfig _workerConfig;
    private readonly ILogger _logger;
    private readonly ConnectDlqHandler? _dlqHandler;
    private readonly ISurgewaveClient _surgewaveClient;
    private readonly ConnectWorkerServices _services;
    private readonly string _transactionalId;
    private Task? _runLoop;
    private CancellationTokenSource? _cts;
    private volatile bool _isPaused;
    private readonly SemaphoreSlim _pauseSemaphore = new(1, 1);

    public int TaskId { get; }
    public TaskRunnerState State { get; private set; } = TaskRunnerState.Unassigned;

    public TransactionalSourceTaskRunner(
        string connectorName,
        int taskId,
        SourceTask task,
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

        // Generate unique transactional ID for this task
        _transactionalId = $"{workerConfig.TransactionIdPrefix}-{connectorName}-{taskId}";
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
        _runLoop = RunTransactionalSourceTaskAsync(_cts.Token);

        _logger.LogInformation(
            "Transactional source task {Connector}/{TaskId} started (TxnId: {TxnId})",
            _connectorName, TaskId, _transactionalId);
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _logger.LogInformation("Stopping transactional source task {Connector}/{TaskId}", _connectorName, TaskId);

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

        _logger.LogInformation("Transactional source task {Connector}/{TaskId} stopped", _connectorName, TaskId);
    }

    public async Task PauseAsync()
    {
        if (_isPaused) return;

        _logger.LogInformation("Pausing transactional source task {Connector}/{TaskId}", _connectorName, TaskId);
        await _pauseSemaphore.WaitAsync();
        _isPaused = true;
        State = TaskRunnerState.Paused;
        _logger.LogInformation("Transactional source task {Connector}/{TaskId} paused", _connectorName, TaskId);
    }

    public Task ResumeAsync()
    {
        if (!_isPaused) return Task.CompletedTask;

        _logger.LogInformation("Resuming transactional source task {Connector}/{TaskId}", _connectorName, TaskId);
        _isPaused = false;
        State = TaskRunnerState.Running;
        _pauseSemaphore.Release();
        _logger.LogInformation("Transactional source task {Connector}/{TaskId} resumed", _connectorName, TaskId);
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

    private async Task RunTransactionalSourceTaskAsync(CancellationToken cancellationToken)
    {
        // Get the native client for transaction support (exposed via ISurgewaveClient interface)
        var nativeClient = _surgewaveClient.NativeClient;
        if (nativeClient == null)
        {
            _logger.LogError(
                "Transactional source task requires Surgewave native client. " +
                "Ensure the client is connected and using Surgewave protocol (not Kafka protocol).");
            State = TaskRunnerState.Failed;
            return;
        }

        // Initialize the transactional producer
        TransactionBuilder? txnBuilder = null;
        try
        {
            txnBuilder = await nativeClient.Transactions
                .BeginTransaction(_transactionalId)
                .WithTimeout(TimeSpan.FromMilliseconds(_workerConfig.TransactionTimeoutMs))
                .InitAsync(cancellationToken);

            _logger.LogDebug("Initialized transactional producer for {TxnId}", _transactionalId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize transactional producer for {TxnId}", _transactionalId);
            State = TaskRunnerState.Failed;
            return;
        }

        await using var producer = _surgewaveClient.CreateProducer<byte[]?, byte[]>();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CheckPauseAsync(cancellationToken);

                var records = await _task.PollAsync(cancellationToken);
                if (records == null || records.Count == 0)
                {
                    await Task.Delay(100, cancellationToken);
                    continue;
                }

                // Collect topics and partitions for this batch
                var topicsToPartitions = new Dictionary<string, List<int>>();
                var recordsWithMetadata = new List<(SourceRecord Record, RecordMetadata? Metadata)>();

                // Begin transaction for this batch
                // Note: We re-initialize for each batch to get fresh producer ID if needed
                // In production, you might want to reuse the transaction builder
                txnBuilder = await nativeClient.Transactions
                    .BeginTransaction(_transactionalId)
                    .WithTimeout(TimeSpan.FromMilliseconds(_workerConfig.TransactionTimeoutMs))
                    .InitAsync(cancellationToken);

                try
                {
                    // Produce all records
                    foreach (var record in records)
                    {
                        var headers = record.Headers != null
                            ? new Dictionary<string, byte[]>(record.Headers) as IReadOnlyDictionary<string, byte[]>
                            : null;

                        var result = await producer.ProduceAsync(
                            record.Topic,
                            record.Key,
                            record.Value,
                            headers,
                            cancellationToken);

                        // Track partition for AddPartitionsToTxn
                        if (!topicsToPartitions.TryGetValue(result.Topic, out var partitions))
                        {
                            partitions = [];
                            topicsToPartitions[result.Topic] = partitions;
                        }
                        if (!partitions.Contains(result.Partition))
                        {
                            partitions.Add(result.Partition);
                        }

                        var metadata = new RecordMetadata
                        {
                            Topic = result.Topic,
                            Partition = result.Partition,
                            Offset = result.Offset,
                            Timestamp = result.Timestamp
                        };
                        recordsWithMetadata.Add((record, metadata));
                    }

                    // Add partitions to transaction
                    if (topicsToPartitions.Count > 0)
                    {
                        var addResult = await txnBuilder.AddPartitionsAsync(topicsToPartitions, cancellationToken);

                        // Check for errors
                        foreach (var (topic, partitionResults) in addResult)
                        {
                            foreach (var partResult in partitionResults)
                            {
                                if (partResult.ErrorCode != SurgewaveErrorCode.None)
                                {
                                    throw new InvalidOperationException(
                                        $"Failed to add partition {topic}-{partResult.Partition} to transaction: {partResult.ErrorCode}");
                                }
                            }
                        }
                    }

                    // Commit the transaction
                    var commitResult = await txnBuilder.CommitAsync(cancellationToken);
                    if (commitResult != SurgewaveErrorCode.None)
                    {
                        throw new InvalidOperationException($"Failed to commit transaction: {commitResult}");
                    }

                    _logger.LogDebug(
                        "Committed transaction for {Count} records in {Connector}/{TaskId}",
                        records.Count, _connectorName, TaskId);

                    // Notify task of successful commits
                    foreach (var (record, metadata) in recordsWithMetadata)
                    {
                        if (metadata != null)
                        {
                            _task.CommitRecord(record, metadata);
                        }
                    }

                    await _task.CommitAsync(cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex,
                        "Transaction failed for {Connector}/{TaskId}, aborting",
                        _connectorName, TaskId);

                    try
                    {
                        var abortResult = await txnBuilder.AbortAsync(cancellationToken);
                        if (abortResult != SurgewaveErrorCode.None)
                        {
                            _logger.LogWarning(
                                "Failed to abort transaction for {TxnId}: {Error}",
                                _transactionalId, abortResult);
                        }
                    }
                    catch (Exception abortEx)
                    {
                        _logger.LogWarning(abortEx, "Error aborting transaction for {TxnId}", _transactionalId);
                    }

                    // Route to DLQ if enabled
                    if (_dlqHandler != null)
                    {
                        foreach (var record in records)
                        {
                            await _dlqHandler.HandleSourceRecordErrorAsync(record, ex, 1, cancellationToken);
                        }
                    }

                    // Delay before retry
                    await Task.Delay(_workerConfig.DlqRetryBackoffMs, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in transactional source task {Connector}/{TaskId}", _connectorName, TaskId);
                State = TaskRunnerState.Failed;
                await Task.Delay(5000, cancellationToken);
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
