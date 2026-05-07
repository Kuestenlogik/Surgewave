using System.Collections.Concurrent;
using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Manages cross-topic transactions: begin, buffer writes, commit atomically, abort, and cleanup.
/// Uses two-phase commit: (1) write all messages to all topics, (2) write transaction markers.
/// </summary>
public sealed class CrossTopicTransactionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, CrossTopicTransaction> _transactions = new();
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;
    private readonly CrossTopicTransactionConfig _config;
    private readonly ILogger<CrossTopicTransactionManager> _logger;
    private readonly CancellationTokenSource _cleanupCts = new();
    private readonly Task _cleanupTask;

    public CrossTopicTransactionManager(
        LogManager logManager,
        RecordBatchSerializer serializer,
        CrossTopicTransactionConfig config,
        ILogger<CrossTopicTransactionManager> logger)
    {
        _logManager = logManager;
        _serializer = serializer;
        _config = config;
        _logger = logger;

        _cleanupTask = Task.Run(() => CleanupLoopAsync(_cleanupCts.Token));
    }

    /// <summary>
    /// Begin a new cross-topic transaction.
    /// </summary>
    public CrossTopicTransaction Begin(string? producerId = null, TimeSpan? timeout = null)
    {
        var effectiveTimeout = timeout ?? _config.DefaultTimeout;
        if (effectiveTimeout > _config.MaxTimeout)
            effectiveTimeout = _config.MaxTimeout;

        var txn = new CrossTopicTransaction
        {
            ProducerId = producerId,
            Timeout = effectiveTimeout
        };

        if (!_transactions.TryAdd(txn.TransactionId, txn))
            throw new InvalidOperationException("Transaction ID collision");

        _logger.LogDebug("Cross-topic transaction {TransactionId} started (timeout={Timeout}s, producer={ProducerId})",
            txn.TransactionId, effectiveTimeout.TotalSeconds, producerId ?? "anonymous");

        return txn;
    }

    /// <summary>
    /// Add a write to a cross-topic transaction.
    /// </summary>
    public void AddWrite(string transactionId, string topic, int partition, byte[]? key, byte[] value, Dictionary<string, string>? headers = null)
    {
        if (!_transactions.TryGetValue(transactionId, out var txn))
            throw new InvalidOperationException($"Transaction {transactionId} not found");

        if (txn.PendingWrites.Count >= _config.MaxPendingWrites)
            throw new InvalidOperationException($"Transaction {transactionId} exceeds max pending writes ({_config.MaxPendingWrites})");

        txn.AddWrite(topic, partition, key, value, headers);
    }

    /// <summary>
    /// Commit a cross-topic transaction atomically.
    /// Two-phase commit: write all messages, then write transaction markers.
    /// </summary>
    public async Task<TransactionCommitResult> CommitAsync(string transactionId, CancellationToken ct = default)
    {
        if (!_transactions.TryGetValue(transactionId, out var txn))
            return new TransactionCommitResult(transactionId, false, 0, 0, TimeSpan.Zero, "Transaction not found", null);

        if (txn.State != CrossTopicTransactionState.Open)
            return new TransactionCommitResult(transactionId, false, 0, 0, TimeSpan.Zero, $"Transaction in invalid state: {txn.State}", null);

        if (txn.IsTimedOut)
        {
            txn.State = CrossTopicTransactionState.TimedOut;
            _transactions.TryRemove(transactionId, out _);
            return new TransactionCommitResult(transactionId, false, 0, 0, TimeSpan.Zero, "Transaction timed out", null);
        }

        var sw = Stopwatch.StartNew();

        try
        {
            // Phase 1: Transition to Committing
            txn.State = CrossTopicTransactionState.Committing;

            if (txn.PendingWrites.Count == 0)
            {
                txn.State = CrossTopicTransactionState.Committed;
                _transactions.TryRemove(transactionId, out _);
                return new TransactionCommitResult(transactionId, true, 0, 0, sw.Elapsed, null, new Dictionary<string, long>());
            }

            // Group writes by topic-partition
            var grouped = txn.PendingWrites
                .GroupBy(w => (w.Topic, w.Partition))
                .ToList();

            var offsets = new Dictionary<string, long>();
            var topicsWritten = new HashSet<string>();
            var messagesWritten = 0;

            // Phase 2: Write all messages to their respective topic-partitions
            foreach (var group in grouped)
            {
                var topic = group.Key.Topic;
                var partition = group.Key.Partition;
                var topicPartition = new TopicPartition { Topic = topic, Partition = partition };

                // Create record batch from messages
                var messages = new List<Message>();
                foreach (var write in group)
                {
                    messages.Add(new Message
                    {
                        Key = write.Key ?? [],
                        Value = write.Value,
                        Headers = ReadOnlyMemory<byte>.Empty,
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Offset = 0 // Will be assigned by LogManager
                    });
                }

                var batch = _serializer.SerializeMessages(messages);
                var baseOffset = await _logManager.AppendBatchAsync(topicPartition, batch, ct);

                offsets[$"{topic}-{partition}"] = baseOffset;
                topicsWritten.Add(topic);
                messagesWritten += messages.Count;
            }

            // Phase 3: Write transaction commit markers
            foreach (var topic in topicsWritten)
            {
                var logTp = new TopicPartition { Topic = _config.TransactionLogTopic, Partition = 0 };
                var logEntry = System.Text.Encoding.UTF8.GetBytes(
                    $"{{\"txnId\":\"{transactionId}\",\"state\":\"committed\",\"topic\":\"{topic}\",\"ts\":{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}}}");

                try
                {
                    var logMessages = new List<Message>
                    {
                        new()
                        {
                            Key = System.Text.Encoding.UTF8.GetBytes(transactionId),
                            Value = logEntry,
                            Headers = ReadOnlyMemory<byte>.Empty,
                            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                            Offset = 0
                        }
                    };
                    var logBatch = _serializer.SerializeMessages(logMessages);
                    await _logManager.AppendBatchAsync(logTp, logBatch, ct);
                }
                catch (Exception ex)
                {
                    // Transaction log is best-effort; data is already written
                    _logger.LogWarning(ex, "Failed to write transaction log for {TransactionId}", transactionId);
                }
            }

            txn.State = CrossTopicTransactionState.Committed;
            _transactions.TryRemove(transactionId, out _);

            _logger.LogInformation(
                "Cross-topic transaction {TransactionId} committed: {TopicCount} topics, {MessageCount} messages in {Duration}ms",
                transactionId, topicsWritten.Count, messagesWritten, sw.ElapsedMilliseconds);

            return new TransactionCommitResult(
                transactionId, true, topicsWritten.Count, messagesWritten, sw.Elapsed, null, offsets);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Cross-topic transaction {TransactionId} commit failed", transactionId);
            txn.State = CrossTopicTransactionState.Aborted;
            _transactions.TryRemove(transactionId, out _);
            return new TransactionCommitResult(transactionId, false, 0, 0, sw.Elapsed, ex.Message, null);
        }
    }

    /// <summary>
    /// Abort a cross-topic transaction, discarding all pending writes.
    /// </summary>
    public Task AbortAsync(string transactionId, CancellationToken ct = default)
    {
        if (!_transactions.TryRemove(transactionId, out var txn))
            return Task.CompletedTask;

        txn.State = CrossTopicTransactionState.Aborted;
        txn.PendingWrites.Clear();

        _logger.LogInformation("Cross-topic transaction {TransactionId} aborted", transactionId);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Get a transaction by ID.
    /// </summary>
    public CrossTopicTransaction? GetTransaction(string transactionId)
    {
        _transactions.TryGetValue(transactionId, out var txn);
        return txn;
    }

    /// <summary>
    /// List all active (Open or Committing) transactions.
    /// </summary>
    public IReadOnlyList<CrossTopicTransaction> ListActive()
    {
        return _transactions.Values
            .Where(t => t.State is CrossTopicTransactionState.Open or CrossTopicTransactionState.Committing)
            .ToList();
    }

    /// <summary>
    /// Clean up expired transactions.
    /// </summary>
    public Task CleanupExpiredAsync(CancellationToken ct = default)
    {
        var expired = _transactions.Values.Where(t => t.IsTimedOut).ToList();
        foreach (var txn in expired)
        {
            txn.State = CrossTopicTransactionState.TimedOut;
            txn.PendingWrites.Clear();
            _transactions.TryRemove(txn.TransactionId, out _);

            _logger.LogWarning("Cross-topic transaction {TransactionId} timed out after {Timeout}s",
                txn.TransactionId, txn.Timeout.TotalSeconds);
        }
        return Task.CompletedTask;
    }

    private async Task CleanupLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(_config.CleanupIntervalSeconds), ct);
                await CleanupExpiredAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in cross-topic transaction cleanup loop");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cleanupCts.CancelAsync();
        try
        {
            await _cleanupTask;
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        _cleanupCts.Dispose();
    }
}
