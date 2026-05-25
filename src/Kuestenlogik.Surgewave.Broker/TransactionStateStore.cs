using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Models;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Persists transaction state to disk for broker restart recovery.
/// Uses a JSON file per transactional producer for storage.
/// Supports log compaction to clean up old completed transaction state.
/// </summary>
public sealed class TransactionStateStore : IDisposable
{
    private readonly string _transactionsDirectory;
    private readonly ILogger<TransactionStateStore> _logger;
    private readonly Dictionary<string, PersistedTransactionState> _transactionStates = new();
    private readonly object _lock = new();
    private readonly Timer _compactionTimer;

    /// <summary>
    /// Retention period for completed transactions before compaction.
    /// Default: 7 days.
    /// </summary>
    public TimeSpan CompletedTransactionRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Interval for running compaction checks.
    /// Default: 1 hour.
    /// </summary>
    public TimeSpan CompactionCheckInterval { get; set; } = TimeSpan.FromHours(1);

    public TransactionStateStore(string dataDirectory, ILogger<TransactionStateStore> logger)
    {
        _transactionsDirectory = Path.Combine(dataDirectory, ".metadata", "transactions");
        _logger = logger;

        Directory.CreateDirectory(_transactionsDirectory);
        LoadAllTransactionStates();

        // Start compaction timer
        _compactionTimer = new Timer(
            callback: _ => CompactTransactionLog(),
            state: null,
            dueTime: CompactionCheckInterval,
            period: CompactionCheckInterval);
    }

    /// <summary>
    /// Persists transaction state after InitProducerId.
    /// </summary>
    public void SaveTransactionState(
        string transactionalId,
        long producerId,
        short producerEpoch,
        TransactionState state,
        int transactionTimeoutMs,
        IEnumerable<TopicPartition>? partitions = null,
        IEnumerable<string>? consumerGroups = null,
        IEnumerable<PendingOffsetEntry>? pendingOffsets = null)
    {
        lock (_lock)
        {
            var txnState = new PersistedTransactionState
            {
                TransactionalId = transactionalId,
                ProducerId = producerId,
                ProducerEpoch = producerEpoch,
                State = state.ToString(),
                TransactionTimeoutMs = transactionTimeoutMs,
                LastModified = DateTimeOffset.UtcNow,
                Partitions = partitions?.Select(p => new PersistedPartition
                {
                    Topic = p.Topic,
                    Partition = p.Partition
                }).ToList() ?? [],
                ConsumerGroups = consumerGroups?.ToList() ?? [],
                PendingOffsets = pendingOffsets?.ToList() ?? []
            };

            _transactionStates[transactionalId] = txnState;
            PersistTransaction(transactionalId, txnState);
        }
    }

    /// <summary>
    /// Gets all persisted transaction states for recovery.
    /// </summary>
    public IReadOnlyList<PersistedTransactionState> GetAllTransactionStates()
    {
        lock (_lock)
        {
            return _transactionStates.Values.ToList();
        }
    }

    /// <summary>
    /// Gets a specific transaction state by transactional ID.
    /// </summary>
    public PersistedTransactionState? GetTransactionState(string transactionalId)
    {
        lock (_lock)
        {
            return _transactionStates.GetValueOrDefault(transactionalId);
        }
    }

    /// <summary>
    /// Removes transaction state after completion.
    /// Only removes if transaction is in a completed state.
    /// </summary>
    public void RemoveTransactionState(string transactionalId)
    {
        lock (_lock)
        {
            if (_transactionStates.TryGetValue(transactionalId, out var state) &&
                (state.State == TransactionState.CompleteCommit.ToString() ||
                 state.State == TransactionState.CompleteAbort.ToString()))
            {
                // Keep the producer ID/epoch mapping but clear partitions
                state.Partitions.Clear();
                state.ConsumerGroups.Clear();
                state.PendingOffsets.Clear();
                state.LastModified = DateTimeOffset.UtcNow;
                PersistTransaction(transactionalId, state);
            }
        }
    }

    private void LoadAllTransactionStates()
    {
        if (!Directory.Exists(_transactionsDirectory))
        {
            return;
        }

        foreach (var file in Directory.GetFiles(_transactionsDirectory, "*.json"))
        {
            try
            {
                var json = File.ReadAllText(file);
                var txnState = JsonSerializer.Deserialize(json, BrokerJsonContext.Default.PersistedTransactionState);

                if (txnState != null && !string.IsNullOrEmpty(txnState.TransactionalId))
                {
                    _transactionStates[txnState.TransactionalId] = txnState;
                    _logger.LogDebug(
                        "Loaded transaction state for {TransactionalId}: State={State}, ProducerId={ProducerId}",
                        txnState.TransactionalId,
                        txnState.State,
                        txnState.ProducerId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load transaction state from {File}", file);
            }
        }

        _logger.LogInformation("Transaction state store loaded {Count} transactions", _transactionStates.Count);
    }

    private void PersistTransaction(string transactionalId, PersistedTransactionState state)
    {
        try
        {
            Directory.CreateDirectory(_transactionsDirectory);

            var fileName = SanitizeFileName(transactionalId) + ".json";
            var filePath = Path.Combine(_transactionsDirectory, fileName);
            var json = JsonSerializer.Serialize(state, BrokerJsonContext.Default.PersistedTransactionState);
            File.WriteAllText(filePath, json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist transaction state for {TransactionalId}", transactionalId);
        }
    }

    private static string SanitizeFileName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        return string.Join("_", name.Split(invalid, StringSplitOptions.RemoveEmptyEntries));
    }

    /// <summary>
    /// Compacts the transaction log by removing old completed transactions.
    /// </summary>
    public void CompactTransactionLog()
    {
        try
        {
            var cutoffTime = DateTimeOffset.UtcNow - CompletedTransactionRetention;
            var compactedCount = 0;

            lock (_lock)
            {
                var toRemove = new List<string>();

                foreach (var kvp in _transactionStates)
                {
                    var state = kvp.Value;

                    // Only compact completed transactions that are old enough
                    if ((state.State == TransactionState.CompleteCommit.ToString() ||
                         state.State == TransactionState.CompleteAbort.ToString() ||
                         state.State == TransactionState.Dead.ToString()) &&
                        state.LastModified < cutoffTime &&
                        state.Partitions.Count == 0)
                    {
                        toRemove.Add(kvp.Key);
                    }
                }

                foreach (var txnId in toRemove)
                {
                    _transactionStates.Remove(txnId);

                    // Delete the file
                    var fileName = SanitizeFileName(txnId) + ".json";
                    var filePath = Path.Combine(_transactionsDirectory, fileName);
                    try
                    {
                        if (File.Exists(filePath))
                        {
                            File.Delete(filePath);
                            compactedCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete compacted transaction file for {TransactionalId}", txnId);
                    }
                }
            }

            if (compactedCount > 0)
            {
                _logger.LogInformation(
                    "Transaction log compaction completed: removed {Count} old transactions older than {Retention}",
                    compactedCount,
                    CompletedTransactionRetention);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during transaction log compaction");
        }
    }

    /// <summary>
    /// Gets the count of active (non-completed) transactions.
    /// </summary>
    public int ActiveTransactionCount
    {
        get
        {
            lock (_lock)
            {
                return _transactionStates.Values.Count(t =>
                    t.State != TransactionState.CompleteCommit.ToString() &&
                    t.State != TransactionState.CompleteAbort.ToString() &&
                    t.State != TransactionState.Dead.ToString());
            }
        }
    }

    /// <summary>
    /// Gets the total count of all transaction states (for diagnostics).
    /// </summary>
    public int TotalTransactionCount
    {
        get
        {
            lock (_lock)
            {
                return _transactionStates.Count;
            }
        }
    }

    public void Dispose()
    {
        _compactionTimer.Dispose();

        // Ensure all states are persisted on shutdown
        lock (_lock)
        {
            foreach (var kvp in _transactionStates)
            {
                PersistTransaction(kvp.Key, kvp.Value);
            }
        }
    }
}

/// <summary>
/// Persisted transaction state for broker restart recovery.
/// </summary>
public sealed class PersistedTransactionState
{
    public string TransactionalId { get; set; } = string.Empty;
    public long ProducerId { get; set; }
    public short ProducerEpoch { get; set; }
    public string State { get; set; } = string.Empty;
    public int TransactionTimeoutMs { get; set; }
    public DateTimeOffset LastModified { get; set; }
    public List<PersistedPartition> Partitions { get; set; } = [];
    public List<string> ConsumerGroups { get; set; } = [];
    public List<PendingOffsetEntry> PendingOffsets { get; set; } = [];
}

/// <summary>
/// Persisted topic partition reference.
/// </summary>
public sealed class PersistedPartition
{
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
}

/// <summary>
/// Persisted pending offset for transactional offset commits.
/// </summary>
public sealed class PendingOffsetEntry
{
    public string GroupId { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string? Metadata { get; set; }
}
