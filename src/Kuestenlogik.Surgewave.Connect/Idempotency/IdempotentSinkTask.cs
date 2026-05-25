using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Base class for sink tasks that require idempotent processing.
/// Automatically filters out duplicate records using a deduplication store.
/// </summary>
public abstract class IdempotentSinkTask : SinkTask
{
    private IDeduplicationStore? _deduplicationStore;
    private Func<SinkRecord, string>? _messageIdExtractor;
    private ILogger _logger = NullLogger.Instance;
    private bool _initialized;

    /// <summary>
    /// Gets or sets the deduplication store to use.
    /// If not set, an in-memory store is used by default.
    /// </summary>
    protected IDeduplicationStore? DeduplicationStore
    {
        get => _deduplicationStore;
        set => _deduplicationStore = value;
    }

    /// <summary>
    /// Gets or sets the function used to extract a message ID from a record.
    /// If not set, defaults to using topic:partition:offset.
    /// </summary>
    protected Func<SinkRecord, string>? MessageIdExtractor
    {
        get => _messageIdExtractor;
        set => _messageIdExtractor = value;
    }

    /// <summary>
    /// Gets or sets the logger instance.
    /// </summary>
    protected ILogger Logger
    {
        get => _logger;
        set => _logger = value ?? NullLogger.Instance;
    }

    public override void Initialize(TaskContext context)
    {
        base.Initialize(context);
    }

    public override void Start(IDictionary<string, string> config)
    {
        // Initialize deduplication store if not provided
        _deduplicationStore ??= CreateDefaultDeduplicationStore(config);

        // Initialize message ID extractor if not provided
        _messageIdExtractor ??= MessageIdGenerator.FromRecord;

        _initialized = true;

        OnStart(config);
    }

    /// <summary>
    /// Override to perform custom initialization.
    /// </summary>
    protected abstract void OnStart(IDictionary<string, string> config);

    public sealed override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        EnsureInitialized();

        if (records.Count == 0)
            return;

        // Filter out duplicates
        var newRecords = new List<SinkRecord>();
        var processedIds = new List<string>();

        foreach (var record in records)
        {
            var messageId = _messageIdExtractor!(record);

            if (await _deduplicationStore!.IsProcessedAsync(messageId, cancellationToken))
            {
                _logger.LogDebug(
                    "Skipping duplicate record {Topic}/{Partition}/{Offset} (ID: {MessageId})",
                    record.Topic, record.Partition, record.Offset, messageId);
                continue;
            }

            newRecords.Add(record);
            processedIds.Add(messageId);
        }

        if (newRecords.Count == 0)
        {
            _logger.LogDebug("All {Count} records were duplicates, skipping batch", records.Count);
            return;
        }

        _logger.LogDebug(
            "Processing {NewCount} new records ({DuplicateCount} duplicates filtered)",
            newRecords.Count, records.Count - newRecords.Count);

        // Process the new records
        await PutNewRecordsAsync(newRecords, cancellationToken);

        // Mark records as processed
        await _deduplicationStore!.MarkProcessedBatchAsync(processedIds, cancellationToken);
    }

    /// <summary>
    /// Override to process new (non-duplicate) records.
    /// </summary>
    protected abstract Task PutNewRecordsAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken);

    public override void Stop()
    {
        OnStop();
    }

    /// <summary>
    /// Override to perform custom cleanup.
    /// </summary>
    protected virtual void OnStop()
    {
    }

    /// <summary>
    /// Creates the default deduplication store based on configuration.
    /// Override to provide a custom store.
    /// </summary>
    protected virtual IDeduplicationStore CreateDefaultDeduplicationStore(IDictionary<string, string> config)
    {
        // Check for max size configuration
        var maxSize = 100_000;
        if (config.TryGetValue("dedup.max.size", out var maxSizeStr) && int.TryParse(maxSizeStr, out var parsed))
        {
            maxSize = parsed;
        }

        return new InMemoryDeduplicationStore(maxSize);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _deduplicationStore?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        base.Dispose(disposing);
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
            throw new InvalidOperationException("Task not started. Call Start() first.");
    }
}
