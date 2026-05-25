using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// In-memory deduplication store using a concurrent dictionary.
/// Best for development/testing or when persistence isn't required.
/// Note: Deduplication state is lost on restart.
/// </summary>
public sealed class InMemoryDeduplicationStore : IDeduplicationStore
{
    private readonly ConcurrentDictionary<string, ProcessedEntry> _processedMessages = new();
    private readonly int _maxSize;
    private readonly object _cleanupLock = new();

    /// <summary>
    /// Creates an in-memory deduplication store.
    /// </summary>
    /// <param name="maxSize">Maximum number of entries to keep. When exceeded, oldest entries are removed.</param>
    public InMemoryDeduplicationStore(int maxSize = 100_000)
    {
        _maxSize = maxSize;
    }

    public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_processedMessages.ContainsKey(messageId));
    }

    public Task MarkProcessedAsync(string messageId, ProcessedMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        var entry = new ProcessedEntry(metadata?.ProcessedAt ?? DateTimeOffset.UtcNow);
        _processedMessages.TryAdd(messageId, entry);

        // Cleanup if over size limit
        if (_processedMessages.Count > _maxSize)
        {
            _ = Task.Run(() => EnforceMaxSize(), cancellationToken);
        }

        return Task.CompletedTask;
    }

    public Task MarkProcessedBatchAsync(IEnumerable<string> messageIds, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var entry = new ProcessedEntry(now);

        foreach (var messageId in messageIds)
        {
            _processedMessages.TryAdd(messageId, entry);
        }

        // Cleanup if over size limit
        if (_processedMessages.Count > _maxSize)
        {
            _ = Task.Run(() => EnforceMaxSize(), cancellationToken);
        }

        return Task.CompletedTask;
    }

    public Task<int> CleanupAsync(TimeSpan maxAge, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTimeOffset.UtcNow - maxAge;
        var removed = 0;

        foreach (var kvp in _processedMessages)
        {
            if (kvp.Value.ProcessedAt < cutoff)
            {
                if (_processedMessages.TryRemove(kvp.Key, out _))
                {
                    removed++;
                }
            }
        }

        return Task.FromResult(removed);
    }

    private void EnforceMaxSize()
    {
        lock (_cleanupLock)
        {
            if (_processedMessages.Count <= _maxSize)
                return;

            // Remove oldest entries (approximately 10% of max size)
            var toRemove = _maxSize / 10;
            var oldest = _processedMessages
                .OrderBy(kvp => kvp.Value.ProcessedAt)
                .Take(toRemove)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var key in oldest)
            {
                _processedMessages.TryRemove(key, out _);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _processedMessages.Clear();
        return ValueTask.CompletedTask;
    }

    private readonly record struct ProcessedEntry(DateTimeOffset ProcessedAt);
}
