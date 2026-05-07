using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Deduplication store backed by a Surgewave/Kafka topic for persistence.
/// State is restored on startup by consuming the entire topic.
/// </summary>
public sealed class TopicBackedDeduplicationStore : IDeduplicationStore
{
    private readonly ISurgewaveClient _client;
    private readonly string _topic;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, ProcessedEntry> _processedMessages = new();
    private IProducer<string, byte[]>? _producer;
    private bool _initialized;
    private bool _disposed;

    /// <summary>
    /// Creates a topic-backed deduplication store.
    /// </summary>
    /// <param name="client">The Surgewave client.</param>
    /// <param name="topic">The topic to use for state storage (e.g., "_connect-dedup-{connector}").</param>
    /// <param name="logger">Logger instance.</param>
    public TopicBackedDeduplicationStore(ISurgewaveClient client, string topic, ILogger logger)
    {
        _client = client;
        _topic = topic;
        _logger = logger;
    }

    /// <summary>
    /// Initializes the store by consuming existing state from the topic.
    /// Must be called before using other methods.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized) return;

        _logger.LogInformation("Initializing deduplication store from topic {Topic}", _topic);

        // Create producer for writing state
        _producer = _client.CreateProducer<string, byte[]>();

        // Consume existing state
        await using var consumer = _client.CreateConsumer<string, byte[]>(opts =>
        {
            opts.GroupId = $"_dedup-restore-{Guid.NewGuid():N}";
            opts.AutoOffsetReset = Client.Consumer.AutoOffsetReset.Earliest;
            opts.EnableAutoCommit = false;
        });

        consumer.Subscribe([_topic]);

        var emptyPollCount = 0;
        const int maxEmptyPolls = 3;

        while (emptyPollCount < maxEmptyPolls && !cancellationToken.IsCancellationRequested)
        {
            var result = await consumer.ConsumeAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
            if (result == null)
            {
                emptyPollCount++;
                continue;
            }

            emptyPollCount = 0;

            if (result.Key == null) continue;

            // Null value = tombstone (deleted)
            if (result.Value == null || result.Value.Length == 0)
            {
                _processedMessages.TryRemove(result.Key, out _);
            }
            else
            {
                try
                {
                    var entry = JsonSerializer.Deserialize<ProcessedEntry>(result.Value);
                    _processedMessages[result.Key] = entry;
                }
                catch
                {
                    // Treat as simple processed marker
                    _processedMessages[result.Key] = new ProcessedEntry(DateTimeOffset.UtcNow);
                }
            }
        }

        _logger.LogInformation("Restored {Count} deduplication entries from topic {Topic}", _processedMessages.Count, _topic);
        _initialized = true;
    }

    public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        return Task.FromResult(_processedMessages.ContainsKey(messageId));
    }

    public async Task MarkProcessedAsync(string messageId, ProcessedMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var entry = new ProcessedEntry(metadata?.ProcessedAt ?? DateTimeOffset.UtcNow);
        _processedMessages[messageId] = entry;

        // Persist to topic
        var value = JsonSerializer.SerializeToUtf8Bytes(entry);
        await _producer!.ProduceAsync(_topic, messageId, value, cancellationToken: cancellationToken);
    }

    public async Task MarkProcessedBatchAsync(IEnumerable<string> messageIds, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var now = DateTimeOffset.UtcNow;
        var entry = new ProcessedEntry(now);
        var value = JsonSerializer.SerializeToUtf8Bytes(entry);

        var tasks = new List<Task>();

        foreach (var messageId in messageIds)
        {
            _processedMessages[messageId] = entry;
            tasks.Add(_producer!.ProduceAsync(_topic, messageId, value, cancellationToken: cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    public async Task<int> CleanupAsync(TimeSpan maxAge, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var cutoff = DateTimeOffset.UtcNow - maxAge;
        var removed = 0;

        var toRemove = _processedMessages
            .Where(kvp => kvp.Value.ProcessedAt < cutoff)
            .Select(kvp => kvp.Key)
            .ToList();

        var tasks = new List<Task>();

        foreach (var key in toRemove)
        {
            if (_processedMessages.TryRemove(key, out _))
            {
                removed++;
                // Write tombstone to topic
                tasks.Add(_producer!.ProduceAsync(_topic, key, [], cancellationToken: cancellationToken));
            }
        }

        await Task.WhenAll(tasks);

        _logger.LogInformation("Cleaned up {Count} old deduplication entries", removed);
        return removed;
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
            throw new InvalidOperationException("Store not initialized. Call InitializeAsync first.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_producer != null)
        {
            await _producer.DisposeAsync();
        }

        _processedMessages.Clear();
    }

    private readonly record struct ProcessedEntry(DateTimeOffset ProcessedAt);
}
