using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Surgewave topic-backed implementation of <see cref="ISourceOffsetStore"/>.
/// Stores offsets in a compacted topic (__connect_offsets) for durability.
/// Key format: connectorName:sourcePartition
/// Value format: JSON serialized offset map.
/// Caches offsets in memory for fast lookup.
/// </summary>
public sealed class SurgewaveSourceOffsetStore : ISourceOffsetStore, IAsyncDisposable
{
    private readonly ISurgewaveClient _client;
    private readonly string _offsetsTopic;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, Dictionary<string, string>> _cache = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private bool _loaded;
    private IProducer<string, string?>? _producer;
    private readonly object _producerLock = new();

    public SurgewaveSourceOffsetStore(
        ISurgewaveClient client,
        string offsetsTopic,
        ILogger logger)
    {
        _client = client;
        _offsetsTopic = offsetsTopic;
        _logger = logger;
    }

    /// <summary>
    /// Loads all stored offsets from the compacted topic into cache.
    /// Must be called before reading offsets.
    /// </summary>
    public async Task LoadAsync(CancellationToken ct = default)
    {
        await _loadLock.WaitAsync(ct);
        try
        {
            if (_loaded) return;

            _logger.LogInformation("Loading source offsets from {Topic}", _offsetsTopic);

            await using var consumer = _client.CreateConsumer<string, string>(opts =>
            {
                opts.GroupId = $"connect-eos-offset-loader-{Guid.NewGuid():N}";
                opts.AutoOffsetReset = AutoOffsetReset.Earliest;
                opts.EnableAutoCommit = false;
            });

            consumer.Subscribe(_offsetsTopic);

            var endReached = false;
            while (!endReached && !ct.IsCancellationRequested)
            {
                var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), ct);
                if (result == null)
                {
                    endReached = true;
                    continue;
                }

                try
                {
                    if (result.Value == null)
                    {
                        // Tombstone - remove offset
                        _cache.TryRemove(result.Key ?? "", out _);
                    }
                    else if (result.Key != null)
                    {
                        var offset = JsonSerializer.Deserialize<Dictionary<string, string>>(result.Value);
                        if (offset != null)
                        {
                            _cache[result.Key] = offset;
                        }
                    }
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Failed to deserialize offset for key {Key}", result.Key);
                }
            }

            _loaded = true;
            _logger.LogInformation(
                "Loaded {Count} source offsets from {Topic}", _cache.Count, _offsetsTopic);
        }
        finally
        {
            _loadLock.Release();
        }
    }

    /// <inheritdoc />
    public Task<Dictionary<string, string>?> GetOffsetAsync(
        string connectorName,
        string sourcePartition,
        CancellationToken ct = default)
    {
        var key = CreateKey(connectorName, sourcePartition);
        var result = _cache.TryGetValue(key, out var offset) ? offset : null;
        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public async Task CommitOffsetAsync(
        string connectorName,
        string sourcePartition,
        Dictionary<string, string> offset,
        CancellationToken ct = default)
    {
        EnsureProducer();

        var key = CreateKey(connectorName, sourcePartition);
        var value = JsonSerializer.Serialize(offset);

        await _producer!.ProduceAsync(_offsetsTopic, key, value, cancellationToken: ct);

        // Update cache
        _cache[key] = new Dictionary<string, string>(offset);

        _logger.LogDebug(
            "Committed offset for {Connector}:{Partition}", connectorName, sourcePartition);
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, Dictionary<string, string>>> GetAllOffsetsAsync(
        string connectorName,
        CancellationToken ct = default)
    {
        var prefix = $"{connectorName}:";
        var result = new Dictionary<string, Dictionary<string, string>>();

        foreach (var (key, value) in _cache)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                var partition = key[prefix.Length..];
                result[partition] = new Dictionary<string, string>(value);
            }
        }

        return Task.FromResult<IReadOnlyDictionary<string, Dictionary<string, string>>>(result);
    }

    /// <inheritdoc />
    public async Task DeleteOffsetsAsync(
        string connectorName,
        CancellationToken ct = default)
    {
        EnsureProducer();

        var prefix = $"{connectorName}:";
        var keysToDelete = _cache.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList();

        foreach (var key in keysToDelete)
        {
            // Send tombstone
            await _producer!.ProduceAsync(_offsetsTopic, key, null, cancellationToken: ct);
            _cache.TryRemove(key, out _);
        }

        _logger.LogInformation(
            "Deleted {Count} offsets for connector {Connector}", keysToDelete.Count, connectorName);
    }

    private void EnsureProducer()
    {
        if (_producer is not null) return;
        lock (_producerLock)
        {
            _producer ??= _client.CreateProducer<string, string?>();
        }
    }

    /// <summary>
    /// Updates the in-memory offset cache after a successful transaction commit.
    /// Called by the pipeline after the cross-topic transaction has committed
    /// (offset was already written to the topic within the transaction).
    /// </summary>
    internal void UpdateCache(string connectorName, string sourcePartition, Dictionary<string, string> offset)
    {
        var key = CreateKey(connectorName, sourcePartition);
        _cache[key] = new Dictionary<string, string>(offset);
    }

    private static string CreateKey(string connectorName, string sourcePartition)
    {
        return $"{connectorName}:{sourcePartition}";
    }

    public async ValueTask DisposeAsync()
    {
        if (_producer != null)
        {
            await _producer.DisposeAsync();
        }
        _loadLock.Dispose();
    }
}
