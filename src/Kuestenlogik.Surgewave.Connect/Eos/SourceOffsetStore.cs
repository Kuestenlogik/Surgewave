using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Eos;

/// <summary>
/// Stores and retrieves source connector offsets from the _connect-offsets topic.
/// Used for exactly-once semantics to track source connector progress.
/// </summary>
public sealed class SourceOffsetStore : IOffsetStorageReader, IAsyncDisposable
{
    private readonly ISurgewaveClient _client;
    private readonly string _offsetsTopic;
    private readonly ILogger _logger;
    private readonly Dictionary<string, IDictionary<string, object>> _cachedOffsets = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private bool _loaded;
    private IProducer<string, string>? _producer;

    public SourceOffsetStore(
        ISurgewaveClient client,
        string offsetsTopic,
        ILogger logger)
    {
        _client = client;
        _offsetsTopic = offsetsTopic;
        _logger = logger;
    }

    /// <summary>
    /// Loads all stored offsets from the topic into cache.
    /// Should be called before reading offsets.
    /// </summary>
    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        await _loadLock.WaitAsync(cancellationToken);
        try
        {
            if (_loaded) return;

            _logger.LogInformation("Loading source offsets from {Topic}", _offsetsTopic);

            await using var consumer = _client.CreateConsumer<string, string>(opts =>
            {
                opts.GroupId = $"connect-offset-loader-{Guid.NewGuid():N}";
                opts.AutoOffsetReset = AutoOffsetReset.Earliest;
                opts.EnableAutoCommit = false;
            });

            consumer.Subscribe(_offsetsTopic);

            // Read all messages until end of topic
            var endReached = false;
            while (!endReached && !cancellationToken.IsCancellationRequested)
            {
                var result = await consumer.ConsumeAsync(TimeSpan.FromSeconds(2), cancellationToken);
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
                        _cachedOffsets.Remove(result.Key ?? "");
                    }
                    else
                    {
                        // Store offset
                        var offset = JsonSerializer.Deserialize<Dictionary<string, object>>(result.Value);
                        if (offset != null && result.Key != null)
                        {
                            _cachedOffsets[result.Key] = offset;
                        }
                    }
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Failed to deserialize offset for key {Key}", result.Key);
                }
            }

            _loaded = true;
            _logger.LogInformation("Loaded {Count} source offsets from {Topic}", _cachedOffsets.Count, _offsetsTopic);
        }
        finally
        {
            _loadLock.Release();
        }
    }

    /// <summary>
    /// Stores source offsets for a connector.
    /// </summary>
    /// <param name="connectorName">The connector name.</param>
    /// <param name="offsets">The source partition to offset mappings.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task StoreOffsetsAsync(
        string connectorName,
        IEnumerable<(IDictionary<string, object> Partition, IDictionary<string, object> Offset)> offsets,
        CancellationToken cancellationToken = default)
    {
        _producer ??= _client.CreateProducer<string, string>();

        foreach (var (partition, offset) in offsets)
        {
            var key = CreateKey(connectorName, partition);
            var value = JsonSerializer.Serialize(offset);

            await _producer.ProduceAsync(_offsetsTopic, key, value, cancellationToken: cancellationToken);

            // Update cache
            _cachedOffsets[key] = offset;
        }

        _logger.LogDebug("Stored offsets for connector {Connector}", connectorName);
    }

    /// <summary>
    /// Deletes stored offsets for a connector.
    /// </summary>
    public async Task DeleteOffsetsAsync(
        string connectorName,
        IEnumerable<IDictionary<string, object>> partitions,
        CancellationToken cancellationToken = default)
    {
        _producer ??= _client.CreateProducer<string, string>();

        foreach (var partition in partitions)
        {
            var key = CreateKey(connectorName, partition);

            // Send tombstone
            await _producer.ProduceAsync(_offsetsTopic, key, null!, cancellationToken: cancellationToken);

            // Update cache
            _cachedOffsets.Remove(key);
        }

        _logger.LogDebug("Deleted offsets for connector {Connector}", connectorName);
    }

    /// <summary>
    /// Gets the stored offset for a source partition.
    /// </summary>
    public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
    {
        // This is called from source tasks, so we need the connector name from context
        // For now, we'll use a simplified approach where partition includes connector info
        var key = SerializePartition(partition);
        return _cachedOffsets.TryGetValue(key, out var offset) ? offset : null;
    }

    /// <summary>
    /// Gets stored offsets for multiple source partitions.
    /// </summary>
    public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
        IReadOnlyCollection<IDictionary<string, object>> partitions)
    {
        var result = new Dictionary<IDictionary<string, object>, IDictionary<string, object>>();

        foreach (var partition in partitions)
        {
            var offset = Offset(partition);
            if (offset != null)
            {
                result[partition] = offset;
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the offset for a specific connector and partition.
    /// </summary>
    public IDictionary<string, object>? GetOffset(string connectorName, IDictionary<string, object> partition)
    {
        var key = CreateKey(connectorName, partition);
        return _cachedOffsets.TryGetValue(key, out var offset) ? offset : null;
    }

    private static string CreateKey(string connectorName, IDictionary<string, object> partition)
    {
        return $"{connectorName}:{SerializePartition(partition)}";
    }

    private static string SerializePartition(IDictionary<string, object> partition)
    {
        // Create a stable key from partition dictionary
        var sortedPairs = partition
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => $"{kvp.Key}={kvp.Value}");
        return string.Join(",", sortedPairs);
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
