using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Stores pipeline definitions in a Surgewave topic for persistence.
/// Uses Surgewave native client for high-performance direct communication.
/// </summary>
public sealed class PipelineStore : IAsyncDisposable
{
    private const string PipelinesTopic = "_surgewave-pipelines";

    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<PipelineStore> _logger;
    private readonly ConcurrentDictionary<string, PipelineDefinition> _pipelines = new();
    private SurgewaveNativeClient? _client;
    private bool _isLoaded;

    public PipelineStore(ConnectWorkerConfig config, ILogger<PipelineStore> logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Initialize the Surgewave native client connection.
    /// </summary>
    private async Task EnsureConnectedAsync(CancellationToken cancellationToken = default)
    {
        if (_client != null && _client.IsConnected)
        {
            return;
        }

        var (host, port) = ParseBootstrapServers(_config.BootstrapServers);
        _client = new SurgewaveNativeClient(host, port);
        await _client.ConnectAsync(cancellationToken);
    }

    /// <summary>
    /// Load all pipeline definitions from the topic.
    /// </summary>
    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        if (_isLoaded)
        {
            return;
        }

        try
        {
            await EnsureConnectedAsync(cancellationToken);

            // Ensure topic exists
            await EnsureTopicExistsAsync(cancellationToken);

            // Load all messages from the topic using native client
            long offset = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _client!.Messaging.ReceiveAsync(
                    PipelinesTopic,
                    partition: 0,
                    offset: offset,
                    maxBytes: 1024 * 1024,
                    maxWaitMs: 100, // Short wait for loading
                    cancellationToken: cancellationToken);

                if (result.Messages.Count == 0)
                {
                    break; // No more messages
                }

                foreach (var message in result.Messages)
                {
                    var pipelineId = message.Key != null ? Encoding.UTF8.GetString(message.Key) : null;
                    if (string.IsNullOrEmpty(pipelineId))
                    {
                        offset = message.Offset + 1;
                        continue;
                    }

                    if (message.Value == null || message.Value.Length == 0)
                    {
                        _pipelines.TryRemove(pipelineId, out _);
                        _logger.LogDebug("Found tombstone for pipeline: {Id}", pipelineId);
                    }
                    else
                    {
                        try
                        {
                            var json = Encoding.UTF8.GetString(message.Value);
                            var pipeline = JsonSerializer.Deserialize<PipelineDefinition>(json);
                            if (pipeline != null)
                            {
                                _pipelines[pipelineId] = pipeline;
                                _logger.LogDebug("Loaded pipeline: {Id}", pipelineId);
                            }
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogWarning(ex, "Invalid pipeline JSON for: {Id}", pipelineId);
                        }
                    }

                    offset = message.Offset + 1;
                }
            }

            _logger.LogInformation("Loaded {Count} pipeline(s) from store", _pipelines.Count);
            _isLoaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load pipelines, starting fresh");
            _isLoaded = true;
        }
    }

    private async Task EnsureTopicExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var topics = await _client!.Topics.ListAsync(cancellationToken);
            if (!topics.Any(t => t.Name == PipelinesTopic))
            {
                await _client.Topics.Create(PipelinesTopic)
                    .WithPartitions(1)
                    .WithReplicationFactor(1)
                    .WithConfig("cleanup.policy", "compact")
                    .ExecuteAsync(cancellationToken);
                _logger.LogInformation("Created pipeline store topic: {Topic}", PipelinesTopic);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Topic check/create failed (may already exist)");
        }
    }

    /// <summary>
    /// Get all pipeline definitions.
    /// </summary>
    public IReadOnlyList<PipelineDefinition> GetAll()
    {
        return _pipelines.Values.ToList();
    }

    /// <summary>
    /// Get a pipeline by ID.
    /// </summary>
    public PipelineDefinition? Get(string id)
    {
        return _pipelines.TryGetValue(id, out var pipeline) ? pipeline : null;
    }

    /// <summary>
    /// Save a pipeline definition.
    /// </summary>
    public async Task SaveAsync(PipelineDefinition pipeline, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);

        var key = Encoding.UTF8.GetBytes(pipeline.Id);
        var value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(pipeline));

        await _client!.Messaging.SendAsync(
            PipelinesTopic,
            partition: 0,
            key: key,
            value: value,
            cancellationToken: cancellationToken);

        _pipelines[pipeline.Id] = pipeline;
        _logger.LogInformation("Saved pipeline: {Id} ({Name})", pipeline.Id, pipeline.Name);
    }

    /// <summary>
    /// Delete a pipeline definition (tombstone).
    /// </summary>
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
        await EnsureConnectedAsync(cancellationToken);

        var key = Encoding.UTF8.GetBytes(id);

        // Send tombstone (empty value) for compacted topic
        await _client!.Messaging.SendAsync(
            PipelinesTopic,
            partition: 0,
            key: key,
            value: [], // Empty array as tombstone marker
            cancellationToken: cancellationToken);

        _pipelines.TryRemove(id, out _);
        _logger.LogInformation("Deleted pipeline: {Id}", id);
    }

    /// <summary>
    /// Update a pipeline's status.
    /// </summary>
    public async Task UpdateStatusAsync(string id, PipelineStatus status, string? error = null, CancellationToken cancellationToken = default)
    {
        if (!_pipelines.TryGetValue(id, out var pipeline))
        {
            throw new InvalidOperationException($"Pipeline '{id}' not found");
        }

        var updated = pipeline with
        {
            Status = status,
            Error = error,
            UpdatedAt = DateTimeOffset.UtcNow
        };

        await SaveAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Update a pipeline's schedule configuration.
    /// </summary>
    public async Task UpdateScheduleAsync(string id, ScheduleConfig schedule, CancellationToken cancellationToken = default)
    {
        if (!_pipelines.TryGetValue(id, out var pipeline))
        {
            throw new InvalidOperationException($"Pipeline '{id}' not found");
        }

        var updated = pipeline with
        {
            Schedule = schedule,
            UpdatedAt = DateTimeOffset.UtcNow
        };

        await SaveAsync(updated, cancellationToken);
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client != null)
        {
            await _client.DisposeAsync();
            _client = null;
        }
    }
}
