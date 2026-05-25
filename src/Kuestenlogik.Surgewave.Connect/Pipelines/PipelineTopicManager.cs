using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Manages internal topics for pipeline node-to-node communication.
/// Uses Surgewave native client for high-performance direct communication.
/// </summary>
public sealed class PipelineTopicManager : IAsyncDisposable
{
    private const string TopicPrefix = "_pipeline-";

    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<PipelineTopicManager> _logger;
    private SurgewaveNativeClient? _client;

    public PipelineTopicManager(ConnectWorkerConfig config, ILogger<PipelineTopicManager> logger)
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
    /// Generate a unique topic name for a pipeline connection.
    /// </summary>
    public static string GetConnectionTopicName(string pipelineId, string connectionId)
    {
        return $"{TopicPrefix}{pipelineId}-{connectionId}";
    }

    /// <summary>
    /// Create internal topics for all connections in a pipeline.
    /// </summary>
    public async Task CreateTopicsForPipelineAsync(PipelineDefinition pipeline, CancellationToken cancellationToken = default)
    {
        if (pipeline.Connections.Count == 0)
        {
            return;
        }

        await EnsureConnectedAsync(cancellationToken);

        foreach (var connection in pipeline.Connections)
        {
            var topicName = GetConnectionTopicName(pipeline.Id, connection.Id);

            try
            {
                await _client!.Topics.Create(topicName)
                    .WithPartitions(1)
                    .WithReplicationFactor(1)
                    .WithConfig("cleanup.policy", "delete")
                    .WithConfig("retention.ms", "86400000") // 24 hours
                    .ExecuteAsync(cancellationToken);

                _logger.LogDebug("Created internal topic: {Topic}", topicName);
            }
            catch (Exception ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("Topic {Topic} already exists", topicName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create topic {Topic}", topicName);
            }
        }

        _logger.LogInformation("Created {Count} internal topic(s) for pipeline {Id}", pipeline.Connections.Count, pipeline.Id);
    }

    /// <summary>
    /// Delete internal topics for a pipeline.
    /// </summary>
    public async Task DeleteTopicsForPipelineAsync(PipelineDefinition pipeline, CancellationToken cancellationToken = default)
    {
        var topicNames = pipeline.Connections
            .Select(c => GetConnectionTopicName(pipeline.Id, c.Id))
            .ToList();

        if (topicNames.Count == 0)
        {
            return;
        }

        await EnsureConnectedAsync(cancellationToken);

        foreach (var topicName in topicNames)
        {
            try
            {
                await _client!.Topics.DeleteAsync(topicName, cancellationToken);
                _logger.LogDebug("Deleted internal topic: {Topic}", topicName);
            }
            catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase) ||
                                        ex.Message.Contains("unknown topic", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("Topic {Topic} does not exist", topicName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete topic {Topic}", topicName);
            }
        }

        _logger.LogInformation("Deleted {Count} internal topic(s) for pipeline {Id}", topicNames.Count, pipeline.Id);
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

    /// <summary>
    /// Get updated connections with internal topic names assigned.
    /// </summary>
    public static List<PipelineConnection> AssignTopicNames(string pipelineId, List<PipelineConnection> connections)
    {
        return connections.Select(c => c with
        {
            InternalTopic = GetConnectionTopicName(pipelineId, c.Id)
        }).ToList();
    }
}
