using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Stores pipeline execution history in a Surgewave topic.
/// </summary>
public sealed class ExecutionStore : IAsyncDisposable
{
    private const string ExecutionsTopic = "_surgewave-pipeline-executions";
    private const int MaxSampleValueLength = 1024;
    private const int DefaultRetentionDays = 7;

    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<ExecutionStore> _logger;
    private readonly ConcurrentDictionary<string, ExecutionRecord> _executions = new();
    private readonly ConcurrentDictionary<string, List<string>> _pipelineExecutions = new();
    private SurgewaveNativeClient? _client;
    private bool _isLoaded;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public ExecutionStore(ConnectWorkerConfig config, ILogger<ExecutionStore> logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Initialize connection to Surgewave.
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
    /// Load execution history from the topic.
    /// </summary>
    public async Task LoadAsync(CancellationToken cancellationToken = default)
    {
        if (_isLoaded) return;

        try
        {
            await EnsureConnectedAsync(cancellationToken);
            await EnsureTopicExistsAsync(cancellationToken);

            long offset = 0;
            var cutoff = DateTimeOffset.UtcNow.AddDays(-DefaultRetentionDays);

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = await _client!.Messaging.ReceiveAsync(
                    ExecutionsTopic,
                    partition: 0,
                    offset: offset,
                    maxBytes: 1024 * 1024,
                    maxWaitMs: 100,
                    cancellationToken: cancellationToken);

                if (result.Messages.Count == 0) break;

                foreach (var message in result.Messages)
                {
                    var executionId = message.Key != null ? Encoding.UTF8.GetString(message.Key) : null;
                    if (string.IsNullOrEmpty(executionId))
                    {
                        offset = message.Offset + 1;
                        continue;
                    }

                    if (message.Value == null || message.Value.Length == 0)
                    {
                        // Tombstone
                        if (_executions.TryRemove(executionId, out var removed))
                        {
                            RemoveFromPipelineIndex(removed.PipelineId, executionId);
                        }
                    }
                    else
                    {
                        try
                        {
                            var json = Encoding.UTF8.GetString(message.Value);
                            var execution = JsonSerializer.Deserialize<ExecutionRecord>(json, JsonOptions);
                            if (execution != null && execution.StartedAt >= cutoff)
                            {
                                _executions[executionId] = execution;
                                AddToPipelineIndex(execution.PipelineId, executionId);
                            }
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogWarning(ex, "Invalid execution JSON for: {Id}", executionId);
                        }
                    }

                    offset = message.Offset + 1;
                }
            }

            _logger.LogInformation("Loaded {Count} execution record(s)", _executions.Count);
            _isLoaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load execution history");
            _isLoaded = true;
        }
    }

    private async Task EnsureTopicExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var topics = await _client!.Topics.ListAsync(cancellationToken);
            if (!topics.Any(t => t.Name == ExecutionsTopic))
            {
                await _client.Topics.Create(ExecutionsTopic)
                    .WithPartitions(1)
                    .WithReplicationFactor(1)
                    .WithConfig("cleanup.policy", "delete")
                    .WithConfig("retention.ms", (DefaultRetentionDays * 24 * 60 * 60 * 1000).ToString())
                    .ExecuteAsync(cancellationToken);
                _logger.LogInformation("Created execution store topic: {Topic}", ExecutionsTopic);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Topic check/create failed (may already exist)");
        }
    }

    /// <summary>
    /// Get all executions for a pipeline.
    /// </summary>
    public IReadOnlyList<ExecutionRecord> GetByPipeline(string pipelineId, int limit = 100, int offset = 0)
    {
        if (!_pipelineExecutions.TryGetValue(pipelineId, out var executionIds))
        {
            return [];
        }

        return executionIds
            .OrderByDescending(id => _executions.TryGetValue(id, out var e) ? e.StartedAt : DateTimeOffset.MinValue)
            .Skip(offset)
            .Take(limit)
            .Select(id => _executions.TryGetValue(id, out var e) ? e : null)
            .Where(e => e != null)
            .Cast<ExecutionRecord>()
            .ToList();
    }

    /// <summary>
    /// Get a specific execution.
    /// </summary>
    public ExecutionRecord? Get(string executionId)
    {
        return _executions.TryGetValue(executionId, out var execution) ? execution : null;
    }

    /// <summary>
    /// Get execution count for a pipeline.
    /// </summary>
    public int GetCount(string pipelineId)
    {
        return _pipelineExecutions.TryGetValue(pipelineId, out var ids) ? ids.Count : 0;
    }

    /// <summary>
    /// Create a new execution record.
    /// </summary>
    public async Task<ExecutionRecord> CreateAsync(
        string pipelineId,
        string pipelineName,
        string triggerType = "manual",
        Dictionary<string, string>? triggerMetadata = null,
        CancellationToken cancellationToken = default)
    {
        var execution = new ExecutionRecord
        {
            Id = Guid.NewGuid().ToString("N"),
            PipelineId = pipelineId,
            PipelineName = pipelineName,
            Status = ExecutionStatus.Running,
            StartedAt = DateTimeOffset.UtcNow,
            TriggerType = triggerType,
            TriggerMetadata = triggerMetadata
        };

        await SaveAsync(execution, cancellationToken);
        return execution;
    }

    /// <summary>
    /// Update an execution record.
    /// </summary>
    public async Task UpdateAsync(ExecutionRecord execution, CancellationToken cancellationToken = default)
    {
        await SaveAsync(execution, cancellationToken);
    }

    /// <summary>
    /// Mark an execution as completed.
    /// </summary>
    public async Task CompleteAsync(
        string executionId,
        long recordsProcessed,
        long recordsFailed,
        List<NodeExecutionRecord>? nodes = null,
        CancellationToken cancellationToken = default)
    {
        if (!_executions.TryGetValue(executionId, out var execution))
        {
            return;
        }

        var completed = execution with
        {
            Status = recordsFailed > 0 ? ExecutionStatus.Failed : ExecutionStatus.Completed,
            CompletedAt = DateTimeOffset.UtcNow,
            RecordsProcessed = recordsProcessed,
            RecordsFailed = recordsFailed,
            Nodes = nodes ?? execution.Nodes
        };

        await SaveAsync(completed, cancellationToken);
    }

    /// <summary>
    /// Mark an execution as failed.
    /// </summary>
    public async Task FailAsync(
        string executionId,
        string error,
        string? stackTrace = null,
        CancellationToken cancellationToken = default)
    {
        if (!_executions.TryGetValue(executionId, out var execution))
        {
            return;
        }

        var failed = execution with
        {
            Status = ExecutionStatus.Failed,
            CompletedAt = DateTimeOffset.UtcNow,
            Error = error,
            StackTrace = stackTrace
        };

        await SaveAsync(failed, cancellationToken);
    }

    /// <summary>
    /// Save an execution record.
    /// </summary>
    private async Task SaveAsync(ExecutionRecord execution, CancellationToken cancellationToken)
    {
        await EnsureConnectedAsync(cancellationToken);

        var key = Encoding.UTF8.GetBytes(execution.Id);
        var value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(execution, JsonOptions));

        await _client!.Messaging.SendAsync(
            ExecutionsTopic,
            partition: 0,
            key: key,
            value: value,
            cancellationToken: cancellationToken);

        _executions[execution.Id] = execution;
        AddToPipelineIndex(execution.PipelineId, execution.Id);
    }

    /// <summary>
    /// Create a sample record for debugging.
    /// </summary>
    public static SampleRecord CreateSample(byte[]? key, byte[] value, DateTimeOffset timestamp)
    {
        var keyStr = key != null ? Encoding.UTF8.GetString(key) : null;
        var valueStr = Encoding.UTF8.GetString(value);
        var truncated = valueStr.Length > MaxSampleValueLength;

        return new SampleRecord
        {
            Key = keyStr,
            Value = truncated ? valueStr[..MaxSampleValueLength] + "..." : valueStr,
            Timestamp = timestamp,
            Truncated = truncated
        };
    }

    private void AddToPipelineIndex(string pipelineId, string executionId)
    {
        var list = _pipelineExecutions.GetOrAdd(pipelineId, _ => []);
        lock (list)
        {
            if (!list.Contains(executionId))
            {
                list.Add(executionId);
            }
        }
    }

    private void RemoveFromPipelineIndex(string pipelineId, string executionId)
    {
        if (_pipelineExecutions.TryGetValue(pipelineId, out var list))
        {
            lock (list)
            {
                list.Remove(executionId);
            }
        }
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
