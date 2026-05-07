using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Background service that periodically monitors topics and infers JSON Schemas
/// from sampled messages. Stores inferred schemas in the Schema Registry with
/// subject name "{topic}-inferred-value".
/// </summary>
public sealed class SchemaInferenceService : BackgroundService
{
    private readonly SchemaInferenceConfig _config;
    private readonly SchemaInferenceEngine _engine;
    private readonly ISchemaStore _store;
    private readonly ITopicMessageSampler _sampler;
    private readonly ILogger<SchemaInferenceService> _logger;

    private readonly ConcurrentDictionary<string, InferredSchemaEntry> _inferredSchemas = new();

    public SchemaInferenceService(
        SchemaInferenceConfig config,
        SchemaInferenceEngine engine,
        ISchemaStore store,
        ITopicMessageSampler sampler,
        ILogger<SchemaInferenceService> logger)
    {
        _config = config;
        _engine = engine;
        _store = store;
        _sampler = sampler;
        _logger = logger;
    }

    /// <summary>
    /// Infer schema from a specific topic on demand (used by REST API).
    /// </summary>
    public async Task<InferredSchemaResponse?> InferSchemaForTopicAsync(
        string topicName,
        int? sampleSize = null,
        CancellationToken cancellationToken = default)
    {
        var actualSampleSize = sampleSize ?? _config.SampleSize;

        try
        {
            var messages = await _sampler.SampleMessagesAsync(topicName, actualSampleSize, cancellationToken);
            if (messages.Count == 0)
            {
                return null;
            }

            var schema = _engine.InferFromBatch(messages);
            if (schema is null)
            {
                return null;
            }

            var schemaString = SchemaInferenceEngine.ToJsonSchemaString(schema);
            var fieldStats = BuildFieldStats(schema, schema.SampleCount);

            var response = new InferredSchemaResponse
            {
                Topic = topicName,
                Schema = schemaString,
                SampleCount = messages.Count,
                ValidMessageCount = schema.SampleCount,
                FieldStats = fieldStats,
                InferredAt = DateTimeOffset.UtcNow
            };

            // Update cache
            _inferredSchemas[topicName] = new InferredSchemaEntry(schema, schemaString, response);

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to infer schema for topic {Topic}", topicName);
            return null;
        }
    }

    /// <summary>
    /// Register an inferred schema in the Schema Registry.
    /// </summary>
    public async Task<Schema?> RegisterInferredSchemaAsync(
        string topicName,
        CancellationToken cancellationToken = default)
    {
        // Infer if not already cached
        if (!_inferredSchemas.TryGetValue(topicName, out var entry))
        {
            var response = await InferSchemaForTopicAsync(topicName, cancellationToken: cancellationToken);
            if (response is null)
            {
                return null;
            }
            _inferredSchemas.TryGetValue(topicName, out entry);
        }

        if (entry is null)
        {
            return null;
        }

        var subject = $"{topicName}-inferred-value";

        try
        {
            return _store.RegisterSchema(subject, entry.SchemaString, SchemaType.Json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to register inferred schema for topic {Topic}", topicName);
            return null;
        }
    }

    /// <summary>
    /// Get all auto-inferred schema summaries.
    /// </summary>
    public IReadOnlyList<InferredSchemaSummary> GetInferredSchemas()
    {
        var summaries = new List<InferredSchemaSummary>();

        foreach (var (topic, entry) in _inferredSchemas)
        {
            var subject = $"{topic}-inferred-value";
            var registeredSchema = _store.GetLatestSchema(subject);

            summaries.Add(new InferredSchemaSummary
            {
                Topic = topic,
                Subject = subject,
                FieldCount = entry.Definition.Properties.Count,
                SampleCount = entry.Definition.SampleCount,
                LastInferredAt = entry.Response.InferredAt,
                Registered = registeredSchema is not null,
                SchemaId = registeredSchema?.Id
            });
        }

        return summaries;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogInformation("Schema inference is disabled");
            return;
        }

        _logger.LogInformation(
            "Schema inference started (sample={SampleSize}, interval={Interval}s, autoRegister={AutoRegister})",
            _config.SampleSize, _config.RefreshIntervalSeconds, _config.AutoRegister);

        // Initial delay to let the broker start up
        await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await InferAllTopicsAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Schema inference cycle failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(_config.RefreshIntervalSeconds), stoppingToken);
        }

        _logger.LogInformation("Schema inference stopped");
    }

    private async Task InferAllTopicsAsync(CancellationToken cancellationToken)
    {
        var topics = _sampler.GetTopics();

        foreach (var topic in topics)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            if (IsTopicExcluded(topic))
            {
                continue;
            }

            try
            {
                var response = await InferSchemaForTopicAsync(topic, cancellationToken: cancellationToken);
                if (response is null)
                {
                    continue;
                }

                if (_config.AutoRegister)
                {
                    await RegisterInferredSchemaAsync(topic, cancellationToken);
                }

                _logger.LogDebug(
                    "Inferred schema for topic {Topic}: {FieldCount} fields from {SampleCount} messages",
                    topic, response.FieldStats.Count, response.ValidMessageCount);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Failed to infer schema for topic {Topic}", topic);
            }
        }
    }

    private bool IsTopicExcluded(string topicName)
    {
        return GlobMatcher.MatchesAny(topicName, _config.ExcludedTopics);
    }

    private static List<FieldStatistic> BuildFieldStats(JsonSchemaDefinition schema, int totalCount)
    {
        var stats = new List<FieldStatistic>();
        CollectFieldStats(schema, "", stats, totalCount);
        return stats;
    }

    private static void CollectFieldStats(
        JsonSchemaDefinition schema,
        string prefix,
        List<FieldStatistic> stats,
        int totalCount)
    {
        foreach (var (name, prop) in schema.Properties)
        {
            var path = string.IsNullOrEmpty(prefix) ? name : $"{prefix}.{name}";

            stats.Add(new FieldStatistic
            {
                Path = path,
                Type = prop.Type,
                Format = prop.Format,
                Nullable = prop.Nullable,
                SeenCount = prop.SeenCount,
                TotalCount = totalCount,
                Required = schema.Required.Contains(name)
            });

            // Recurse into nested objects
            if (prop.ObjectSchema is not null)
            {
                CollectFieldStats(prop.ObjectSchema, path, stats, totalCount);
            }
        }
    }

    /// <summary>
    /// Cached inferred schema entry.
    /// </summary>
    private sealed record InferredSchemaEntry(
        JsonSchemaDefinition Definition,
        string SchemaString,
        InferredSchemaResponse Response);
}
