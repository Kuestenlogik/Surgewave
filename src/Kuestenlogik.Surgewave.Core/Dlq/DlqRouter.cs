using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Routes failed records to Dead Letter Queue topics with auto-creation.
/// </summary>
public sealed class DlqRouter : IDlqRouter
{
    private readonly DlqConfig _config;
    private readonly IDlqProducer _producer;
    private readonly ILogger<DlqRouter>? _logger;
    private readonly ConcurrentDictionary<string, bool> _ensuredTopics = new();

    public DlqRouter(
        DlqConfig config,
        IDlqProducer producer,
        ILogger<DlqRouter>? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<bool> RouteAsync(DlqRecord record, CancellationToken cancellationToken = default)
    {
        if (!_config.Enabled)
        {
            _logger?.LogDebug("DLQ routing disabled, skipping record from {Topic}:{Partition}:{Offset}",
                record.OriginalTopic, record.OriginalPartition, record.OriginalOffset);
            return false;
        }

        try
        {
            var dlqTopic = _config.GetDlqTopicName(record.OriginalTopic);

            // Ensure DLQ topic exists (cached check)
            await EnsureDlqTopicExistsAsync(dlqTopic, cancellationToken);

            // Serialize and produce
            var serialized = DlqRecordSerializer.Serialize(record);
            await _producer.ProduceAsync(dlqTopic, record.OriginalKey, serialized, cancellationToken);

            _logger?.LogWarning(
                "Message routed to DLQ {DlqTopic} from {OriginalTopic}:{Partition}:{Offset} after {Attempts} attempts - {ErrorType}: {ErrorMessage}",
                dlqTopic,
                record.OriginalTopic,
                record.OriginalPartition,
                record.OriginalOffset,
                record.AttemptCount,
                record.ExceptionType,
                record.ExceptionMessage);

            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex,
                "Failed to route message to DLQ from {Topic}:{Partition}:{Offset}",
                record.OriginalTopic, record.OriginalPartition, record.OriginalOffset);
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<int> RouteBatchAsync(IEnumerable<DlqRecord> records, CancellationToken cancellationToken = default)
    {
        var successCount = 0;
        foreach (var record in records)
        {
            if (await RouteAsync(record, cancellationToken))
            {
                successCount++;
            }
        }
        return successCount;
    }

    private async Task EnsureDlqTopicExistsAsync(string dlqTopic, CancellationToken cancellationToken)
    {
        // Fast path: already ensured
        if (_ensuredTopics.ContainsKey(dlqTopic))
        {
            return;
        }

        // Create topic with DLQ-specific configuration
        var topicConfig = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "delete",
            ["retention.ms"] = _config.RetentionMs.ToString()
        };

        await _producer.EnsureTopicExistsAsync(
            dlqTopic,
            _config.DlqPartitionCount,
            topicConfig,
            cancellationToken);

        _ensuredTopics[dlqTopic] = true;
        _logger?.LogInformation("Ensured DLQ topic exists: {DlqTopic}", dlqTopic);
    }
}
