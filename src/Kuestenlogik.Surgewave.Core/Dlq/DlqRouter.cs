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

            // Serialize and produce. The payload-copy decision happens HERE, not at the call site:
            // a caller that hands us the value must not thereby cause it to be stored.
            var serialized = DlqRecordSerializer.Serialize(ApplyCopyPolicy(record));
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

    /// <summary>
    /// Strips what must not be copied. The key is deliberately kept: it is the routing and
    /// compaction key, and a DLQ record that cannot be correlated back to its subject is of little
    /// use. Value and headers are the parts that carry payload.
    /// </summary>
    private DlqRecord ApplyCopyPolicy(DlqRecord record)
    {
        if (_config.CopyRecordValue)
            return record;

        if (record.OriginalValue is null && record.OriginalHeaders is null)
            return record;

        return record with { OriginalValue = null, OriginalHeaders = null };
    }

    private async Task EnsureDlqTopicExistsAsync(string dlqTopic, CancellationToken cancellationToken)
    {
        // Fast path: already ensured
        if (_ensuredTopics.ContainsKey(dlqTopic))
        {
            return;
        }

        if (!_config.AutoCreateTopics)
        {
            // Creating a topic nobody declared is the operator's call, not ours. Producing into a
            // topic that does not exist fails loudly, which is the intended outcome.
            _logger?.LogDebug(
                "DLQ topic {DlqTopic} not created: automatic creation is disabled", dlqTopic);
            _ensuredTopics[dlqTopic] = true;
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
