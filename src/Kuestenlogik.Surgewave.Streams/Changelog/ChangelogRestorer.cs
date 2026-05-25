using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Restores changelog-backed state stores from their changelog topics.
/// Fires IStateRestoreListener callbacks for progress tracking.
/// </summary>
internal sealed class ChangelogRestorer
{
    private readonly StreamsConsumer? _consumer;
    private readonly IStateRestoreListener _listener;
    private readonly ILogger _logger;
    private readonly int _batchReportInterval;

    public ChangelogRestorer(
        StreamsConsumer? consumer,
        IStateRestoreListener listener,
        ILogger logger,
        int batchReportInterval = 100)
    {
        _consumer = consumer;
        _listener = listener;
        _logger = logger;
        _batchReportInterval = batchReportInterval;
    }

    /// <summary>
    /// Restores a single changelog-backed store from its changelog topic.
    /// </summary>
    public async Task RestoreStoreAsync(
        IChangelogBacked store,
        CancellationToken cancellationToken)
    {
        if (_consumer == null)
        {
            _logger.LogDebug("No consumer available for changelog restoration of {StoreName}",
                ((IStateStore)store).Name);
            return;
        }

        var storeName = ((IStateStore)store).Name;
        var topicName = store.ChangelogTopicName;
        var partition = store.ChangelogPartition;
        var tp = new TopicPartition(topicName, partition);

        _logger.LogInformation("Starting changelog restoration for store {StoreName} from {Topic}-{Partition}",
            storeName, topicName, partition);

        // Get end offset (high watermark)
        var startingOffset = 0L;
        var endingOffset = _consumer.GetEndOffset(tp);

        var context = new StateRestoreContext
        {
            StoreName = storeName,
            Topic = topicName,
            Partition = partition,
            StartingOffset = startingOffset,
            EndingOffset = endingOffset,
            TotalRestored = 0
        };

        // Fire restore start
        _listener.OnRestoreStart(context);

        if (endingOffset <= 0)
        {
            _logger.LogInformation("No records to restore for store {StoreName}", storeName);
            _listener.OnRestoreEnd(context, 0);
            return;
        }

        // Seek to beginning of changelog
        _consumer.Seek(tp, startingOffset);

        var totalRestored = 0L;
        var batchCount = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var records = await _consumer.PollAsync(TimeSpan.FromMilliseconds(100), cancellationToken);

                if (records.Count == 0)
                    break; // No more records

                var reachedEnd = false;

                foreach (var record in records)
                {
                    if (record.Topic != topicName || record.Partition != partition)
                        continue;

                    // Apply record to store
                    store.RestoreRecord(record.Key, record.Value);
                    totalRestored++;
                    batchCount++;

                    if (record.Offset >= endingOffset - 1)
                    {
                        reachedEnd = true;
                        break;
                    }
                }

                context.TotalRestored = totalRestored;

                // Report batch progress at intervals
                if (batchCount >= _batchReportInterval)
                {
                    _listener.OnBatchRestored(context, batchCount);
                    batchCount = 0;
                }

                if (reachedEnd)
                    break;
            }

            // Report any remaining batch
            if (batchCount > 0)
            {
                _listener.OnBatchRestored(context, batchCount);
            }

            // Fire restore end
            _listener.OnRestoreEnd(context, totalRestored);

            _logger.LogInformation("Completed changelog restoration for store {StoreName}: {Count} records restored",
                storeName, totalRestored);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Changelog restoration cancelled for store {StoreName} after {Count} records",
                storeName, totalRestored);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during changelog restoration for store {StoreName} after {Count} records",
                storeName, totalRestored);
            throw;
        }
    }
}
