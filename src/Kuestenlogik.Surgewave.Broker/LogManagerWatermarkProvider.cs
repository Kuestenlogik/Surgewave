using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Monitoring;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Adapter that answers high-watermark and log-start-offset queries for lag
/// calculation directly from the broker's <see cref="LogManager"/>.
/// </summary>
public sealed class LogManagerWatermarkProvider : IHighWatermarkProvider
{
    private readonly LogManager _logManager;

    public LogManagerWatermarkProvider(LogManager logManager)
    {
        _logManager = logManager;
    }

    public long GetHighWatermark(string topic, int partition)
    {
        var log = _logManager.GetLog(new TopicPartition { Topic = topic, Partition = partition });
        return log?.HighWatermark ?? -1;
    }

    public long GetLogStartOffset(string topic, int partition)
    {
        var log = _logManager.GetLog(new TopicPartition { Topic = topic, Partition = partition });
        return log?.LogStartOffset ?? 0;
    }
}
