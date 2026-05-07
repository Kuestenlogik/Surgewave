using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Calculates consumer lag from active stream tasks and consumer high watermarks.
/// </summary>
internal sealed class StreamsLagProvider : IStreamsLagProvider
{
    private readonly string _applicationId;
    private readonly TaskManager _taskManager;
    private readonly StreamsConsumer _consumer;
    private readonly StreamsMetrics _metrics;

    public StreamsLagProvider(
        string applicationId,
        TaskManager taskManager,
        StreamsConsumer consumer,
        StreamsMetrics metrics)
    {
        _applicationId = applicationId;
        _taskManager = taskManager;
        _consumer = consumer;
        _metrics = metrics;
    }

    public ApplicationLag GetApplicationLag()
    {
        var partitions = new List<StreamsPartitionLag>();

        foreach (var task in _taskManager.ActiveTasks)
        {
            foreach (var (tp, currentOffset) in task.CurrentOffsets)
            {
                var committed = _consumer.Committed(tp) ?? 0;
                var highWatermark = _consumer.GetHighWatermark(tp);
                var lag = Math.Max(0, highWatermark - currentOffset);

                partitions.Add(new StreamsPartitionLag(
                    tp.Topic,
                    tp.Partition,
                    currentOffset,
                    committed,
                    highWatermark,
                    lag));
            }
        }

        var totalLag = partitions.Sum(p => p.Lag);
        _metrics.UpdateLag(totalLag);

        return new ApplicationLag(_applicationId, totalLag, partitions, DateTimeOffset.UtcNow);
    }

    public StreamsPartitionLag? GetPartitionLag(string topic, int partition)
    {
        var lag = GetApplicationLag();
        return lag.Partitions.FirstOrDefault(p => p.Topic == topic && p.Partition == partition);
    }

    public long GetTotalLag()
    {
        return GetApplicationLag().TotalLag;
    }
}
