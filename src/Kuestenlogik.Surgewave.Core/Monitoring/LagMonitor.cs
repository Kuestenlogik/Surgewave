using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Core.Monitoring;

/// <summary>
/// Provides lag calculation for consumer groups.
/// Does not manage consumer group state - relies on external offset providers.
/// </summary>
public interface ILagCalculator
{
    /// <summary>
    /// Calculate lag for a specific consumer group.
    /// </summary>
    ConsumerGroupLagInfo? GetGroupLag(string groupId);

    /// <summary>
    /// Get lag summary for all consumer groups.
    /// </summary>
    LagSummary GetLagSummary();

    /// <summary>
    /// Get lag measurements for metrics reporting.
    /// Returns one measurement per group/topic/partition.
    /// </summary>
    IEnumerable<Measurement<long>> GetLagMeasurements();

    /// <summary>
    /// Get maximum lag across all groups.
    /// </summary>
    long GetMaxLag();
}

/// <summary>
/// Provides committed offsets for consumer groups.
/// </summary>
public interface IOffsetProvider
{
    /// <summary>
    /// Get all committed offsets for a consumer group.
    /// Key format: "{topic}:{partition}"
    /// </summary>
    Dictionary<string, long> GetCommittedOffsets(string groupId);

    /// <summary>
    /// Get all consumer group IDs.
    /// </summary>
    IEnumerable<string> GetGroupIds();

    /// <summary>
    /// Get consumer group state.
    /// </summary>
    string GetGroupState(string groupId);

    /// <summary>
    /// Get member count for a consumer group.
    /// </summary>
    int GetMemberCount(string groupId);
}

/// <summary>
/// Provides high watermarks for topic partitions.
/// </summary>
public interface IHighWatermarkProvider
{
    /// <summary>
    /// Get high watermark for a topic partition.
    /// Returns -1 if partition doesn't exist.
    /// </summary>
    long GetHighWatermark(string topic, int partition);

    /// <summary>
    /// Get log start offset for a topic partition.
    /// Returns 0 if partition doesn't exist.
    /// </summary>
    long GetLogStartOffset(string topic, int partition);
}

/// <summary>
/// Monitors consumer group lag and generates alerts when thresholds are exceeded.
/// </summary>
public sealed class LagMonitor : IDisposable
{
    private readonly ILagCalculator _lagCalculator;
    private readonly LagAlertConfig _alertConfig;
    private readonly ILogger<LagMonitor>? _logger;
    private readonly Timer? _checkTimer;
    private readonly HashSet<string> _groupsWithHighLag = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Event raised when lag exceeds warning threshold.
    /// </summary>
    public event EventHandler<LagAlertEventArgs>? LagWarning;

    /// <summary>
    /// Event raised when lag exceeds critical threshold.
    /// </summary>
    public event EventHandler<LagAlertEventArgs>? LagCritical;

    /// <summary>
    /// Event raised when lag returns to normal after being high.
    /// </summary>
    public event EventHandler<LagAlertEventArgs>? LagNormalized;

    public LagMonitor(
        ILagCalculator lagCalculator,
        LagAlertConfig? alertConfig = null,
        ILogger<LagMonitor>? logger = null)
    {
        _lagCalculator = lagCalculator;
        _alertConfig = alertConfig ?? new LagAlertConfig();
        _logger = logger;

        if (_alertConfig.Enabled && _alertConfig.CheckInterval > TimeSpan.Zero)
        {
            _checkTimer = new Timer(
                CheckLag,
                null,
                _alertConfig.CheckInterval,
                _alertConfig.CheckInterval);
        }
    }

    /// <summary>
    /// Get lag information for a specific consumer group.
    /// </summary>
    public ConsumerGroupLagInfo? GetGroupLag(string groupId)
    {
        return _lagCalculator.GetGroupLag(groupId);
    }

    /// <summary>
    /// Get lag summary for all consumer groups.
    /// </summary>
    public LagSummary GetLagSummary()
    {
        return _lagCalculator.GetLagSummary();
    }

    /// <summary>
    /// Get the current alert configuration.
    /// </summary>
    public LagAlertConfig AlertConfig => _alertConfig;

    private void CheckLag(object? state)
    {
        try
        {
            var summary = _lagCalculator.GetLagSummary();

            lock (_lock)
            {
                var currentHighLagGroups = new HashSet<string>();

                foreach (var group in summary.Groups)
                {
                    var isHighLag = false;

                    if (group.TotalLag >= _alertConfig.CriticalThreshold)
                    {
                        isHighLag = true;
                        LagCritical?.Invoke(this, new LagAlertEventArgs(
                            group.GroupId,
                            group.TotalLag,
                            _alertConfig.CriticalThreshold,
                            LagAlertLevel.Critical));
                    }
                    else if (group.TotalLag >= _alertConfig.WarningThreshold)
                    {
                        isHighLag = true;
                        LagWarning?.Invoke(this, new LagAlertEventArgs(
                            group.GroupId,
                            group.TotalLag,
                            _alertConfig.WarningThreshold,
                            LagAlertLevel.Warning));
                    }

                    if (isHighLag)
                    {
                        currentHighLagGroups.Add(group.GroupId);
                    }

                    // Check if lag was high but now normalized
                    if (_groupsWithHighLag.Contains(group.GroupId) && !isHighLag)
                    {
                        LagNormalized?.Invoke(this, new LagAlertEventArgs(
                            group.GroupId,
                            group.TotalLag,
                            _alertConfig.WarningThreshold,
                            LagAlertLevel.Normal));
                    }
                }

                _groupsWithHighLag.Clear();
                foreach (var g in currentHighLagGroups)
                {
                    _groupsWithHighLag.Add(g);
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error checking consumer lag");
        }
    }

    public void Dispose()
    {
        _checkTimer?.Dispose();
    }
}

/// <summary>
/// Default implementation of ILagCalculator that uses offset and watermark providers.
/// </summary>
public sealed class DefaultLagCalculator : ILagCalculator
{
    private readonly IOffsetProvider _offsetProvider;
    private readonly IHighWatermarkProvider _watermarkProvider;

    public DefaultLagCalculator(
        IOffsetProvider offsetProvider,
        IHighWatermarkProvider watermarkProvider)
    {
        _offsetProvider = offsetProvider;
        _watermarkProvider = watermarkProvider;
    }

    public ConsumerGroupLagInfo? GetGroupLag(string groupId)
    {
        var offsets = _offsetProvider.GetCommittedOffsets(groupId);
        if (offsets.Count == 0)
        {
            return null;
        }

        var topicPartitions = ParseOffsets(offsets);
        var topics = CalculateTopicLags(topicPartitions);

        return new ConsumerGroupLagInfo
        {
            GroupId = groupId,
            State = _offsetProvider.GetGroupState(groupId),
            TotalLag = topics.Sum(t => t.TotalLag),
            PartitionCount = topicPartitions.Count,
            MemberCount = _offsetProvider.GetMemberCount(groupId),
            Topics = topics
        };
    }

    public LagSummary GetLagSummary()
    {
        var groups = new List<ConsumerGroupLagInfo>();
        var maxLag = 0L;
        string? maxLagGroup = null;
        var groupsWithHighLag = 0;

        foreach (var groupId in _offsetProvider.GetGroupIds())
        {
            var groupLag = GetGroupLag(groupId);
            if (groupLag != null)
            {
                groups.Add(groupLag);

                if (groupLag.TotalLag > maxLag)
                {
                    maxLag = groupLag.TotalLag;
                    maxLagGroup = groupLag.GroupId;
                }

                if (groupLag.TotalLag > 1000) // Default warning threshold
                {
                    groupsWithHighLag++;
                }
            }
        }

        return new LagSummary
        {
            GroupCount = groups.Count,
            GroupsWithHighLag = groupsWithHighLag,
            TotalLag = groups.Sum(g => g.TotalLag),
            MaxLag = maxLag,
            MaxLagGroup = maxLagGroup,
            Groups = groups
        };
    }

    public IEnumerable<Measurement<long>> GetLagMeasurements()
    {
        foreach (var groupId in _offsetProvider.GetGroupIds())
        {
            var offsets = _offsetProvider.GetCommittedOffsets(groupId);

            foreach (var (key, committedOffset) in offsets)
            {
                var parts = key.Split(':');
                if (parts.Length != 2 || !int.TryParse(parts[1], out var partition))
                {
                    continue;
                }

                var topic = parts[0];
                var highWatermark = _watermarkProvider.GetHighWatermark(topic, partition);
                var lag = highWatermark >= 0 && committedOffset >= 0
                    ? Math.Max(0, highWatermark - committedOffset)
                    : 0;

                yield return new Measurement<long>(lag, new TagList
                {
                    { "group_id", groupId },
                    { "topic", topic },
                    { "partition", partition }
                });
            }
        }
    }

    public long GetMaxLag()
    {
        var maxLag = 0L;

        foreach (var groupId in _offsetProvider.GetGroupIds())
        {
            var offsets = _offsetProvider.GetCommittedOffsets(groupId);

            foreach (var (key, committedOffset) in offsets)
            {
                var parts = key.Split(':');
                if (parts.Length != 2 || !int.TryParse(parts[1], out var partition))
                {
                    continue;
                }

                var topic = parts[0];
                var highWatermark = _watermarkProvider.GetHighWatermark(topic, partition);
                var lag = highWatermark >= 0 && committedOffset >= 0
                    ? Math.Max(0, highWatermark - committedOffset)
                    : 0;

                if (lag > maxLag)
                {
                    maxLag = lag;
                }
            }
        }

        return maxLag;
    }

    private static List<(string Topic, int Partition, long Offset)> ParseOffsets(Dictionary<string, long> offsets)
    {
        var result = new List<(string, int, long)>();

        foreach (var (key, offset) in offsets)
        {
            var parts = key.Split(':');
            if (parts.Length == 2 && int.TryParse(parts[1], out var partition))
            {
                result.Add((parts[0], partition, offset));
            }
        }

        return result;
    }

    private List<TopicLagInfo> CalculateTopicLags(List<(string Topic, int Partition, long Offset)> topicPartitions)
    {
        var topicGroups = topicPartitions.GroupBy(tp => tp.Topic);
        var topics = new List<TopicLagInfo>();

        foreach (var group in topicGroups)
        {
            var partitions = new List<PartitionLagInfo>();

            foreach (var (topic, partition, committedOffset) in group)
            {
                var highWatermark = _watermarkProvider.GetHighWatermark(topic, partition);
                var logStartOffset = _watermarkProvider.GetLogStartOffset(topic, partition);
                var lag = highWatermark >= 0 && committedOffset >= 0
                    ? Math.Max(0, highWatermark - committedOffset)
                    : 0;

                partitions.Add(new PartitionLagInfo
                {
                    Partition = partition,
                    CommittedOffset = committedOffset,
                    HighWatermark = highWatermark,
                    Lag = lag,
                    LogStartOffset = logStartOffset
                });
            }

            topics.Add(new TopicLagInfo
            {
                Topic = group.Key,
                TotalLag = partitions.Sum(p => p.Lag),
                Partitions = partitions
            });
        }

        return topics;
    }
}

/// <summary>
/// Adapter that wraps LogManager to provide high watermarks.
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

/// <summary>
/// Alert level for lag events.
/// </summary>
public enum LagAlertLevel
{
    Normal,
    Warning,
    Critical
}

/// <summary>
/// Event arguments for lag alert events.
/// </summary>
public sealed class LagAlertEventArgs : EventArgs
{
    public string GroupId { get; }
    public long CurrentLag { get; }
    public long Threshold { get; }
    public LagAlertLevel Level { get; }
    public DateTimeOffset Timestamp { get; }

    public LagAlertEventArgs(string groupId, long currentLag, long threshold, LagAlertLevel level)
    {
        GroupId = groupId;
        CurrentLag = currentLag;
        Threshold = threshold;
        Level = level;
        Timestamp = DateTimeOffset.UtcNow;
    }
}
