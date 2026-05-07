using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Manages the lifecycle of mirror topics: creation, promotion, and failover.
/// </summary>
public sealed partial class MirrorTopicManager
{
    private readonly LogManager _logManager;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, MirrorTopicState> _mirrorTopics = new();

    public MirrorTopicManager(LogManager logManager, ILogger logger)
    {
        _logManager = logManager;
        _logger = logger;
    }

    /// <summary>
    /// Create a local mirror topic with read-only flag set.
    /// </summary>
    public async Task<bool> CreateMirrorTopicAsync(
        string linkId,
        string sourceTopic,
        int partitionCount,
        CancellationToken ct = default)
    {
        if (_mirrorTopics.ContainsKey(sourceTopic))
        {
            _logger.LogWarning("Mirror topic {Topic} already exists", sourceTopic);
            return false;
        }

        // Create the topic via LogManager
        var metadata = await _logManager.CreateTopicAsync(sourceTopic, partitionCount, cancellationToken: ct);

        // Set mirror metadata
        metadata.IsMirror = true;
        metadata.IsReadOnly = true;
        metadata.SourceLinkId = linkId;

        var state = new MirrorTopicState
        {
            SourceTopic = sourceTopic,
            LinkId = linkId,
            IsReadOnly = true,
            PartitionCount = partitionCount
        };
        _mirrorTopics[sourceTopic] = state;

        LogMirrorTopicCreated(sourceTopic, linkId, partitionCount);
        await Task.CompletedTask;
        return true;
    }

    /// <summary>
    /// Promote a mirror topic to a normal writable topic (planned migration).
    /// Waits for replication lag to reach zero before promoting.
    /// </summary>
    public async Task<bool> PromoteMirrorTopicAsync(
        string topic,
        GeoReplicaFetcher? fetcher,
        TimeSpan timeout,
        CancellationToken ct = default)
    {
        if (!_mirrorTopics.TryGetValue(topic, out var state))
        {
            _logger.LogWarning("Topic {Topic} is not a mirror topic", topic);
            return false;
        }

        // Wait for lag to reach 0
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (ct.IsCancellationRequested) return false;

            var totalLag = 0L;
            if (fetcher != null)
            {
                for (int p = 0; p < state.PartitionCount; p++)
                {
                    totalLag += fetcher.GetLag(new TopicPartition { Topic = topic, Partition = p });
                }
            }

            if (totalLag == 0)
                break;

            LogWaitingForZeroLag(topic, totalLag);
            await Task.Delay(500, ct);
        }

        // Stop fetcher for this topic
        fetcher?.RemoveTopic(topic);

        // Promote: set writable
        SetTopicWritable(topic);

        LogMirrorTopicPromoted(topic);
        return true;
    }

    /// <summary>
    /// Emergency failover: immediately make mirror topic writable.
    /// Warning: possible data loss if replication was behind.
    /// </summary>
    public Task<bool> FailoverMirrorTopicAsync(
        string topic,
        GeoReplicaFetcher? fetcher,
        CancellationToken ct = default)
    {
        if (!_mirrorTopics.TryGetValue(topic, out _))
        {
            _logger.LogWarning("Topic {Topic} is not a mirror topic", topic);
            return Task.FromResult(false);
        }

        // Stop fetcher immediately
        fetcher?.RemoveTopic(topic);

        // Failover: set writable
        SetTopicWritable(topic);

        LogMirrorTopicFailover(topic);
        return Task.FromResult(true);
    }

    /// <summary>
    /// Check if a topic is a mirror topic.
    /// </summary>
    public bool IsMirrorTopic(string topic) => _mirrorTopics.ContainsKey(topic);

    /// <summary>
    /// Check if a topic is read-only (mirror topics are read-only until promoted).
    /// </summary>
    public bool IsReadOnly(string topic)
    {
        if (!_mirrorTopics.TryGetValue(topic, out var state))
            return false;
        return state.IsReadOnly;
    }

    /// <summary>
    /// Get all mirror topics.
    /// </summary>
    public IReadOnlyCollection<MirrorTopicState> GetMirrorTopics() =>
        _mirrorTopics.Values.ToList().AsReadOnly();

    /// <summary>
    /// Get mirror topic state.
    /// </summary>
    public MirrorTopicState? GetMirrorTopicState(string topic) =>
        _mirrorTopics.TryGetValue(topic, out var state) ? state : null;

    /// <summary>
    /// Get mirror topics for a specific link.
    /// </summary>
    public List<MirrorTopicState> GetMirrorTopicsForLink(string linkId) =>
        _mirrorTopics.Values.Where(s => s.LinkId == linkId).ToList();

    private void SetTopicWritable(string topic)
    {
        var metadata = _logManager.GetTopicMetadata(topic);
        if (metadata != null)
        {
            metadata.IsMirror = false;
            metadata.IsReadOnly = false;
            metadata.SourceLinkId = null;
        }

        if (_mirrorTopics.TryGetValue(topic, out _))
        {
            _mirrorTopics.TryRemove(topic, out _);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created mirror topic {Topic} from link {LinkId} with {Partitions} partitions")]
    private partial void LogMirrorTopicCreated(string topic, string linkId, int partitions);

    [LoggerMessage(Level = LogLevel.Information, Message = "Mirror topic {Topic} promoted to writable")]
    private partial void LogMirrorTopicPromoted(string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Mirror topic {Topic} failover completed (possible data loss)")]
    private partial void LogMirrorTopicFailover(string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting for zero lag on mirror topic {Topic}, current lag: {Lag}")]
    private partial void LogWaitingForZeroLag(string topic, long lag);
}
