using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Central management of cluster links for geo-replication.
/// Handles link lifecycle, topic discovery, and coordination of fetcher threads.
/// </summary>
public sealed partial class ClusterLinkManager : IAsyncDisposable
{
    private readonly LogManager _logManager;
    private readonly MirrorTopicManager _mirrorTopicManager;
    private readonly IPeerTransport _peerTransport;
    private readonly IClusteringMetrics? _metrics;
    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<string, ClusterLink> _links = new();
    private readonly ConcurrentDictionary<string, GeoReplicaFetcher> _fetchers = new();
    private readonly ConcurrentDictionary<string, ConsumerOffsetSyncService> _offsetSyncServices = new();
    private readonly ConcurrentDictionary<string, ConfigSyncService> _configSyncServices = new();

    private CancellationTokenSource? _cts;
    private Task? _metadataSyncTask;

    public MirrorTopicManager MirrorTopicManager => _mirrorTopicManager;

    public ClusterLinkManager(
        LogManager logManager,
        IPeerTransport peerTransport,
        IClusteringMetrics? metrics,
        ILogger logger)
    {
        _logManager = logManager;
        _peerTransport = peerTransport;
        _metrics = metrics;
        _logger = logger;
        _mirrorTopicManager = new MirrorTopicManager(logManager, logger);
    }

    /// <summary>
    /// Start the manager and initialize configured links.
    /// </summary>
    public async Task StartAsync(ClusterLinkConfig[] configs, CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        foreach (var config in configs)
        {
            await CreateLinkAsync(config, ct);
        }

        // Start background metadata sync loop
        _metadataSyncTask = Task.Run(() => MetadataSyncLoopAsync(_cts.Token), _cts.Token);

        LogManagerStarted(configs.Length);
    }

    /// <summary>
    /// Create and connect a new cluster link.
    /// </summary>
    public async Task<ClusterLinkStatus> CreateLinkAsync(ClusterLinkConfig config, CancellationToken ct = default)
    {
        if (_links.ContainsKey(config.LinkId))
            throw new InvalidOperationException($"Link {config.LinkId} already exists");

        var link = new ClusterLink(config, _peerTransport, _logger);

        try
        {
            await link.ConnectAsync(ct);
            _links[config.LinkId] = link;

            // Create and start fetcher
            var fetcher = new GeoReplicaFetcher(link, _logManager, _metrics, _logger);
            _fetchers[config.LinkId] = fetcher;
            await fetcher.StartAsync(ct);

            // Create consumer offset sync if enabled
            if (config.SyncConsumerOffsets)
            {
                var offsetSync = new ConsumerOffsetSyncService(
                    link, _logManager, _mirrorTopicManager, _logger, config.ConsumerOffsetSyncIntervalMs);
                _offsetSyncServices[config.LinkId] = offsetSync;
                await offsetSync.StartAsync(ct);
            }

            // Create config sync if enabled
            if (config.SyncTopicConfigs)
            {
                var configSync = new ConfigSyncService(
                    link, _logManager, _mirrorTopicManager, _logger, config.MetadataSyncIntervalMs);
                _configSyncServices[config.LinkId] = configSync;
                await configSync.StartAsync(ct);
            }

            // Discover and create mirror topics
            await DiscoverAndMirrorTopicsAsync(link, fetcher, ct);

            LogLinkCreated(config.LinkId, config.RemoteBootstrapServers);
        }
        catch (Exception ex)
        {
            link.SetState(ClusterLinkState.Error, ex.Message);
            _links[config.LinkId] = link;
            LogLinkCreateFailed(config.LinkId, ex);
        }

        return GetLinkStatus(config.LinkId);
    }

    /// <summary>
    /// Remove a cluster link and stop all associated services.
    /// </summary>
    public async Task RemoveLinkAsync(string linkId, CancellationToken ct = default)
    {
        if (!_links.TryRemove(linkId, out var link))
            throw new InvalidOperationException($"Link {linkId} not found");

        // Stop fetcher
        if (_fetchers.TryRemove(linkId, out var fetcher))
            await fetcher.DisposeAsync();

        // Stop offset sync
        if (_offsetSyncServices.TryRemove(linkId, out var offsetSync))
            await offsetSync.DisposeAsync();

        // Stop config sync
        if (_configSyncServices.TryRemove(linkId, out var configSync))
            await configSync.DisposeAsync();

        // Disconnect link
        await link.DisconnectAsync();
        await link.DisposeAsync();

        LogLinkRemoved(linkId);
    }

    /// <summary>
    /// Pause a cluster link (stops fetching but maintains connection).
    /// </summary>
    public Task PauseLinkAsync(string linkId)
    {
        if (!_links.TryGetValue(linkId, out var link))
            throw new InvalidOperationException($"Link {linkId} not found");

        link.SetState(ClusterLinkState.Paused);
        LogLinkPaused(linkId);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Resume a paused cluster link.
    /// </summary>
    public async Task ResumeLinkAsync(string linkId, CancellationToken ct = default)
    {
        if (!_links.TryGetValue(linkId, out var link))
            throw new InvalidOperationException($"Link {linkId} not found");

        if (!link.IsConnected)
            await link.ConnectAsync(ct);

        link.SetState(ClusterLinkState.Active);
        LogLinkResumed(linkId);
    }

    /// <summary>
    /// Get status of a specific link.
    /// </summary>
    public ClusterLinkStatus GetLinkStatus(string linkId)
    {
        if (!_links.TryGetValue(linkId, out var link))
            throw new InvalidOperationException($"Link {linkId} not found");

        var mirrorTopics = _mirrorTopicManager.GetMirrorTopicsForLink(linkId);
        var totalLag = _fetchers.TryGetValue(linkId, out var fetcher) ? fetcher.GetTotalLag() : 0;

        return new ClusterLinkStatus
        {
            LinkId = linkId,
            State = link.State,
            RemoteClusterId = link.Config.RemoteClusterId,
            MirroredTopicCount = mirrorTopics.Count,
            TotalLagMessages = totalLag,
            LastFetchTimestamp = link.LastFetchTimestamp,
            ErrorMessage = link.ErrorMessage
        };
    }

    /// <summary>
    /// Get status of a specific link, or <c>null</c> if the link does not exist.
    /// Non-throwing variant of <see cref="GetLinkStatus"/> for management APIs.
    /// </summary>
    public ClusterLinkStatus? GetLinkStatusOrNull(string linkId)
    {
        if (!_links.ContainsKey(linkId))
            return null;

        try
        {
            return GetLinkStatus(linkId);
        }
        catch (InvalidOperationException)
        {
            // Link was removed concurrently between the existence check and the status read.
            return null;
        }
    }

    /// <summary>
    /// Get all cluster links. Links removed concurrently while enumerating are skipped.
    /// </summary>
    public List<ClusterLinkStatus> GetAllLinks() =>
        _links.Keys
            .Select(GetLinkStatusOrNull)
            .OfType<ClusterLinkStatus>()
            .ToList();

    /// <summary>
    /// Promote a mirror topic (planned migration).
    /// </summary>
    public async Task<bool> PromoteMirrorTopicAsync(string topic, TimeSpan timeout, CancellationToken ct = default)
    {
        var state = _mirrorTopicManager.GetMirrorTopicState(topic);
        if (state == null) return false;

        _fetchers.TryGetValue(state.LinkId, out var fetcher);
        return await _mirrorTopicManager.PromoteMirrorTopicAsync(topic, fetcher, timeout, ct);
    }

    /// <summary>
    /// Failover a mirror topic (emergency).
    /// </summary>
    public async Task<bool> FailoverMirrorTopicAsync(string topic, CancellationToken ct = default)
    {
        var state = _mirrorTopicManager.GetMirrorTopicState(topic);
        if (state == null) return false;

        _fetchers.TryGetValue(state.LinkId, out var fetcher);
        return await _mirrorTopicManager.FailoverMirrorTopicAsync(topic, fetcher, ct);
    }

    private async Task DiscoverAndMirrorTopicsAsync(ClusterLink link, GeoReplicaFetcher fetcher, CancellationToken ct)
    {
        try
        {
            var remoteTopics = await link.GetRemoteTopicsAsync(ct);
            var topicFilter = new Regex(link.Config.TopicFilter);
            var excludes = new HashSet<string>(link.Config.TopicExcludes);

            foreach (var remoteTopic in remoteTopics)
            {
                if (!topicFilter.IsMatch(remoteTopic.Name))
                    continue;
                if (excludes.Contains(remoteTopic.Name))
                    continue;
                if (remoteTopic.Name.StartsWith("__", StringComparison.Ordinal))
                    continue;

                // Create mirror topic if not already exists
                if (!_mirrorTopicManager.IsMirrorTopic(remoteTopic.Name))
                {
                    await _mirrorTopicManager.CreateMirrorTopicAsync(
                        link.LinkId, remoteTopic.Name, remoteTopic.PartitionCount, ct);
                }

                // Add partitions to fetcher
                var partitions = Enumerable.Range(0, remoteTopic.PartitionCount)
                    .Select(p => new TopicPartition { Topic = remoteTopic.Name, Partition = p })
                    .ToList();
                fetcher.AddPartitions(partitions);
            }
        }
        catch (Exception ex)
        {
            LogTopicDiscoveryError(link.LinkId, ex);
        }
    }

    private async Task MetadataSyncLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Use shortest configured interval
                var interval = _links.Values
                    .Select(l => l.Config.MetadataSyncIntervalMs)
                    .DefaultIfEmpty(30_000)
                    .Min();

                await Task.Delay(interval, ct);

                foreach (var (linkId, link) in _links)
                {
                    if (link.State != ClusterLinkState.Active)
                        continue;

                    if (_fetchers.TryGetValue(linkId, out var fetcher))
                    {
                        await DiscoverAndMirrorTopicsAsync(link, fetcher, ct);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogMetadataSyncError(ex);
                await Task.Delay(5000, ct);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_metadataSyncTask != null)
        {
            try { await _metadataSyncTask; } catch (OperationCanceledException) { }
        }

        foreach (var fetcher in _fetchers.Values)
            await fetcher.DisposeAsync();

        foreach (var offsetSync in _offsetSyncServices.Values)
            await offsetSync.DisposeAsync();

        foreach (var configSync in _configSyncServices.Values)
            await configSync.DisposeAsync();

        foreach (var link in _links.Values)
            await link.DisposeAsync();

        _fetchers.Clear();
        _offsetSyncServices.Clear();
        _configSyncServices.Clear();
        _links.Clear();
        _cts?.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster link manager started with {LinkCount} links")]
    private partial void LogManagerStarted(int linkCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster link {LinkId} created, connected to {Remote}")]
    private partial void LogLinkCreated(string linkId, string remote);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to create cluster link {LinkId}")]
    private partial void LogLinkCreateFailed(string linkId, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster link {LinkId} removed")]
    private partial void LogLinkRemoved(string linkId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster link {LinkId} paused")]
    private partial void LogLinkPaused(string linkId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster link {LinkId} resumed")]
    private partial void LogLinkResumed(string linkId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error discovering remote topics for link {LinkId}")]
    private partial void LogTopicDiscoveryError(string linkId, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in metadata sync loop")]
    private partial void LogMetadataSyncError(Exception ex);
}
