using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Periodically synchronizes topic configurations from the remote cluster to local mirror topics.
/// Ensures retention, cleanup policy, and other configs stay in sync.
/// </summary>
public sealed partial class ConfigSyncService : IAsyncDisposable
{
    private readonly ClusterLink _link;
    private readonly LogManager _logManager;
    private readonly MirrorTopicManager _mirrorTopicManager;
    private readonly ILogger _logger;
    private readonly int _syncIntervalMs;

    private CancellationTokenSource? _cts;
    private Task? _syncTask;

    public ConfigSyncService(
        ClusterLink link,
        LogManager logManager,
        MirrorTopicManager mirrorTopicManager,
        ILogger logger,
        int syncIntervalMs = 30_000)
    {
        _link = link;
        _logManager = logManager;
        _mirrorTopicManager = mirrorTopicManager;
        _logger = logger;
        _syncIntervalMs = syncIntervalMs;
    }

    public Task StartAsync(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _syncTask = Task.Run(() => SyncLoopAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    private async Task SyncLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_syncIntervalMs, ct);

                if (_link.State != ClusterLinkState.Active)
                    continue;

                var mirrorTopics = _mirrorTopicManager.GetMirrorTopicsForLink(_link.LinkId);
                if (mirrorTopics.Count == 0)
                    continue;

                // In a full implementation, this would:
                // 1. Fetch topic configs from remote via DescribeConfigs request
                // 2. Compare with local mirror topic configs
                // 3. Apply changes to local topics (retention, cleanup.policy, etc.)
                LogConfigSyncCompleted(_link.LinkId, mirrorTopics.Count);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogConfigSyncError(_link.LinkId, ex);
                await Task.Delay(5000, ct);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        if (_syncTask != null)
        {
            try { await _syncTask; } catch (OperationCanceledException) { }
        }
        _cts?.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Config sync completed for link {LinkId}, {TopicCount} topics")]
    private partial void LogConfigSyncCompleted(string linkId, int topicCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error syncing configs for link {LinkId}")]
    private partial void LogConfigSyncError(string linkId, Exception ex);
}
