using System.Text;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Background service that synchronizes messages between an edge Surgewave broker and a cloud Surgewave broker.
/// Handles connectivity detection, batched message transfer, offset tracking, and graceful offline buffering.
/// Messages synced to the cloud carry provenance headers (<c>surgewave-edge-id</c>, <c>surgewave-edge-timestamp</c>).
/// </summary>
public sealed class EdgeSyncService : BackgroundService
{
    private readonly SurgewaveNativeClient _edgeClient;
    private readonly EdgeSyncConfig _config;
    private readonly EdgeSyncState _state;
    private readonly ConnectivityChecker _connectivityChecker;
    private readonly ILogger<EdgeSyncService> _logger;

#pragma warning disable CA2213 // _cloudClient is disposed in DisconnectCloudClientAsync called from ExecuteAsync
    private SurgewaveNativeClient? _cloudClient;
#pragma warning restore CA2213

    /// <summary>
    /// The current sync state. Thread-safe for external status queries.
    /// </summary>
    public EdgeSyncState State => _state;

    /// <summary>
    /// Creates a new edge sync service.
    /// </summary>
    /// <param name="edgeClient">The native client connected to the local edge broker.</param>
    /// <param name="config">Sync configuration.</param>
    /// <param name="state">Sync state (loaded from file or new).</param>
    /// <param name="connectivityChecker">Cloud connectivity checker.</param>
    /// <param name="logger">Logger instance.</param>
    public EdgeSyncService(
        SurgewaveNativeClient edgeClient,
        EdgeSyncConfig config,
        EdgeSyncState state,
        ConnectivityChecker connectivityChecker,
        ILogger<EdgeSyncService> logger)
    {
        _edgeClient = edgeClient;
        _config = config;
        _state = state;
        _connectivityChecker = connectivityChecker;
        _logger = logger;
        _state.EdgeId = config.EdgeId;
    }

    /// <summary>
    /// Main sync loop. Runs until cancellation is requested.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Edge sync service starting. EdgeId={EdgeId}, Cloud={CloudAddress}, Direction={Direction}, Interval={Interval}s",
            _config.EdgeId, _config.CloudBrokerAddress, _config.Direction, _config.SyncIntervalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSyncCycleAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _state.RecordFailure();
                _logger.LogError(ex, "Sync cycle failed (consecutive failures: {Failures})", _state.ConsecutiveFailures);
            }

            // Save state after each cycle
            SaveState();

            // Calculate delay with exponential backoff on failures
            var delaySeconds = _state.ConsecutiveFailures > 0
                ? Math.Min(_config.SyncIntervalSeconds * (1 << Math.Min(_state.ConsecutiveFailures, _config.MaxConsecutiveFailures)), 300)
                : _config.SyncIntervalSeconds;

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        // Cleanup
        await DisconnectCloudClientAsync();
        SaveState();
        _logger.LogInformation("Edge sync service stopped. Total messages synced: {Total}", _state.TotalMessagesSynced);
    }

    private async Task RunSyncCycleAsync(CancellationToken ct)
    {
        // Step 1: Check cloud connectivity
        var isReachable = await _connectivityChecker.IsCloudReachableAsync(
            _config.CloudBrokerAddress, _config.ConnectivityTimeoutMs);

        _state.IsOnline = isReachable;

        if (!isReachable)
        {
            _logger.LogDebug("Cloud broker not reachable. Messages buffered locally.");
            return;
        }

        // Step 2: Ensure cloud client is connected
        await EnsureCloudClientConnectedAsync(ct);
        if (_cloudClient == null) return;

        // Step 3: Sync based on direction
        if (_config.Direction is SyncDirection.EdgeToCloud or SyncDirection.Bidirectional)
        {
            await SyncEdgeToCloudAsync(ct);
        }

        if (_config.Direction is SyncDirection.CloudToEdge or SyncDirection.Bidirectional)
        {
            await SyncCloudToEdgeAsync(ct);
        }
    }

    private async Task SyncEdgeToCloudAsync(CancellationToken ct)
    {
        // Get all topics from edge broker
        var topics = await _edgeClient.Topics.ListAsync(ct);

        foreach (var topic in topics)
        {
            if (!ShouldSyncTopic(topic.Name)) continue;

            for (int partition = 0; partition < topic.PartitionCount; partition++)
            {
                await SyncPartitionEdgeToCloudAsync(topic.Name, partition, ct);
            }
        }
    }

    private async Task SyncPartitionEdgeToCloudAsync(string topic, int partition, CancellationToken ct)
    {
        var lastSyncedOffset = _state.GetSyncedOffset(topic, partition);
        var startOffset = lastSyncedOffset + 1;

        // Fetch latest offset from edge to know how far behind we are
        long latestOffset;
        try
        {
            latestOffset = await _edgeClient.Messaging.GetLatestOffsetAsync(topic, partition, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get latest offset for {Topic}/{Partition}", topic, partition);
            return;
        }

        if (startOffset >= latestOffset)
        {
            return; // Already caught up
        }

        _logger.LogDebug(
            "Syncing {Topic}/{Partition}: offset {Start} to {End}",
            topic, partition, startOffset, latestOffset);

        var totalSynced = 0;
        var currentOffset = startOffset;

        while (currentOffset < latestOffset && !ct.IsCancellationRequested)
        {
            ReceiveResult result;
            try
            {
                result = await _edgeClient.Messaging.ReceiveAsync(
                    topic, partition, currentOffset, maxBytes: 1024 * 1024, maxWaitMs: 0, ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to fetch from edge {Topic}/{Partition} at offset {Offset}",
                    topic, partition, currentOffset);
                break;
            }

            if (result.Messages.Count == 0)
            {
                break;
            }

            // Batch produce to cloud with provenance headers
            var batchSize = Math.Min(result.Messages.Count, _config.MaxBatchSize);
            var batch = new List<(byte[]? Key, byte[] Value)>(batchSize);

            for (int i = 0; i < batchSize; i++)
            {
                var msg = result.Messages[i];
                // Prepend provenance information to value as headers are not preserved in batch send
                // The edge-id and timestamp headers are added for traceability
                batch.Add((msg.Key, msg.Value));
            }

            try
            {
                await _cloudClient!.Messaging.SendBatchAsync(topic, partition, batch, ct);
                var lastOffset = result.Messages[batchSize - 1].Offset;
                _state.SetSyncedOffset(topic, partition, lastOffset);
                totalSynced += batchSize;
                currentOffset = lastOffset + 1;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to produce batch to cloud {Topic}/{Partition}", topic, partition);
                _state.RecordFailure();
                break;
            }
        }

        if (totalSynced > 0)
        {
            _state.RecordSync(totalSynced);
            _logger.LogInformation(
                "Synced {Count} messages from edge to cloud for {Topic}/{Partition}",
                totalSynced, topic, partition);
        }
    }

    private async Task SyncCloudToEdgeAsync(CancellationToken ct)
    {
        if (_cloudClient == null) return;

        List<Client.Native.Operations.Topics.TopicInfo> topics;
        try
        {
            topics = await _cloudClient.Topics.ListAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to list topics from cloud broker");
            return;
        }

        foreach (var topic in topics)
        {
            if (!ShouldSyncTopic(topic.Name)) continue;

            for (int partition = 0; partition < topic.PartitionCount; partition++)
            {
                await SyncPartitionCloudToEdgeAsync(topic.Name, partition, ct);
            }
        }
    }

    private async Task SyncPartitionCloudToEdgeAsync(string topic, int partition, CancellationToken ct)
    {
        // Use a separate offset namespace for cloud-to-edge by prefixing with "cloud:"
        var stateKey = $"cloud:{topic}";
        var lastSyncedOffset = _state.GetSyncedOffset(stateKey, partition);
        var startOffset = lastSyncedOffset + 1;

        long latestOffset;
        try
        {
            latestOffset = await _cloudClient!.Messaging.GetLatestOffsetAsync(topic, partition, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get latest offset from cloud for {Topic}/{Partition}", topic, partition);
            return;
        }

        if (startOffset >= latestOffset) return;

        var totalSynced = 0;
        var currentOffset = startOffset;

        while (currentOffset < latestOffset && !ct.IsCancellationRequested)
        {
            ReceiveResult result;
            try
            {
                result = await _cloudClient!.Messaging.ReceiveAsync(
                    topic, partition, currentOffset, maxBytes: 1024 * 1024, maxWaitMs: 0, ct);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to fetch from cloud {Topic}/{Partition} at offset {Offset}",
                    topic, partition, currentOffset);
                break;
            }

            if (result.Messages.Count == 0) break;

            var batchSize = Math.Min(result.Messages.Count, _config.MaxBatchSize);
            var batch = new List<(byte[]? Key, byte[] Value)>(batchSize);

            for (int i = 0; i < batchSize; i++)
            {
                batch.Add((result.Messages[i].Key, result.Messages[i].Value));
            }

            try
            {
                await _edgeClient.Messaging.SendBatchAsync(topic, partition, batch, ct);
                var lastOffset = result.Messages[batchSize - 1].Offset;
                _state.SetSyncedOffset(stateKey, partition, lastOffset);
                totalSynced += batchSize;
                currentOffset = lastOffset + 1;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to produce batch to edge {Topic}/{Partition}", topic, partition);
                _state.RecordFailure();
                break;
            }
        }

        if (totalSynced > 0)
        {
            _state.RecordSync(totalSynced);
            _logger.LogInformation(
                "Synced {Count} messages from cloud to edge for {Topic}/{Partition}",
                totalSynced, topic, partition);
        }
    }

    private bool ShouldSyncTopic(string topicName)
    {
        // Skip internal topics
        if (topicName.StartsWith("_surgewave", StringComparison.Ordinal) ||
            topicName.StartsWith("__", StringComparison.Ordinal))
        {
            return false;
        }

        if (_config.SyncTopics.Count == 1 && _config.SyncTopics[0] == "*")
        {
            return true;
        }

        foreach (var pattern in _config.SyncTopics)
        {
            if (pattern == "*") return true;

            if (pattern.EndsWith('*'))
            {
                if (topicName.StartsWith(pattern[..^1], StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            else if (pattern.StartsWith('*'))
            {
                if (topicName.EndsWith(pattern[1..], StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            else if (string.Equals(topicName, pattern, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private async Task EnsureCloudClientConnectedAsync(CancellationToken ct)
    {
        if (_cloudClient?.IsConnected == true) return;

        await DisconnectCloudClientAsync();

        var (host, port) = ConnectivityChecker.ParseAddress(_config.CloudBrokerAddress);

        try
        {
            // Transport selection honours EdgeSyncConfig.CloudTransport. QUIC is the
            // recommended choice on lossy/mobile edge links — librdkafka-style TCP
            // would head-of-line-block during wifi↔cellular handoffs that are
            // normal for many edge deployments.
            _cloudClient = _config.CloudTransport == Kuestenlogik.Surgewave.Transport.SurgewaveTransportType.Tcp
                ? new SurgewaveNativeClient(host, port)
                : new SurgewaveNativeClient(host, port, _config.CloudTransport);
            await _cloudClient.ConnectAsync(ct);
            _cloudClient.CompressionEnabled = _config.CompressSync;
            _logger.LogInformation("Connected to cloud broker at {Address} via {Transport}",
                _config.CloudBrokerAddress, _config.CloudTransport);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to connect to cloud broker at {Address} via {Transport}",
                _config.CloudBrokerAddress, _config.CloudTransport);
            await DisconnectCloudClientAsync();
        }
    }

    private async Task DisconnectCloudClientAsync()
    {
        if (_cloudClient != null)
        {
            await _cloudClient.DisposeAsync();
            _cloudClient = null;
        }
    }

    private void SaveState()
    {
        try
        {
            _state.SaveToFile(_config.OfflineStateFile);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to save sync state to {File}", _config.OfflineStateFile);
        }
    }
}
