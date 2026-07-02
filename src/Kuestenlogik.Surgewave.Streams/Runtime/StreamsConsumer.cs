using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;
using TopicInfo = Kuestenlogik.Surgewave.Client.Native.Operations.Topics.TopicInfo;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Kafka consumer wrapper for Streams processing.
/// Manages subscription, polling, and offset tracking for stream tasks.
///
/// Connects lazily to the broker configured in <see cref="StreamsConfig.BootstrapServers"/>
/// via the Surgewave native protocol on the first <see cref="PollAsync"/>. All partitions
/// of the subscribed topics are self-assigned to this instance (single-instance semantics);
/// cross-instance rebalancing via the broker's group coordinator is a follow-up.
/// Offsets are committed to the consumer group named after <see cref="StreamsConfig.ApplicationId"/>.
///
/// When <see cref="SimulateAssignment"/> is used (testing/standalone mode) no broker
/// connection is established and the consumer behaves purely in-memory.
/// </summary>
internal sealed class StreamsConsumer : IDisposable
{
    private const int DefaultFetchMaxBytes = 1024 * 1024;
    private static readonly TimeSpan InitialReconnectBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxReconnectBackoff = TimeSpan.FromSeconds(30);

    private readonly StreamsConfig _config;
    private readonly ILogger _logger;
    private readonly List<string> _subscribedTopics = [];
    private readonly ConcurrentDictionary<TopicPartition, long> _currentOffsets = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _committedOffsets = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _highWatermarks = new();
    private readonly ConcurrentDictionary<TopicPartition, byte> _pausedPartitions = new();
    private readonly HashSet<string> _discoveredTopics = [];
    private readonly object _clientLock = new();

    private SurgewaveNativeClient? _client;
    private long _nextConnectAttemptTick;
    private TimeSpan _reconnectBackoff = InitialReconnectBackoff;
    private bool _connectFailureLogged;
    private bool _simulatedMode;
    private volatile bool _disposed;

    public event Action<IEnumerable<TopicPartition>>? PartitionsAssigned;
    public event Action<IEnumerable<TopicPartition>>? PartitionsRevoked;

    public IReadOnlyList<TopicPartition> Assignment => _currentOffsets.Keys.ToList();

    public StreamsConsumer(StreamsConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// The member id used for broker-side offset commits. The native group coordinator
    /// auto-creates the group on commit and does not validate member/generation for
    /// simple (non-rebalancing) offset storage.
    /// </summary>
    private string GroupMemberId => $"{_config.ApplicationId}-streams";

    /// <summary>
    /// Subscribes to a list of topics.
    /// </summary>
    public void Subscribe(IEnumerable<string> topics)
    {
        _subscribedTopics.Clear();
        _subscribedTopics.AddRange(topics);
        _discoveredTopics.Clear();
        _logger.LogInformation("Subscribed to topics: {Topics}", string.Join(", ", _subscribedTopics));
    }

    /// <summary>
    /// Polls for new records from all assigned (non-paused) partitions starting at the
    /// current offsets. Respects <paramref name="timeout"/>: if no records are available,
    /// the call waits out the remaining budget instead of busy-looping.
    /// </summary>
    public async Task<IReadOnlyList<ConsumerRecord>> PollAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        var client = _disposed || _simulatedMode
            ? null
            : await EnsureClientAsync(cancellationToken).ConfigureAwait(false);

        if (client == null)
        {
            // Test/standalone mode or broker (still) unreachable: behave like the
            // previous in-memory implementation — wait out the poll timeout.
            await Task.Delay(timeout, cancellationToken).ConfigureAwait(false);
            return [];
        }

        await MaybeDiscoverPartitionsAsync(client, cancellationToken).ConfigureAwait(false);

        var records = await FetchAssignedAsync(client, deadline, cancellationToken).ConfigureAwait(false);

        if (records.Count == 0)
        {
            var remainingMs = deadline - Environment.TickCount64;
            if (remainingMs > 0)
                await Task.Delay((int)Math.Min(remainingMs, int.MaxValue), cancellationToken).ConfigureAwait(false);
        }

        return records;
    }

    /// <summary>
    /// Seeks to a specific offset.
    /// </summary>
    public void Seek(TopicPartition partition, long offset)
    {
        _currentOffsets[partition] = offset;
        _logger.LogDebug("Seeking {Topic}-{Partition} to offset {Offset}",
            partition.Topic, partition.Partition, offset);
    }

    /// <summary>
    /// Gets the current position for a partition.
    /// </summary>
    public long Position(TopicPartition partition)
    {
        return _currentOffsets.GetValueOrDefault(partition, 0);
    }

    /// <summary>
    /// Commits offsets synchronously.
    /// </summary>
    public void CommitSync()
    {
        CommitAsync().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Commits offsets asynchronously to the consumer group named after the ApplicationId.
    /// Without a broker connection (test mode) only the in-memory tracking is updated.
    /// </summary>
    public async Task CommitAsync()
    {
        var client = _simulatedMode ? null : _client;
        var count = 0;

        foreach (var (partition, offset) in _currentOffsets)
        {
            if (client != null)
            {
                try
                {
                    await client.Groups.CommitOffsetAsync(
                        _config.ApplicationId,
                        GroupMemberId,
                        generationId: 0,
                        partition.Topic,
                        partition.Partition,
                        offset).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Failed to commit offset {Offset} for {Topic}-{Partition} to group {GroupId}",
                        offset, partition.Topic, partition.Partition, _config.ApplicationId);
                    HandleClientFailure(ex);
                    throw;
                }
            }

            _committedOffsets[partition] = offset;
            count++;
        }

        _logger.LogDebug("Committed offsets for {Count} partitions", count);
    }

    /// <summary>
    /// Commits specific offsets.
    /// </summary>
    public void Commit(IDictionary<TopicPartition, long> offsets)
    {
        var client = _simulatedMode ? null : _client;

        foreach (var (partition, offset) in offsets)
        {
            if (client != null)
            {
                try
                {
                    client.Groups.CommitOffsetAsync(
                        _config.ApplicationId,
                        GroupMemberId,
                        generationId: 0,
                        partition.Topic,
                        partition.Partition,
                        offset).GetAwaiter().GetResult();
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Failed to commit offset {Offset} for {Topic}-{Partition} to group {GroupId}",
                        offset, partition.Topic, partition.Partition, _config.ApplicationId);
                    HandleClientFailure(ex);
                    throw;
                }
            }

            _committedOffsets[partition] = offset;
            _currentOffsets[partition] = offset;
        }
    }

    /// <summary>
    /// Gets committed offset for a partition.
    /// </summary>
    public long? Committed(TopicPartition partition)
    {
        return _committedOffsets.TryGetValue(partition, out var offset) ? offset : null;
    }

    /// <summary>
    /// Gets the high watermark for a partition.
    /// </summary>
    public long GetHighWatermark(TopicPartition partition)
    {
        return _highWatermarks.GetValueOrDefault(partition, 0);
    }

    /// <summary>
    /// Updates the high watermark for a partition.
    /// </summary>
    public void UpdateHighWatermark(TopicPartition partition, long highWatermark)
    {
        _highWatermarks[partition] = highWatermark;
    }

    /// <summary>
    /// Gets the end (high watermark) offset for a topic-partition.
    /// Uses the last fetch response if available, otherwise queries the broker.
    /// Returns 0 if not available (e.g. no broker connection).
    /// </summary>
    public long GetEndOffset(TopicPartition tp)
    {
        if (_highWatermarks.TryGetValue(tp, out var cached))
            return cached;

        var client = _simulatedMode ? null : _client;
        if (client != null)
        {
            try
            {
                var latest = client.Messaging.GetLatestOffsetAsync(tp.Topic, tp.Partition).GetAwaiter().GetResult();
                _highWatermarks[tp] = latest;
                return latest;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogDebug(ex, "Failed to query end offset for {Topic}-{Partition}", tp.Topic, tp.Partition);
            }
        }

        return 0;
    }

    /// <summary>
    /// Pauses consumption from specified partitions. Paused partitions are skipped during fetches.
    /// </summary>
    public void Pause(IEnumerable<TopicPartition> partitions)
    {
        var list = partitions.ToList();
        foreach (var partition in list)
        {
            _pausedPartitions[partition] = 0;
        }
        _logger.LogDebug("Pausing partitions: {Partitions}",
            string.Join(", ", list.Select(p => $"{p.Topic}-{p.Partition}")));
    }

    /// <summary>
    /// Resumes consumption from specified partitions.
    /// </summary>
    public void Resume(IEnumerable<TopicPartition> partitions)
    {
        var list = partitions.ToList();
        foreach (var partition in list)
        {
            _pausedPartitions.TryRemove(partition, out _);
        }
        _logger.LogDebug("Resuming partitions: {Partitions}",
            string.Join(", ", list.Select(p => $"{p.Topic}-{p.Partition}")));
    }

    /// <summary>
    /// Simulates a partition assignment (for testing/standalone mode).
    /// Switches the consumer into simulated mode: no broker connection or
    /// partition discovery will be attempted.
    /// </summary>
    public void SimulateAssignment(IEnumerable<TopicPartition> partitions)
    {
        _simulatedMode = true;

        var partitionList = partitions.ToList();
        foreach (var partition in partitionList)
        {
            _currentOffsets[partition] = 0;
        }
        PartitionsAssigned?.Invoke(partitionList);
    }

    /// <summary>
    /// Simulates partition revocation (for testing/standalone mode).
    /// </summary>
    public void SimulateRevocation(IEnumerable<TopicPartition> partitions)
    {
        var partitionList = partitions.ToList();
        foreach (var partition in partitionList)
        {
            _currentOffsets.TryRemove(partition, out _);
        }
        PartitionsRevoked?.Invoke(partitionList);
    }

    public void Dispose()
    {
        SurgewaveNativeClient? client;
        lock (_clientLock)
        {
            if (_disposed)
                return;

            _disposed = true;
            client = _client;
            _client = null;
        }

        if (client != null)
        {
            try
            {
                client.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error disposing StreamsConsumer native client");
            }
        }

        _subscribedTopics.Clear();
        _currentOffsets.Clear();
        _committedOffsets.Clear();
        _highWatermarks.Clear();
        _pausedPartitions.Clear();
    }

    /// <summary>
    /// Lazily establishes the native client connection. Failures are logged honestly
    /// (first failure at Warning, retries at Debug) and retried with exponential backoff;
    /// while disconnected the consumer keeps functioning without records.
    /// </summary>
    private async Task<SurgewaveNativeClient?> EnsureClientAsync(CancellationToken cancellationToken)
    {
        var existing = _client;
        if (existing != null)
            return existing;

        if (_disposed || Environment.TickCount64 < Interlocked.Read(ref _nextConnectAttemptTick))
            return null;

        var (host, port) = ParseBootstrapServers(_config.BootstrapServers);
        var candidate = new SurgewaveNativeClient(host, port, SurgewaveTransportType.Auto);
        try
        {
            await candidate.ConnectAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await candidate.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await candidate.DisposeAsync().ConfigureAwait(false);
            Interlocked.Exchange(ref _nextConnectAttemptTick,
                Environment.TickCount64 + (long)_reconnectBackoff.TotalMilliseconds);

            if (!_connectFailureLogged)
            {
                _connectFailureLogged = true;
                _logger.LogWarning(ex,
                    "StreamsConsumer failed to connect to {BootstrapServers}; retrying with backoff (up to {MaxBackoff})",
                    _config.BootstrapServers, MaxReconnectBackoff);
            }
            else
            {
                _logger.LogDebug(ex, "StreamsConsumer reconnect to {BootstrapServers} failed",
                    _config.BootstrapServers);
            }

            _reconnectBackoff = TimeSpan.FromMilliseconds(Math.Min(
                _reconnectBackoff.TotalMilliseconds * 2, MaxReconnectBackoff.TotalMilliseconds));
            return null;
        }

        lock (_clientLock)
        {
            if (_disposed)
            {
                candidate.DisposeAsync().AsTask().GetAwaiter().GetResult();
                return null;
            }
            _client = candidate;
        }

        _connectFailureLogged = false;
        _reconnectBackoff = InitialReconnectBackoff;
        _logger.LogInformation("StreamsConsumer connected to {BootstrapServers} (native protocol)",
            _config.BootstrapServers);
        return candidate;
    }

    /// <summary>
    /// Discovers partitions for subscribed topics that are not yet assigned and
    /// self-assigns all of them (single-instance semantics). Start offsets come from
    /// the committed offsets of the group (ApplicationId) if present, otherwise from
    /// AutoOffsetReset ("earliest" → log start, "latest" → high watermark).
    /// Topics that do not exist yet are retried on subsequent polls.
    /// </summary>
    private async Task MaybeDiscoverPartitionsAsync(SurgewaveNativeClient client, CancellationToken cancellationToken)
    {
        if (_simulatedMode || _subscribedTopics.Count == 0)
            return;

        var missing = _subscribedTopics.Where(t => !_discoveredTopics.Contains(t)).ToList();
        if (missing.Count == 0)
            return;

        List<TopicInfo> topicInfos;
        try
        {
            // Trigger broker-side auto-creation for topics that don't exist yet
            // (same mechanism as SurgewaveConsumer: ListOffsets auto-creates).
            foreach (var topic in missing)
            {
                try
                {
                    await client.Messaging.GetEarliestOffsetAsync(topic, 0, cancellationToken).ConfigureAwait(false);
                }
                catch (ProtocolException)
                {
                    // Auto-creation disabled — the topic may appear later; retried on next poll.
                }
            }

            topicInfos = await client.Topics.ListAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            HandleClientFailure(ex);
            return;
        }

        var assigned = new List<TopicPartition>();

        foreach (var topic in missing)
        {
            var info = topicInfos.FirstOrDefault(t => t.Name == topic);
            if (info == null)
            {
                _logger.LogDebug("Topic {Topic} not found on broker yet; will retry discovery", topic);
                continue;
            }

            // Resolve all partition start offsets first so a mid-topic failure
            // does not leave a half-assigned topic behind.
            var resolved = new List<(TopicPartition Tp, long Offset)>(info.PartitionCount);
            try
            {
                for (var partition = 0; partition < info.PartitionCount; partition++)
                {
                    var tp = new TopicPartition(topic, partition);
                    var startOffset = await ResolveStartOffsetAsync(client, tp, cancellationToken).ConfigureAwait(false);
                    resolved.Add((tp, startOffset));
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                HandleClientFailure(ex);
                break;
            }

            foreach (var (tp, offset) in resolved)
            {
                _currentOffsets[tp] = offset;
                assigned.Add(tp);
            }
            _discoveredTopics.Add(topic);
        }

        if (assigned.Count > 0)
        {
            _logger.LogInformation(
                "Self-assigned {Count} partition(s) (single-instance mode): {Partitions}",
                assigned.Count,
                string.Join(", ", assigned.Select(p => $"{p.Topic}-{p.Partition}")));
            PartitionsAssigned?.Invoke(assigned);
        }
    }

    /// <summary>
    /// Determines the start offset for a newly assigned partition: the group's committed
    /// offset if valid, otherwise the AutoOffsetReset position.
    /// </summary>
    private async Task<long> ResolveStartOffsetAsync(
        SurgewaveNativeClient client,
        TopicPartition tp,
        CancellationToken cancellationToken)
    {
        long committed = -1;
        try
        {
            committed = await client.Groups.FetchOffsetAsync(
                _config.ApplicationId, tp.Topic, tp.Partition, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Failed to fetch committed offset for {Topic}-{Partition} (group {GroupId})",
                tp.Topic, tp.Partition, _config.ApplicationId);
        }

        if (committed >= 0)
        {
            // Guard against stale committed offsets beyond the current log end
            // (mirrors SurgewaveConsumer's out-of-range handling).
            var latest = await client.Messaging.GetLatestOffsetAsync(tp.Topic, tp.Partition, cancellationToken).ConfigureAwait(false);
            if (committed <= latest)
            {
                _committedOffsets[tp] = committed;
                _logger.LogDebug("Resuming {Topic}-{Partition} from committed offset {Offset}",
                    tp.Topic, tp.Partition, committed);
                return committed;
            }

            _logger.LogWarning(
                "Committed offset {Committed} for {Topic}-{Partition} is beyond log end {Latest}; applying AutoOffsetReset={Reset}",
                committed, tp.Topic, tp.Partition, latest, _config.AutoOffsetReset);
        }

        return string.Equals(_config.AutoOffsetReset, "latest", StringComparison.OrdinalIgnoreCase)
            ? await client.Messaging.GetLatestOffsetAsync(tp.Topic, tp.Partition, cancellationToken).ConfigureAwait(false)
            : await client.Messaging.GetEarliestOffsetAsync(tp.Topic, tp.Partition, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Fetches records for all assigned, non-paused partitions starting at the current
    /// offsets, advances the offsets, and updates the high watermarks.
    /// </summary>
    private async Task<List<ConsumerRecord>> FetchAssignedAsync(
        SurgewaveNativeClient client,
        long deadline,
        CancellationToken cancellationToken)
    {
        var records = new List<ConsumerRecord>();

        var active = new List<TopicPartition>();
        foreach (var tp in _currentOffsets.Keys)
        {
            if (!_pausedPartitions.ContainsKey(tp))
                active.Add(tp);
        }

        if (active.Count == 0)
            return records;

        foreach (var tp in active)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!_currentOffsets.TryGetValue(tp, out var fetchOffset) || fetchOffset < 0)
                continue; // revoked meanwhile or position not resolved

            // Single partition: long-poll with the remaining budget for low latency.
            // Multiple partitions: sweep without waiting so no partition is starved;
            // the caller waits out the remaining timeout when nothing was fetched.
            var maxWaitMs = 0;
            if (active.Count == 1)
            {
                var remaining = deadline - Environment.TickCount64;
                maxWaitMs = (int)Math.Clamp(remaining, 0, int.MaxValue);
            }

            try
            {
                var result = await client.Messaging.ReceiveAsync(
                    tp.Topic, tp.Partition, fetchOffset,
                    DefaultFetchMaxBytes, maxWaitMs, cancellationToken).ConfigureAwait(false);

                _highWatermarks[tp] = result.HighWatermark;

                if (result.Messages.Count == 0)
                {
                    if (result.HighWatermark > fetchOffset)
                    {
                        // Requested range may have been deleted by retention:
                        // advance to the log start if it moved past our position.
                        var logStart = await client.Messaging.GetEarliestOffsetAsync(
                            tp.Topic, tp.Partition, cancellationToken).ConfigureAwait(false);
                        if (logStart > fetchOffset)
                        {
                            _logger.LogWarning(
                                "Offset {Offset} for {Topic}-{Partition} is below log start {LogStart}; advancing",
                                fetchOffset, tp.Topic, tp.Partition, logStart);
                            _currentOffsets[tp] = logStart;
                        }
                    }
                    continue;
                }

                var nextOffset = fetchOffset;
                foreach (var msg in result.Messages)
                {
                    // The broker may return a batch starting before the requested offset.
                    if (msg.Offset < fetchOffset)
                        continue;

                    records.Add(new ConsumerRecord(
                        tp.Topic, tp.Partition, msg.Offset, msg.Timestamp, msg.Key ?? [], msg.Value));
                    nextOffset = msg.Offset + 1;
                }

                if (nextOffset > fetchOffset)
                    _currentOffsets[tp] = nextOffset;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (ProtocolException ex)
            {
                // Partition-level broker error (e.g. topic deleted) — log and keep polling others.
                _logger.LogDebug(ex, "Fetch failed for {Topic}-{Partition}: {Error}",
                    tp.Topic, tp.Partition, ex.ErrorCode);
            }
            catch (Exception ex)
            {
                HandleClientFailure(ex);
                break;
            }
        }

        return records;
    }

    /// <summary>
    /// Drops the current client after a connection-level failure so the next poll
    /// reconnects with backoff.
    /// </summary>
    private void HandleClientFailure(Exception ex)
    {
        SurgewaveNativeClient? client;
        lock (_clientLock)
        {
            client = _client;
            _client = null;
        }

        Interlocked.Exchange(ref _nextConnectAttemptTick,
            Environment.TickCount64 + (long)_reconnectBackoff.TotalMilliseconds);

        if (client != null)
        {
            _logger.LogWarning(ex, "StreamsConsumer lost connection to {BootstrapServers}; reconnecting with backoff",
                _config.BootstrapServers);
            try
            {
                client.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // Best effort — connection is already broken.
            }
        }
    }

    private static (string Host, int Port) ParseBootstrapServers(string servers)
    {
        // Use the first entry of a comma-separated list (single-broker native protocol).
        var first = servers.Split(',')[0].Trim();
        var parts = first.Split(':');
        return (parts[0], parts.Length > 1 && int.TryParse(parts[1], out var port) ? port : 9092);
    }
}

/// <summary>
/// Represents a consumed record.
/// </summary>
public readonly record struct ConsumerRecord(
    string Topic,
    int Partition,
    long Offset,
    long Timestamp,
    byte[] Key,
    byte[] Value);
