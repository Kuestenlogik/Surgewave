using System.Buffers.Binary;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Fetches data from leader replicas for partitions where this broker is a follower.
/// </summary>
public sealed partial class ReplicaFetcher : IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly ClusterState _clusterState;
    private readonly LogManager _logManager;
    private readonly ReplicaManager _replicaManager;
    private readonly ClusteringConfig _config;
    private readonly IPeerTransport _peerTransport;

    private readonly ConcurrentDictionary<int, LeaderConnection> _leaderConnections = new();
    private readonly ConcurrentDictionary<TopicPartition, FetchState> _fetchStates = new();

    private CancellationTokenSource? _cts;
    private Task? _fetchLoopTask;

    public TimeSpan FetchInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public int MaxFetchBytes { get; set; } = 1024 * 1024; // 1MB per fetch

    public ReplicaFetcher(
        ILogger logger,
        ClusterState clusterState,
        LogManager logManager,
        ReplicaManager replicaManager,
        ClusteringConfig config,
        IPeerTransport peerTransport)
    {
        _logger = logger;
        _clusterState = clusterState;
        _logManager = logManager;
        _replicaManager = replicaManager;
        _config = config;
        _peerTransport = peerTransport;
    }

    public Task StartAsync(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _fetchLoopTask = Task.Run(() => FetchLoopAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_fetchLoopTask != null)
        {
            try { await _fetchLoopTask; } catch (OperationCanceledException) { }
        }

        foreach (var conn in _leaderConnections.Values)
        {
            await conn.DisposeAsync();
        }

        _leaderConnections.Clear();
        _cts?.Dispose();
    }

    public void StartFetching(TopicPartition tp, int leaderId)
    {
        var state = new FetchState
        {
            TopicPartition = tp,
            LeaderId = leaderId,
            FetchOffset = GetLocalLogEndOffset(tp),
            IsActive = true
        };

        _fetchStates[tp] = state;
        LogStartedFetching(tp.Topic, tp.Partition, leaderId, state.FetchOffset);
    }

    public void StopFetching(TopicPartition tp)
    {
        if (_fetchStates.TryRemove(tp, out var state))
        {
            state.IsActive = false;
            LogStoppedFetching(tp.Topic, tp.Partition);
        }
    }

    private long GetLocalLogEndOffset(TopicPartition tp)
    {
        var replica = _replicaManager.GetReplica(tp);
        return replica?.LogEndOffset ?? 0;
    }

    private async Task FetchLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(FetchInterval, ct);

                // Group partitions by leader
                var partitionsByLeader = _fetchStates.Values
                    .Where(s => s.IsActive)
                    .GroupBy(s => s.LeaderId)
                    .ToList();

                // Fetch from each leader in parallel
                var fetchTasks = partitionsByLeader
                    .Select(g => FetchFromLeaderAsync(g.Key, g.ToList(), ct))
                    .ToList();

                await Task.WhenAll(fetchTasks);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogFetchLoopError(ex);
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
    }

    private async Task FetchFromLeaderAsync(int leaderId, List<FetchState> partitions, CancellationToken ct)
    {
        try
        {
            var connection = await GetOrCreateConnectionAsync(leaderId, ct);
            if (connection == null)
                return;

            // Build fetch request
            var fetchRequest = BuildFetchRequest(partitions);

            // Send request and get response
            var response = await connection.SendFetchRequestAsync(fetchRequest, ct);

            // Process response
            await ProcessFetchResponseAsync(partitions, response, ct);
        }
        catch (Exception ex)
        {
            LogFetchFromLeaderError(leaderId, ex);

            // Remove connection on error
            if (_leaderConnections.TryRemove(leaderId, out var conn))
            {
                await conn.DisposeAsync();
            }
        }
    }

    private ReplicaFetchRequest BuildFetchRequest(List<FetchState> partitions)
    {
        var topics = partitions
            .GroupBy(p => p.TopicPartition.Topic)
            .Select(g => new ReplicaFetchRequest.TopicData
            {
                Topic = g.Key,
                Partitions = g.Select(p => new ReplicaFetchRequest.PartitionData
                {
                    Partition = p.TopicPartition.Partition,
                    FetchOffset = p.FetchOffset,
                    PartitionMaxBytes = MaxFetchBytes
                }).ToList()
            })
            .ToList();

        return new ReplicaFetchRequest
        {
            ReplicaId = _config.BrokerId,
            MaxWaitMs = 500,
            MinBytes = 1,
            MaxBytes = MaxFetchBytes * partitions.Count,
            IsolationLevel = 0, // READ_UNCOMMITTED for replication
            Topics = topics
        };
    }

    private async Task ProcessFetchResponseAsync(List<FetchState> partitions, ReplicaFetchResponse response, CancellationToken ct)
    {
        foreach (var topicResponse in response.Topics)
        {
            foreach (var partitionResponse in topicResponse.Partitions)
            {
                var tp = new TopicPartition
                {
                    Topic = topicResponse.Topic,
                    Partition = partitionResponse.Partition
                };

                if (!_fetchStates.TryGetValue(tp, out var state))
                    continue;

                if (partitionResponse.ErrorCode != 0)
                {
                    LogFetchPartitionError(tp.Topic, tp.Partition, partitionResponse.ErrorCode);
                    continue;
                }

                // Append fetched data to local log
                if (partitionResponse.RecordBatch.Length > 0)
                {
                    await AppendFetchedDataAsync(tp, state, partitionResponse, ct);
                }

                // Update high watermark from leader
                state.LeaderHighWatermark = partitionResponse.HighWatermark;
            }
        }
    }

    private async Task AppendFetchedDataAsync(
        TopicPartition tp,
        FetchState state,
        ReplicaFetchResponse.PartitionResponse partitionResponse,
        CancellationToken ct)
    {
        var data = partitionResponse.RecordBatch;
        if (data.Length < 12)
            return;

        // Parse base offset from record batch
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(0, 8));

        // Append to local log. AppendAsync now returns the log-end offset after
        // the whole batch (baseOffset + recordCount), which is exactly the next
        // offset to fetch — so no +1 here, otherwise multi-record batches would
        // skip records and the follower could never catch the leader (#69).
        var appendedOffset = await _replicaManager.AppendAsync(tp, data, ct);

        // Update fetch offset for next request
        state.FetchOffset = appendedOffset;
        state.LastFetchTime = DateTimeOffset.UtcNow;

        // Notify replica manager that we fetched up to this offset
        // (leader uses this to track ISR)
        var partitionState = _clusterState.GetPartitionState(tp);
        if (partitionState != null)
        {
            _replicaManager.UpdateFollowerFetchPosition(tp, _config.BrokerId, state.FetchOffset);
        }

        LogFetchedData(tp.Topic, tp.Partition, baseOffset, data.Length);
    }

    private async Task<LeaderConnection?> GetOrCreateConnectionAsync(int leaderId, CancellationToken ct)
    {
        if (_leaderConnections.TryGetValue(leaderId, out var existing) && existing.IsConnected)
            return existing;

        var broker = _clusterState.GetBroker(leaderId);
        if (broker == null)
        {
            LogBrokerNotFound(leaderId);
            return null;
        }

        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var connection = new LeaderConnection(broker, _peerTransport, _logger);
                await connection.ConnectAsync(ct);

                _leaderConnections[leaderId] = connection;
                return connection;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception ex)
            {
                if (attempt < maxRetries)
                {
                    LogConnectionRetry(leaderId, attempt);
                    await Task.Delay(delayMs, ct);
                    delayMs *= 2;
                }
                else
                {
                    LogConnectionFailed(leaderId, ex);
                }
            }
        }

        return null;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Started fetching {Topic}-{Partition} from leader {LeaderId} at offset {Offset}")]
    private partial void LogStartedFetching(string topic, int partition, int leaderId, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stopped fetching {Topic}-{Partition}")]
    private partial void LogStoppedFetching(string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in fetch loop")]
    private partial void LogFetchLoopError(Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error fetching from leader {LeaderId}")]
    private partial void LogFetchFromLeaderError(int leaderId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Fetch error for {Topic}-{Partition}: {ErrorCode}")]
    private partial void LogFetchPartitionError(string topic, int partition, short errorCode);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fetched {Topic}-{Partition} offset={BaseOffset} size={Size}")]
    private partial void LogFetchedData(string topic, int partition, long baseOffset, int size);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} not found in cluster state")]
    private partial void LogBrokerNotFound(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connection to leader {LeaderId} failed on attempt {Attempt}, retrying")]
    private partial void LogConnectionRetry(int leaderId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to connect to leader {LeaderId} after retries")]
    private partial void LogConnectionFailed(int leaderId, Exception ex);
}

internal sealed class FetchState
{
    public required TopicPartition TopicPartition { get; init; }
    public int LeaderId { get; set; }
    public long FetchOffset { get; set; }
    public long LeaderHighWatermark { get; set; }
    public DateTimeOffset LastFetchTime { get; set; }
    public bool IsActive { get; set; }
}

// Internal types for inter-broker replication protocol
internal sealed class ReplicaFetchRequest
{
    public int ReplicaId { get; set; }
    public int MaxWaitMs { get; set; }
    public int MinBytes { get; set; }
    public int MaxBytes { get; set; }
    public byte IsolationLevel { get; set; }
    public List<TopicData> Topics { get; set; } = [];

    public sealed class TopicData
    {
        public required string Topic { get; set; }
        public List<PartitionData> Partitions { get; set; } = [];
    }

    public sealed class PartitionData
    {
        public int Partition { get; set; }
        public long FetchOffset { get; set; }
        public int PartitionMaxBytes { get; set; }
    }
}

internal sealed class ReplicaFetchResponse
{
    public int ThrottleTimeMs { get; set; }
    public List<TopicResponse> Topics { get; set; } = [];

    public sealed class TopicResponse
    {
        public required string Topic { get; set; }
        public List<PartitionResponse> Partitions { get; set; } = [];
    }

    public sealed class PartitionResponse
    {
        public int Partition { get; set; }
        public short ErrorCode { get; set; }
        public long HighWatermark { get; set; }
        public long LastStableOffset { get; set; }
        public long LogStartOffset { get; set; }
        public byte[] RecordBatch { get; set; } = [];
    }
}

/// <summary>
/// Connection to a leader broker for fetching data. Rides on top of
/// <see cref="IPeerTransport"/> so TCP / QUIC / future transports can be
/// selected via configuration.
/// </summary>
internal sealed class LeaderConnection : IAsyncDisposable
{
    private readonly BrokerNode _broker;
    private readonly IPeerTransport _peerTransport;
    private readonly ILogger _logger;
    private IPeerConnection? _connection;
    private int _correlationId;

    public bool IsConnected => _connection?.IsConnected == true;

    public LeaderConnection(BrokerNode broker, IPeerTransport peerTransport, ILogger logger)
    {
        _broker = broker;
        _peerTransport = peerTransport;
        _logger = logger;
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        _connection = await _peerTransport.ConnectAsync(_broker.Host, _broker.ReplicationPort, ct);
    }

    public async Task<ReplicaFetchResponse> SendFetchRequestAsync(ReplicaFetchRequest request, CancellationToken ct)
    {
        if (_connection == null)
            throw new InvalidOperationException("Not connected");

        await using var lease = await _connection.AcquireStreamAsync(ct);
        var correlationId = Interlocked.Increment(ref _correlationId);

        var requestBytes = SerializeFetchRequest(request, correlationId);

        await lease.Stream.WriteAsync(requestBytes, ct);
        await lease.Stream.FlushAsync(ct);

        var responseBytes = await ReadResponseAsync(lease.Stream, ct);

        return ParseFetchResponse(responseBytes);
    }

    private byte[] SerializeFetchRequest(ReplicaFetchRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Placeholder for size
        writer.Write(0);

        // API Key (Fetch = 1)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)1));
        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)11));
        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        // Client ID (nullable string)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // Replica ID
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ReplicaId));
        // Max Wait Ms
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MaxWaitMs));
        // Min Bytes
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MinBytes));
        // Max Bytes
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MaxBytes));
        // Isolation Level
        writer.Write((byte)request.IsolationLevel);
        // Session ID
        writer.Write(BinaryPrimitives.ReverseEndianness(0));
        // Session Epoch
        writer.Write(BinaryPrimitives.ReverseEndianness(-1));

        // Topics array
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Topics.Count));
        foreach (var topic in request.Topics)
        {
            // Topic name
            var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic.Topic);
            writer.Write(BinaryPrimitives.ReverseEndianness((short)topicBytes.Length));
            writer.Write(topicBytes);

            // Partitions
            writer.Write(BinaryPrimitives.ReverseEndianness(topic.Partitions.Count));
            foreach (var partition in topic.Partitions)
            {
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.Partition));
                writer.Write(BinaryPrimitives.ReverseEndianness(-1)); // Current leader epoch
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.FetchOffset));
                writer.Write(BinaryPrimitives.ReverseEndianness(-1L)); // Log start offset
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.PartitionMaxBytes));
            }
        }

        // Forgotten topics
        writer.Write(BinaryPrimitives.ReverseEndianness(0));
        // Rack ID
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        var data = ms.ToArray();
        var size = data.Length - 4;
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(0, 4), size);

        return data;
    }

    private static async Task<byte[]> ReadResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        return body;
    }

    private ReplicaFetchResponse ParseFetchResponse(byte[] data)
    {
        var response = new ReplicaFetchResponse { Topics = [] };
        var offset = 0;

        // Correlation ID
        _ = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Throttle time
        response.ThrottleTimeMs = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Error code
        _ = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;

        // Session ID
        _ = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Topics array
        var topicCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        for (int i = 0; i < topicCount; i++)
        {
            var topicResponse = new ReplicaFetchResponse.TopicResponse { Topic = "", Partitions = [] };

            // Topic name
            var topicLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            topicResponse.Topic = System.Text.Encoding.UTF8.GetString(data, offset, topicLen);
            offset += topicLen;

            // Partitions
            var partitionCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionResponse = new ReplicaFetchResponse.PartitionResponse();

                partitionResponse.Partition = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                partitionResponse.ErrorCode = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
                offset += 2;

                partitionResponse.HighWatermark = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;

                partitionResponse.LastStableOffset = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;

                partitionResponse.LogStartOffset = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;

                // Aborted transactions
                var abortedCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;
                // Skip for now
                offset += abortedCount * 16;

                // Preferred read replica
                _ = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                // Record batch
                var recordsLen = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                if (recordsLen > 0)
                {
                    partitionResponse.RecordBatch = data.AsSpan(offset, recordsLen).ToArray();
                    offset += recordsLen;
                }
                else
                {
                    partitionResponse.RecordBatch = [];
                }

                topicResponse.Partitions.Add(partitionResponse);
            }

            response.Topics.Add(topicResponse);
        }

        return response;
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}
