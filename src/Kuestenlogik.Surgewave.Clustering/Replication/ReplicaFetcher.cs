using System.Buffers;
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

            // Send request and get response. The response owns the pooled body its record slices point
            // into; dispose it only after ProcessFetchResponseAsync has appended every batch.
            using var response = await connection.SendFetchRequestAsync(fetchRequest, ct);

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
        var baseOffset = BinaryPrimitives.ReadInt64BigEndian(data.Span[..8]);

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

internal sealed class ReplicaFetchResponse : IDisposable
{
    private byte[]? _pooledBuffer;

    public int ThrottleTimeMs { get; set; }
    public List<TopicResponse> Topics { get; set; } = [];

    /// <summary>
    /// Take ownership of the pooled response body that the partition <see cref="PartitionResponse.RecordBatch"/>
    /// slices point into. It is returned to the pool on <see cref="Dispose"/> — i.e. only after the caller
    /// has finished appending the fetched batches, so the slices stay valid for the whole append.
    /// </summary>
    public void AttachPooledBuffer(byte[] buffer) => _pooledBuffer = buffer;

    public void Dispose()
    {
        var buffer = _pooledBuffer;
        _pooledBuffer = null;
        if (buffer is not null)
            ArrayPool<byte>.Shared.Return(buffer);
    }

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
        public ReadOnlyMemory<byte> RecordBatch { get; set; }
    }
}

/// <summary>
/// Connection to a leader broker for fetching data. Rides on top of
/// <see cref="IPeerTransport"/> so TCP / QUIC / future transports can be
/// selected via configuration.
/// </summary>
internal sealed class LeaderConnection : IAsyncDisposable
{
    /// <summary>Upper bound on a fetch response body, mirroring the leader's own frame cap.</summary>
    private const int MaxResponseBytes = 100 * 1024 * 1024;

    private readonly BrokerNode _broker;
    private readonly IPeerTransport _peerTransport;
    private readonly ILogger _logger;
    // Reused per connection: AcquireStreamAsync serializes reads, so exactly one fetch response is
    // being read at a time and the 4-byte size prefix never needs a fresh allocation.
    private readonly byte[] _sizeBuffer = new byte[4];
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

        var (requestBuffer, requestLength) = SerializeFetchRequest(request, correlationId);
        await lease.Stream.WriteAsync(requestBuffer.AsMemory(0, requestLength), ct);
        await lease.Stream.FlushAsync(ct);
        // Return the pooled request buffer ONLY after a fully successful write+flush; on cancel/fault the
        // throw skips this and the rent drops to GC (same reasoning as the leader's WriteResponseAsync —
        // a canceled-but-still-draining send may pin the buffer, so a finally-return could hand it to the
        // next renter mid-send and corrupt the wire).
        ArrayPool<byte>.Shared.Return(requestBuffer);

        var (body, length) = await ReadResponseAsync(lease.Stream, ct);
        ReplicaFetchResponse response;
        try
        {
            response = ParseFetchResponse(body, length);
        }
        catch
        {
            // Parse threw before the response could take ownership of the pooled body — return it here
            // so the rent is not leaked.
            ArrayPool<byte>.Shared.Return(body);
            throw;
        }

        // Ownership transfers to the response: each partition's records are now a slice INTO this pooled
        // body, so the buffer must stay alive until the response is disposed — which the caller does only
        // after the fetched batches have been appended.
        response.AttachPooledBuffer(body);
        return response;
    }

    // Fixed fetch-REQUEST body bytes, excluding the 4-byte size prefix and the variable topic section:
    // header (apiKey 2 + apiVer 2 + corrId 4 + clientId 2 + replicaId 4 + maxWait 4 + minBytes 4 +
    // maxBytes 4 + isolation 1 + sessionId 4 + sessionEpoch 4 + topicCount 4 = 39) + trailer
    // (forgottenTopics 4 + rackId 2 = 6).
    private const int FetchRequestFixedBytes = 39 + 6;
    // Per-partition fetch-REQUEST width: partition 4 + currentLeaderEpoch 4 + fetchOffset 8 +
    // logStartOffset 8 + partitionMaxBytes 4. (Distinct from the response's FetchPartitionFixedBytes.)
    private const int FetchRequestPartitionBytes = 4 + 4 + 8 + 8 + 4;

    /// <summary>
    /// Two-pass exact-size serialization of a fetch request into a pooled buffer — replaces the old
    /// MemoryStream + per-topic GetBytes + ToArray path. Pass 1 computes the exact size; pass 2 writes
    /// big-endian directly via <see cref="BinaryPrimitives"/>. The returned buffer is rented from
    /// <see cref="ArrayPool{T}"/> and may be larger than <c>TotalLength</c>; the caller sends exactly
    /// <c>TotalLength</c> bytes and returns the buffer to the pool after a successful send. Wire bytes are
    /// byte-identical to the previous BinaryWriter path on a little-endian host.
    /// <c>static</c> + no instance state so the round-trip test can reach it (InternalsVisibleTo).
    /// </summary>
    internal static (byte[] Buffer, int TotalLength) SerializeFetchRequest(ReplicaFetchRequest request, int correlationId)
    {
        // Pass 1 — exact body size (excludes the 4-byte size prefix).
        var size = FetchRequestFixedBytes;
        foreach (var topic in request.Topics)
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic.Topic) + 4
                  + FetchRequestPartitionBytes * topic.Partitions.Count;

        var total = 4 + size;
        var buffer = ArrayPool<byte>.Shared.Rent(total);
        var span = buffer.AsSpan();
        var o = 0;

        // Pass 2 — write into the exact-size frame.
        BinaryPrimitives.WriteInt32BigEndian(span[o..], size); o += 4;             // size prefix
        BinaryPrimitives.WriteInt16BigEndian(span[o..], 1); o += 2;                // api key (Fetch)
        BinaryPrimitives.WriteInt16BigEndian(span[o..], 11); o += 2;               // api version
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId); o += 4;
        BinaryPrimitives.WriteInt16BigEndian(span[o..], -1); o += 2;               // client id (null string)
        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.ReplicaId); o += 4;
        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.MaxWaitMs); o += 4;
        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.MinBytes); o += 4;
        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.MaxBytes); o += 4;
        span[o] = request.IsolationLevel; o += 1;                                  // isolation level (raw byte)
        BinaryPrimitives.WriteInt32BigEndian(span[o..], 0); o += 4;                // session id
        BinaryPrimitives.WriteInt32BigEndian(span[o..], -1); o += 4;               // session epoch
        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.Topics.Count); o += 4;

        foreach (var topic in request.Topics)
        {
            var nameLen = System.Text.Encoding.UTF8.GetByteCount(topic.Topic);
            BinaryPrimitives.WriteInt16BigEndian(span[o..], (short)nameLen); o += 2;
            System.Text.Encoding.UTF8.GetBytes(topic.Topic, span.Slice(o, nameLen)); o += nameLen;
            BinaryPrimitives.WriteInt32BigEndian(span[o..], topic.Partitions.Count); o += 4;
            foreach (var partition in topic.Partitions)
            {
                BinaryPrimitives.WriteInt32BigEndian(span[o..], partition.Partition); o += 4;
                BinaryPrimitives.WriteInt32BigEndian(span[o..], -1); o += 4;         // current leader epoch
                BinaryPrimitives.WriteInt64BigEndian(span[o..], partition.FetchOffset); o += 8;
                BinaryPrimitives.WriteInt64BigEndian(span[o..], -1L); o += 8;        // log start offset
                BinaryPrimitives.WriteInt32BigEndian(span[o..], partition.PartitionMaxBytes); o += 4;
            }
        }

        BinaryPrimitives.WriteInt32BigEndian(span[o..], 0); o += 4;                // forgotten topics
        BinaryPrimitives.WriteInt16BigEndian(span[o..], -1); o += 2;               // rack id

        System.Diagnostics.Debug.Assert(o == total, "fetch-request pass-1 size disagrees with pass-2 writes");
        return (buffer, o);
    }

    private async Task<(byte[] Buffer, int Length)> ReadResponseAsync(Stream stream, CancellationToken ct)
    {
        await stream.ReadExactlyAsync(_sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(_sizeBuffer);
        if (size <= 0 || size > MaxResponseBytes)
            throw new InvalidDataException($"Replication fetch response size {size} is out of range (0, {MaxResponseBytes}].");

        // Rent the body instead of allocating it: for a real fetch this is routinely >=1MB (an LOH
        // allocation per RPC). The rent may be larger than `size`; the caller parses exactly `size`
        // bytes and returns the buffer to the pool once parsing completes.
        var body = ArrayPool<byte>.Shared.Rent(size);
        await stream.ReadExactlyAsync(body.AsMemory(0, size), ct);

        return (body, size);
    }

    private ReplicaFetchResponse ParseFetchResponse(byte[] data, int length)
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
                    // Bound the record slice against the actual response length: the body is now a
                    // (possibly larger) pooled rent, so a malformed over-claim must not read past the
                    // real bytes into the rented tail.
                    if (recordsLen > length - offset)
                        throw new InvalidDataException($"Replication fetch response truncated: record batch of {recordsLen} bytes exceeds the {length}-byte body at offset {offset}.");
                    // Slice into the pooled body — no copy. The body outlives parsing (returned to the
                    // pool only when the response is disposed, after the batches are appended).
                    partitionResponse.RecordBatch = new ReadOnlyMemory<byte>(data, offset, recordsLen);
                    offset += recordsLen;
                }
                else
                {
                    partitionResponse.RecordBatch = ReadOnlyMemory<byte>.Empty;
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
