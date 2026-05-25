using System.Buffers.Binary;
using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// Transport for Raft RPC messages. Rides on top of <see cref="IPeerTransport"/>
/// so the underlying bytes can flow over TCP or QUIC (or any future transport)
/// depending on <see cref="ClusteringConfig.InterBrokerTransport"/>.
/// </summary>
public sealed partial class RaftTransport : IRaftTransport, IAsyncDisposable
{
    private const short RequestVoteApiKey = 101;
    private const short AppendEntriesApiKey = 102;
    private const short PreVoteApiKey = 104; // After MetadataUpdate (103)

    private readonly ILogger<RaftTransport> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly IPeerTransport _peerTransport;
    private readonly ConcurrentDictionary<int, IPeerConnection> _connections = new();
    private int _correlationId;

    public RaftTransport(
        ILogger<RaftTransport> logger,
        ClusterState clusterState,
        ClusteringConfig config,
        IPeerTransport peerTransport)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config;
        _peerTransport = peerTransport;
    }

    public IReadOnlyList<int> GetPeerIds()
    {
        return _clusterState.Brokers.Keys
            .Where(id => id != _config.BrokerId)
            .ToList();
    }

    public async Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(500); // Quick check, 500ms timeout

            var connection = await GetOrCreateConnectionAsync(peerId, cts.Token);
            return connection?.IsConnected == true;
        }
        catch
        {
            return false;
        }
    }

    public async Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
    {
        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var connection = await GetOrCreateConnectionAsync(peerId, ct);
                if (connection == null)
                {
                    if (attempt < maxRetries)
                    {
                        await Task.Delay(delayMs, ct);
                        delayMs *= 2;
                        continue;
                    }
                    throw new RaftTransportException($"Cannot connect to peer {peerId}");
                }

                var correlationId = Interlocked.Increment(ref _correlationId);
                var requestBytes = SerializePreVote(request, correlationId);

                // AcquireStreamAsync: QUIC opens a fresh bidi stream per RPC
                // (independent flow control, no head-of-line blocking); TCP
                // holds a connection-level lock and reuses the single stream.
                await using var lease = await connection.AcquireStreamAsync(ct);
                var stream = lease.Stream;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(5000);

                await stream.WriteAsync(requestBytes, cts.Token);
                await stream.FlushAsync(cts.Token);

                return await ReadPreVoteResponseAsync(stream, cts.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxRetries)
            {
                LogPreVoteRetry(peerId, attempt, ex);
                await RemoveConnectionAsync(peerId);
                await Task.Delay(delayMs, ct);
                delayMs *= 2;
            }
        }

        throw new RaftTransportException($"Failed to send PreVote to peer {peerId} after {maxRetries} attempts");
    }

    public async Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
    {
        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var connection = await GetOrCreateConnectionAsync(peerId, ct);
                if (connection == null)
                {
                    if (attempt < maxRetries)
                    {
                        await Task.Delay(delayMs, ct);
                        delayMs *= 2;
                        continue;
                    }
                    throw new RaftTransportException($"Cannot connect to peer {peerId}");
                }

                var correlationId = Interlocked.Increment(ref _correlationId);
                var requestBytes = SerializeRequestVote(request, correlationId);

                await using var lease = await connection.AcquireStreamAsync(ct);
                var stream = lease.Stream;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(5000);

                await stream.WriteAsync(requestBytes, cts.Token);
                await stream.FlushAsync(cts.Token);

                return await ReadRequestVoteResponseAsync(stream, cts.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxRetries)
            {
                LogRequestVoteRetry(peerId, attempt, ex);
                await RemoveConnectionAsync(peerId);
                await Task.Delay(delayMs, ct);
                delayMs *= 2;
            }
        }

        throw new RaftTransportException($"Failed to send RequestVote to peer {peerId} after {maxRetries} attempts");
    }

    public async Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
    {
        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var connection = await GetOrCreateConnectionAsync(peerId, ct);
                if (connection == null)
                {
                    if (attempt < maxRetries)
                    {
                        await Task.Delay(delayMs, ct);
                        delayMs *= 2;
                        continue;
                    }
                    throw new RaftTransportException($"Cannot connect to peer {peerId}");
                }

                var correlationId = Interlocked.Increment(ref _correlationId);
                var requestBytes = SerializeAppendEntries(request, correlationId);

                await using var lease = await connection.AcquireStreamAsync(ct);
                var stream = lease.Stream;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(5000);

                await stream.WriteAsync(requestBytes, cts.Token);
                await stream.FlushAsync(cts.Token);

                return await ReadAppendEntriesResponseAsync(stream, cts.Token);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxRetries)
            {
                LogAppendEntriesRetry(peerId, attempt, ex);
                await RemoveConnectionAsync(peerId);
                await Task.Delay(delayMs, ct);
                delayMs *= 2;
            }
        }

        throw new RaftTransportException($"Failed to send AppendEntries to peer {peerId} after {maxRetries} attempts");
    }

    private async Task<IPeerConnection?> GetOrCreateConnectionAsync(int peerId, CancellationToken ct)
    {
        if (_connections.TryGetValue(peerId, out var existing) && existing.IsConnected)
            return existing;

        if (!_clusterState.Brokers.TryGetValue(peerId, out var broker))
            return null;

        IPeerConnection? connection = null;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(3000);

            connection = await _peerTransport.ConnectAsync(broker.Host, _config.ReplicationPort, cts.Token);

            // Replace any stale entry and dispose it if the swap landed on a concurrent winner.
            if (!_connections.TryAdd(peerId, connection))
            {
                var winner = _connections[peerId];
                await connection.DisposeAsync();
                return winner;
            }

            LogConnectedToPeer(peerId, broker.Host, _config.ReplicationPort);
            var result = connection;
            connection = null; // ownership transferred to _connections
            return result;
        }
        catch (Exception ex)
        {
            LogConnectionFailed(peerId, ex);
            if (connection is not null)
            {
                try { await connection.DisposeAsync(); } catch { /* best-effort */ }
            }
            return null;
        }
    }

    private static byte[] SerializePreVote(PreVoteRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Reserve space for size
        writer.Write(0);

        // API Key
        writer.Write(BinaryPrimitives.ReverseEndianness(PreVoteApiKey));

        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)0));

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // Request fields (same as RequestVote but using ProposedTerm)
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ProposedTerm));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.CandidateId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LastLogIndex));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LastLogTerm));

        var bytes = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), bytes.Length - 4);

        return bytes;
    }

    private static byte[] SerializeRequestVote(RequestVoteRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Reserve space for size
        writer.Write(0);

        // API Key
        writer.Write(BinaryPrimitives.ReverseEndianness(RequestVoteApiKey));

        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)0));

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // Request fields
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Term));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.CandidateId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LastLogIndex));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LastLogTerm));

        var bytes = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), bytes.Length - 4);

        return bytes;
    }

    private static byte[] SerializeAppendEntries(AppendEntriesRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Reserve space for size
        writer.Write(0);

        // API Key
        writer.Write(BinaryPrimitives.ReverseEndianness(AppendEntriesApiKey));

        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)0));

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // Request fields
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Term));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LeaderId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.PrevLogIndex));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.PrevLogTerm));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.LeaderCommit));

        // Entries
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Entries.Length));
        foreach (var entry in request.Entries)
        {
            writer.Write(BinaryPrimitives.ReverseEndianness(entry.Term));
            writer.Write(BinaryPrimitives.ReverseEndianness(entry.Index));
            writer.Write(BinaryPrimitives.ReverseEndianness((int)entry.CommandType));
            writer.Write(BinaryPrimitives.ReverseEndianness(entry.Timestamp));
            writer.Write(BinaryPrimitives.ReverseEndianness(entry.Data.Length));
            writer.Write(entry.Data);
        }

        var bytes = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), bytes.Length - 4);

        return bytes;
    }

    private static async Task<PreVoteResponse> ReadPreVoteResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        var offset = 4; // Skip correlation ID
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var voteGranted = body[offset] != 0;

        return new PreVoteResponse(term, voteGranted);
    }

    private static async Task<RequestVoteResponse> ReadRequestVoteResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        var offset = 4; // Skip correlation ID
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var voteGranted = body[offset] != 0;

        return new RequestVoteResponse(term, voteGranted);
    }

    private static async Task<AppendEntriesResponse> ReadAppendEntriesResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        var offset = 4; // Skip correlation ID
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var success = body[offset] != 0;
        offset += 1;
        var matchIndex = BinaryPrimitives.ReadInt64BigEndian(body.AsSpan(offset, 8));

        return new AppendEntriesResponse(term, success, matchIndex);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (_, connection) in _connections)
        {
            try { await connection.DisposeAsync(); } catch { /* best-effort */ }
        }
        _connections.Clear();
    }

    private async Task RemoveConnectionAsync(int peerId)
    {
        if (!_connections.TryRemove(peerId, out var connection))
        {
            return;
        }

        await using (connection)
        {
            // Disposal happens at the end of this using-scope. Any exception
            // from DisposeAsync is rethrown by the await using, which is what
            // we want for accurate shutdown diagnostics.
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connected to peer {PeerId} at {Host}:{Port}")]
    private partial void LogConnectedToPeer(int peerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Failed to connect to peer {PeerId}")]
    private partial void LogConnectionFailed(int peerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "PreVote to peer {PeerId} failed on attempt {Attempt}, retrying")]
    private partial void LogPreVoteRetry(int peerId, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RequestVote to peer {PeerId} failed on attempt {Attempt}, retrying")]
    private partial void LogRequestVoteRetry(int peerId, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "AppendEntries to peer {PeerId} failed on attempt {Attempt}, retrying")]
    private partial void LogAppendEntriesRetry(int peerId, int attempt, Exception ex);
}

/// <summary>
/// Exception thrown when Raft transport fails.
/// </summary>
public sealed class RaftTransportException : Exception
{
    public RaftTransportException() { }
    public RaftTransportException(string message) : base(message) { }
    public RaftTransportException(string message, Exception innerException) : base(message, innerException) { }
}
