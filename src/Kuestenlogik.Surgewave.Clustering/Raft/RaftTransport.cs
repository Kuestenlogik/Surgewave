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

    // An answer is 13 or 21 bytes; anything far from that is a stream out of step, not an answer to allocate for.
    private const int MaxResponseBytes = 1024;

    /// <summary>
    /// How long one attempt of a request may take, written and answered — settable for tests only (via
    /// InternalsVisibleTo), which cannot wait out three attempts of five seconds each.
    /// </summary>
    internal TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(5);

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

    public Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct) =>
        SendAsync(peerId, "PreVote", id => SerializePreVote(request, id), ParsePreVoteResponse, ct);

    public Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct) =>
        SendAsync(peerId, "RequestVote", id => SerializeRequestVote(request, id), ParseRequestVoteResponse, ct);

    public Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct) =>
        SendAsync(peerId, "AppendEntries", id => SerializeAppendEntries(request, id), ParseAppendEntriesResponse, ct);

    /// <summary>
    /// Sends one request and returns its answer, in up to three attempts, each on a connection that carries nothing
    /// of an earlier request.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A TCP connection carries one request after the other and the answers in that order, so a request that was
    /// written and not answered — the caller cancelled, the attempt ran out of time, the read failed — leaves an
    /// answer on it that nobody waits for. The connection used to stay in place when the caller cancelled and when
    /// the last attempt failed, and the next request on it read that answer as its own (#275): a pre-vote granted for
    /// one term as the vote of another, the two having the same layout, or the match index of an AppendEntries sent
    /// earlier. A connection is now dropped with the request that was abandoned on it, however the request ended,
    /// and the next one connects anew.
    /// </para>
    /// <para>
    /// The answer names the request it answers: the peer returns the correlation id it was sent, and an answer with
    /// another one is not taken. With the connection dropped as above that does not happen between two brokers of
    /// this version; it is the check that says so if the pairing breaks some other way, instead of a wrong vote.
    /// </para>
    /// </remarks>
    private async Task<TResponse> SendAsync<TResponse>(
        int peerId, string rpc, Func<int, byte[]> serialize, Func<byte[], TResponse> parse, CancellationToken ct)
    {
        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; ; attempt++)
        {
            IPeerConnection? connection = null;
            var written = false;
            try
            {
                connection = await GetOrCreateConnectionAsync(peerId, ct);
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
                var requestBytes = serialize(correlationId);

                // AcquireStreamAsync: QUIC opens a fresh bidi stream per RPC
                // (independent flow control, no head-of-line blocking); TCP
                // holds a connection-level lock and reuses the single stream.
                await using var lease = await connection.AcquireStreamAsync(ct);
                var stream = lease.Stream;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(RequestTimeout);

                written = true;
                await stream.WriteAsync(requestBytes, cts.Token);
                await stream.FlushAsync(cts.Token);

                var body = await ReadResponseAsync(stream, cts.Token);
                var answered = BinaryPrimitives.ReadInt32BigEndian(body);
                if (answered != correlationId)
                {
                    throw new RaftTransportException(
                        $"Peer {peerId} answered request {answered} where {rpc} request {correlationId} was asked");
                }

                return parse(body);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // A caller that gave up before anything was written leaves the connection as it found it.
                if (written)
                    await DropConnectionAsync(peerId, connection);
                throw;
            }
            catch (Exception ex)
            {
                await DropConnectionAsync(peerId, connection);
                if (attempt >= maxRetries)
                    throw;

                LogRequestRetry(rpc, peerId, attempt, ex);
                await Task.Delay(delayMs, ct);
                delayMs *= 2;
            }
        }
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

            // The PEER's replication port, not this node's. Dialling _config.ReplicationPort sent
            // every Raft RPC to ourselves whenever the peer was on the same host — so each node
            // answered its own RequestVote, voted for itself and became leader, and a three-broker
            // cluster produced three leaders in the same term (#176).
            //
            // Invisible until now: Raft was off by default and the push path used a different
            // client with the correct port, and a single-node quorum never dials anyone.
            connection = await _peerTransport.ConnectAsync(broker.Host, broker.ReplicationPort, cts.Token);

            // Replace any stale entry and dispose it if the swap landed on a concurrent winner.
            if (!_connections.TryAdd(peerId, connection))
            {
                var winner = _connections[peerId];
                await connection.DisposeAsync();
                return winner;
            }

            LogConnectedToPeer(peerId, broker.Host, broker.ReplicationPort);
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

    // One answer frame: the correlation id of the request it answers, then the answer.
    private static async Task<byte[]> ReadResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        if (size < 4 || size > MaxResponseBytes)
            throw new RaftTransportException($"A Raft answer of {size} bytes is not one");

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);
        return body;
    }

    private static PreVoteResponse ParsePreVoteResponse(byte[] body)
    {
        var offset = 4; // The correlation id, checked by the caller
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var voteGranted = body[offset] != 0;

        return new PreVoteResponse(term, voteGranted);
    }

    private static RequestVoteResponse ParseRequestVoteResponse(byte[] body)
    {
        var offset = 4; // The correlation id, checked by the caller
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var voteGranted = body[offset] != 0;

        return new RequestVoteResponse(term, voteGranted);
    }

    private static AppendEntriesResponse ParseAppendEntriesResponse(byte[] body)
    {
        var offset = 4; // The correlation id, checked by the caller
        var term = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset, 4));
        offset += 4;
        var success = body[offset] != 0;
        offset += 1;
        var matchIndex = BinaryPrimitives.ReadInt64BigEndian(body.AsSpan(offset, 8));

        return new AppendEntriesResponse(term, success, matchIndex);
    }

    public async ValueTask DisposeAsync()
    {
        // Whoever removes a connection closes it; see DropConnectionAsync.
        foreach (var entry in _connections)
        {
            if (!_connections.TryRemove(entry))
                continue;
            try { await entry.Value.DisposeAsync(); } catch { /* best-effort */ }
        }
    }

    // Takes the connection a request was abandoned on out of use and closes it. Whoever removes it closes it: a
    // request that waited for its turn on the same connection fails on it and closes nothing a second time.
    private async Task DropConnectionAsync(int peerId, IPeerConnection? connection)
    {
        if (connection is null || !_connections.TryRemove(new KeyValuePair<int, IPeerConnection>(peerId, connection)))
            return;

        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex)
        {
            LogConnectionCloseFailed(peerId, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connected to peer {PeerId} at {Host}:{Port}")]
    private partial void LogConnectedToPeer(int peerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Failed to connect to peer {PeerId}")]
    private partial void LogConnectionFailed(int peerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Rpc} to peer {PeerId} failed on attempt {Attempt}, retrying")]
    private partial void LogRequestRetry(string rpc, int peerId, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing the connection to peer {PeerId} failed")]
    private partial void LogConnectionCloseFailed(int peerId, Exception ex);
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
