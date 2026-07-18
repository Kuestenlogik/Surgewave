using System.Buffers.Binary;
using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.InterBroker;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Accepts and handles replication fetch requests from follower brokers.
/// Listens on a separate port from the client-facing Kafka protocol.
/// Also handles heartbeat messages for broker failure detection and Raft RPCs.
/// </summary>
public sealed partial class ReplicationServer : IAsyncDisposable
{
    private const short HeartbeatApiKey = 100;
    private const short RequestVoteApiKey = 101;
    private const short AppendEntriesApiKey = 102;
    private const short MetadataUpdateApiKey = 103;
    private const short PreVoteApiKey = 104;

    private readonly ILogger<ReplicationServer> _logger;
    private readonly ClusterState _clusterState;
    private readonly LogManager _logManager;
    private readonly ReplicaManager _replicaManager;
    private readonly ClusteringConfig _config;
    private readonly IPeerTransport _peerTransport;
    private HeartbeatManager? _heartbeatManager;
    private RaftNode? _raftNode;
    private MetadataStateMachine? _metadataStateMachine;
    private NativeInterBrokerServer? _nativeInterBrokerServer;

    private IPeerListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;

    public ReplicationServer(
        ILogger<ReplicationServer> logger,
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

    /// <summary>
    /// Set the heartbeat manager for processing heartbeat requests.
    /// </summary>
    public void SetHeartbeatManager(HeartbeatManager heartbeatManager)
    {
        _heartbeatManager = heartbeatManager;
    }

    /// <summary>
    /// Set the Raft node for processing Raft RPCs.
    /// </summary>
    public void SetRaftNode(RaftNode raftNode)
    {
        _raftNode = raftNode;
    }

    /// <summary>
    /// Set the metadata state machine for applying metadata updates.
    /// </summary>
    public void SetMetadataStateMachine(MetadataStateMachine stateMachine)
    {
        _metadataStateMachine = stateMachine;
    }

    /// <summary>
    /// Set the native inter-broker server (#60 Inc4). When set, frames whose opcode is in the native
    /// SRWV band (<see cref="NativeInterBrokerServer.IsNativeOpcode"/>) are handed off to it instead of
    /// the Family-B replication parser. Legacy peers never emit native opcodes, so the fetch/Raft path
    /// is unchanged.
    /// </summary>
    public void SetNativeInterBrokerServer(NativeInterBrokerServer nativeInterBrokerServer)
    {
        _nativeInterBrokerServer = nativeInterBrokerServer;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _listener = _peerTransport.CreateListener(new IPEndPoint(IPAddress.Any, _config.ReplicationPort));
        await _listener.StartAsync(_cts.Token);

        _acceptTask = Task.Run(() => AcceptLoopAsync(_cts.Token), _cts.Token);

        LogReplicationServerStarted(_config.ReplicationPort);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_listener is not null)
        {
            await _listener.DisposeAsync();
        }

        if (_acceptTask != null)
        {
            try { await _acceptTask; } catch (OperationCanceledException) { }
        }

        _cts?.Dispose();
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _listener != null)
        {
            try
            {
                var connection = await _listener.AcceptAsync(ct);
                _ = HandleReplicaClientAsync(connection, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogAcceptError(ex);
            }
        }
    }

    private async Task HandleReplicaClientAsync(IPeerConnection connection, CancellationToken ct)
    {
        var endpoint = connection.RemoteEndPoint?.ToString() ?? "unknown";
        LogReplicaConnected(endpoint);

        try
        {
            await using (connection)
            {
                // Accept inbound streams from the peer and process one RPC each.
                // For TCP, AcceptInboundStreamAsync returns the single shared
                // stream immediately (it does not wait for data), so we MUST
                // stop the loop as soon as a read hits EOF — otherwise a closed
                // connection (e.g. a readiness probe) spins on read==0 forever,
                // saturating the thread pool and starving real fetch reads (#69).
                // Processing is awaited inline: the TCP stream is shared and
                // lock-serialised anyway, so there is no parallelism to lose.
                while (!ct.IsCancellationRequested && connection.IsConnected)
                {
                    IPeerStreamLease lease;
                    try
                    {
                        lease = await connection.AcceptInboundStreamAsync(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (IOException)
                    {
                        break;
                    }

                    var alive = await HandleSingleRpcAsync(lease, ct);
                    if (!alive)
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            LogReplicaClientError(endpoint, ex);
        }
        finally
        {
            LogReplicaDisconnected(endpoint);
        }
    }

    /// <summary>
    /// Process a single RPC on the leased stream. Returns <c>false</c> when the
    /// stream is finished (EOF / bad frame / cancellation / error) so the caller
    /// stops looping on a dead connection instead of spinning.
    /// </summary>
    private async Task<bool> HandleSingleRpcAsync(IPeerStreamLease lease, CancellationToken ct)
    {
        try
        {
            await using (lease)
            {
                var stream = lease.Stream;

                var body = await ReadBodyAsync(stream, ct);
                if (body == null) return false;

                // #60 Inc4 — multiplex the native SRWV inter-broker band onto the shared ReplicationPort.
                // Family-B frames (Fetch=1, Heartbeat=100, Raft=101/102/104) all sit below the native
                // band, so this peek leaves the hot fetch/Raft path untouched; only frames a native peer
                // emits — which no legacy peer ever sends — take the native branch.
                if (_nativeInterBrokerServer is not null && body.Length >= 2 &&
                    NativeInterBrokerServer.IsNativeOpcode(BinaryPrimitives.ReadUInt16BigEndian(body)))
                {
                    await _nativeInterBrokerServer.HandleBodyAsync(stream, body, ct);
                    return true;
                }

                var request = ParseRequest(body);
                var response = await ProcessRequestAsync(request, ct);
                await WriteResponseAsync(stream, response, ct);
                return true;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception ex)
        {
            LogReplicaClientError("stream-rpc", ex);
            return false;
        }
    }

    private static async Task<byte[]?> ReadBodyAsync(Stream stream, CancellationToken ct)
    {
        // Read the 4-byte size prefix EXACTLY: a single ReadAsync may legally return 1-3 bytes
        // (TCP gives no alignment guarantee), and parsing a torn prefix yields a garbage size that
        // desyncs every subsequent RPC on the connection (#77). read == 0 is a clean close between
        // frames; 1-3 bytes is a peer death mid-prefix — both end this stream.
        var sizeBuffer = new byte[4];
        var read = await stream.ReadAtLeastAsync(sizeBuffer, 4, throwOnEndOfStream: false, ct);
        if (read < 4)
            return null;

        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        if (size <= 0 || size > 100 * 1024 * 1024) // Max 100MB
            return null;

        // Read body
        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        return body;
    }

    private ReplicationRequest ParseRequest(byte[] data)
    {
        var offset = 0;

        // API Key
        var apiKey = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;

        // API Version
        var apiVersion = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;

        // Correlation ID
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Client ID (nullable string)
        var clientIdLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;
        if (clientIdLen > 0)
            offset += clientIdLen;

        // Parse based on API key
        return apiKey switch
        {
            1 => ParseFetchRequest(data, offset, correlationId), // Fetch
            HeartbeatApiKey => ParseHeartbeatRequest(data, offset, correlationId), // Heartbeat
            RequestVoteApiKey => ParseRequestVoteRequest(data, offset, correlationId), // Raft RequestVote
            AppendEntriesApiKey => ParseAppendEntriesRequest(data, offset, correlationId), // Raft AppendEntries
            MetadataUpdateApiKey => ParseMetadataUpdateRequest(data, offset, correlationId), // Metadata Update
            PreVoteApiKey => ParsePreVoteRequest(data, offset, correlationId), // Raft PreVote
            _ => new ReplicationRequest { ApiKey = apiKey, CorrelationId = correlationId }
        };
    }

    private static ReplicationRequest ParseMetadataUpdateRequest(byte[] data, int offset, int correlationId)
    {
        var controllerId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var controllerEpoch = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var metadataVersion = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;
        var commandType = (MetadataCommandType)BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var timestamp = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;
        var dataLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var commandData = data.AsSpan(offset, dataLength).ToArray();

        return new ReplicationRequest
        {
            ApiKey = MetadataUpdateApiKey,
            CorrelationId = correlationId,
            MetadataUpdateRequest = new MetadataUpdateRequest(
                controllerId, controllerEpoch, metadataVersion, commandType, commandData, timestamp)
        };
    }

    private static ReplicationRequest ParseRequestVoteRequest(byte[] data, int offset, int correlationId)
    {
        var term = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var candidateId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var lastLogIndex = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;
        var lastLogTerm = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));

        return new ReplicationRequest
        {
            ApiKey = RequestVoteApiKey,
            CorrelationId = correlationId,
            RequestVoteRequest = new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm)
        };
    }

    private static ReplicationRequest ParsePreVoteRequest(byte[] data, int offset, int correlationId)
    {
        var proposedTerm = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var candidateId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var lastLogIndex = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;
        var lastLogTerm = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));

        return new ReplicationRequest
        {
            ApiKey = PreVoteApiKey,
            CorrelationId = correlationId,
            PreVoteRequest = new PreVoteRequest(proposedTerm, candidateId, lastLogIndex, lastLogTerm)
        };
    }

    private static ReplicationRequest ParseAppendEntriesRequest(byte[] data, int offset, int correlationId)
    {
        var term = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var leaderId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var prevLogIndex = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;
        var prevLogTerm = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        var leaderCommit = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;

        var entryCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        var entries = new RaftLogEntry[entryCount];
        for (var i = 0; i < entryCount; i++)
        {
            var entryTerm = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;
            var entryIndex = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
            offset += 8;
            var commandType = (MetadataCommandType)BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;
            var timestamp = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
            offset += 8;
            var dataLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;
            var entryData = data.AsSpan(offset, dataLength).ToArray();
            offset += dataLength;

            entries[i] = new RaftLogEntry
            {
                Term = entryTerm,
                Index = entryIndex,
                CommandType = commandType,
                Timestamp = timestamp,
                Data = entryData
            };
        }

        return new ReplicationRequest
        {
            ApiKey = AppendEntriesApiKey,
            CorrelationId = correlationId,
            AppendEntriesRequest = new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
        };
    }

    private static ReplicationRequest ParseHeartbeatRequest(byte[] data, int offset, int correlationId)
    {
        var request = new ReplicationRequest
        {
            ApiKey = HeartbeatApiKey,
            CorrelationId = correlationId,
            HeartbeatRequest = new HeartbeatRequest(
                BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4)),
                BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset + 4, 4)),
                BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset + 8, 8)),
                BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset + 16, 4)),
                BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset + 20, 4))
            )
        };

        return request;
    }

    private ReplicationRequest ParseFetchRequest(byte[] data, int offset, int correlationId)
    {
        var request = new ReplicationRequest
        {
            ApiKey = 1,
            CorrelationId = correlationId,
            FetchRequest = new ReplicaFetchRequest()
        };

        // Replica ID
        request.FetchRequest.ReplicaId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Max Wait Ms
        request.FetchRequest.MaxWaitMs = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Min Bytes
        request.FetchRequest.MinBytes = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Max Bytes
        request.FetchRequest.MaxBytes = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Isolation Level
        request.FetchRequest.IsolationLevel = data[offset++];

        // Session ID
        offset += 4;

        // Session Epoch
        offset += 4;

        // Topics
        var topicCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        request.FetchRequest.Topics = [];

        for (int i = 0; i < topicCount; i++)
        {
            var topicData = new ReplicaFetchRequest.TopicData { Topic = "", Partitions = [] };

            // Topic name
            var topicLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            topicData.Topic = System.Text.Encoding.UTF8.GetString(data, offset, topicLen);
            offset += topicLen;

            // Partitions
            var partitionCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;

            for (int j = 0; j < partitionCount; j++)
            {
                var partitionData = new ReplicaFetchRequest.PartitionData();

                partitionData.Partition = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                // Current leader epoch
                offset += 4;

                partitionData.FetchOffset = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;

                // Log start offset
                offset += 8;

                partitionData.PartitionMaxBytes = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                topicData.Partitions.Add(partitionData);
            }

            request.FetchRequest.Topics.Add(topicData);
        }

        return request;
    }

    private async Task<byte[]> ProcessRequestAsync(ReplicationRequest request, CancellationToken ct)
    {
        return request.ApiKey switch
        {
            1 => await HandleFetchAsync(request, ct),
            HeartbeatApiKey => HandleHeartbeat(request),
            RequestVoteApiKey => await HandleRequestVoteAsync(request, ct),
            AppendEntriesApiKey => await HandleAppendEntriesAsync(request, ct),
            MetadataUpdateApiKey => HandleMetadataUpdate(request),
            PreVoteApiKey => await HandlePreVoteAsync(request, ct),
            _ => BuildErrorResponse(request.CorrelationId, ClusterRpcStatus.UnsupportedVersion)
        };
    }

    private byte[] HandleMetadataUpdate(ReplicationRequest request)
    {
        if (_metadataStateMachine == null || request.MetadataUpdateRequest == null)
        {
            return SerializeMetadataUpdateResponse(request.CorrelationId,
                new MetadataUpdateResponse(_config.BrokerId, (short)ClusterRpcStatus.Unknown, 0));
        }

        var req = request.MetadataUpdateRequest;

        // Validate + advance atomically through the shared fence (#72 Inc4): the previous unlocked
        // check-then-write could interleave two racing frames and regress the epoch, which the
        // composed broker-epoch mint would amplify. No wire is noted — this legacy ApiKey-103
        // channel is neither the Kafka client wire nor the SRWV band (and is currently unreachable:
        // nothing wires SetMetadataStateMachine).
        if (!_clusterState.TryAdvanceControllerEpoch(req.ControllerId, req.ControllerEpoch))
        {
            LogStaleMetadataUpdate(req.ControllerId, req.ControllerEpoch, _clusterState.ControllerEpoch);
            return SerializeMetadataUpdateResponse(request.CorrelationId,
                new MetadataUpdateResponse(_config.BrokerId, (short)ClusterRpcStatus.StaleControllerEpoch, _clusterState.MetadataVersion));
        }

        // Apply the metadata command using the state machine
        var entry = new RaftLogEntry
        {
            Term = 0, // Not Raft-originated
            Index = req.MetadataVersion,
            CommandType = req.CommandType,
            Data = req.CommandData,
            Timestamp = req.Timestamp
        };

        _metadataStateMachine.Apply(entry);
        _clusterState.MetadataVersion = req.MetadataVersion;

        LogMetadataUpdateApplied(req.CommandType, req.MetadataVersion, req.ControllerId);

        return SerializeMetadataUpdateResponse(request.CorrelationId,
            new MetadataUpdateResponse(_config.BrokerId, (short)ClusterRpcStatus.None, _clusterState.MetadataVersion));
    }

    private static byte[] SerializeMetadataUpdateResponse(int correlationId, MetadataUpdateResponse response)
    {
        var frame = NewFrame(18, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], response.BrokerId);
        BinaryPrimitives.WriteInt16BigEndian(span[(o + 8)..], response.ErrorCode);
        BinaryPrimitives.WriteInt64BigEndian(span[(o + 10)..], response.MetadataVersion);
        return frame;
    }

    private async Task<byte[]> HandleRequestVoteAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.RequestVoteRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ClusterRpcStatus.Unknown);
        }

        var response = await _raftNode.HandleRequestVoteAsync(request.RequestVoteRequest, ct);
        return SerializeRequestVoteResponse(request.CorrelationId, response);
    }

    private async Task<byte[]> HandleAppendEntriesAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.AppendEntriesRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ClusterRpcStatus.Unknown);
        }

        var response = await _raftNode.HandleAppendEntriesAsync(request.AppendEntriesRequest, ct);
        return SerializeAppendEntriesResponse(request.CorrelationId, response);
    }

    private async Task<byte[]> HandlePreVoteAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.PreVoteRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ClusterRpcStatus.Unknown);
        }

        var response = await _raftNode.HandlePreVoteAsync(request.PreVoteRequest, ct);
        return SerializePreVoteResponse(request.CorrelationId, response);
    }

    private static byte[] SerializePreVoteResponse(int correlationId, PreVoteResponse response)
    {
        var frame = NewFrame(9, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], response.Term);
        frame[o + 8] = (byte)(response.VoteGranted ? 1 : 0);
        return frame;
    }

    private static byte[] SerializeRequestVoteResponse(int correlationId, RequestVoteResponse response)
    {
        var frame = NewFrame(9, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], response.Term);
        frame[o + 8] = (byte)(response.VoteGranted ? 1 : 0);
        return frame;
    }

    private static byte[] SerializeAppendEntriesResponse(int correlationId, AppendEntriesResponse response)
    {
        var frame = NewFrame(17, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], response.Term);
        frame[o + 8] = (byte)(response.Success ? 1 : 0);
        BinaryPrimitives.WriteInt64BigEndian(span[(o + 9)..], response.MatchIndex);
        return frame;
    }

    private byte[] HandleHeartbeat(ReplicationRequest request)
    {
        if (_heartbeatManager == null || request.HeartbeatRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ClusterRpcStatus.Unknown);
        }

        var response = _heartbeatManager.ProcessHeartbeat(request.HeartbeatRequest);
        return SerializeHeartbeatResponse(request.CorrelationId, response);
    }

    private static byte[] SerializeHeartbeatResponse(int correlationId, HeartbeatResponse response)
    {
        var frame = NewFrame(21, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], response.BrokerId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 8)..], response.BrokerEpoch);
        BinaryPrimitives.WriteInt64BigEndian(span[(o + 12)..], response.Timestamp);
        frame[o + 20] = (byte)(response.IsController ? 1 : 0);
        return frame;
    }

    /// <summary>
    /// Per-partition fetch result gathered in pass 1 of <see cref="HandleFetchAsync"/> so the exact
    /// response size is known before serializing. <see cref="Records"/> is the concatenated record
    /// batches as a single contiguous slice (empty for an error partition, which serializes with
    /// zero-length records).
    /// </summary>
    private readonly record struct FetchPartitionResult(
        int Partition,
        ClusterRpcStatus Status,
        long HighWatermark,
        long LogStartOffset,
        ReadOnlyMemory<byte> Records,
        int BatchBytes);

    /// <summary>Fixed serialized size of one fetch partition response, excluding the record batches.</summary>
    private const int FetchPartitionFixedBytes = 4 + 2 + 8 + 8 + 8 + 4 + 4 + 4;

    private async Task<byte[]> HandleFetchAsync(ReplicationRequest request, CancellationToken ct)
    {
        var fetchRequest = request.FetchRequest!;
        var replicaId = fetchRequest.ReplicaId;

        // Pass 1 — read the requested batches and pre-compute the exact response size, so pass 2 can
        // serialize into a single right-sized frame: no MemoryStream growth chain, no ToArray copy,
        // and each record batch is copied exactly once (into the frame the transport writes).
        var bodySize = 4 + 4 + 2 + 4 + 4; // correlation id + throttle + error code + session id + topic count
        var topics = new List<(byte[] Name, List<FetchPartitionResult> Partitions)>(fetchRequest.Topics.Count);

        foreach (var topicData in fetchRequest.Topics)
        {
            var name = System.Text.Encoding.UTF8.GetBytes(topicData.Topic);
            bodySize += 2 + name.Length + 4; // name length + name + partition count
            var partitions = new List<FetchPartitionResult>(topicData.Partitions.Count);

            foreach (var partitionData in topicData.Partitions)
            {
                var tp = new TopicPartition { Topic = topicData.Topic, Partition = partitionData.Partition };

                if (!_replicaManager.IsLeader(tp))
                {
                    partitions.Add(new FetchPartitionResult(
                        partitionData.Partition, ClusterRpcStatus.NotLeaderForPartition, 0L, 0L, default, 0));
                    bodySize += FetchPartitionFixedBytes;
                    continue;
                }

                // Contiguous read: one slice over the concatenated batches instead of a List<byte[]> plus
                // its per-batch heap arrays. The returned memory stays valid until pass 2 copies it into
                // the frame (same lifetime the client fetch path already relies on).
                var (records, _) = await _logManager.ReadBatchesContiguousAsync(
                    tp, partitionData.FetchOffset, partitionData.PartitionMaxBytes, ct);

                // Update follower position for ISR tracking
                _replicaManager.UpdateFollowerFetchPosition(tp, replicaId, partitionData.FetchOffset);

                var hw = _replicaManager.GetHighWatermark(tp);
                var log = _logManager.GetOrCreateLog(tp);

                partitions.Add(new FetchPartitionResult(
                    partitionData.Partition, ClusterRpcStatus.None, hw, log.LogStartOffset, records, records.Length));
                bodySize += FetchPartitionFixedBytes + records.Length;
            }

            topics.Add((name, partitions));
        }

        // Pass 2 — serialize into the exact-size frame.
        var frame = NewFrame(bodySize, out var o);
        var span = frame.AsSpan();

        BinaryPrimitives.WriteInt32BigEndian(span[o..], request.CorrelationId); o += 4;
        BinaryPrimitives.WriteInt32BigEndian(span[o..], 0); o += 4;            // throttle time
        BinaryPrimitives.WriteInt16BigEndian(span[o..], 0); o += 2;            // error code
        BinaryPrimitives.WriteInt32BigEndian(span[o..], 0); o += 4;            // session id
        BinaryPrimitives.WriteInt32BigEndian(span[o..], topics.Count); o += 4;

        foreach (var (name, partitions) in topics)
        {
            BinaryPrimitives.WriteInt16BigEndian(span[o..], (short)name.Length); o += 2;
            name.CopyTo(span[o..]); o += name.Length;
            BinaryPrimitives.WriteInt32BigEndian(span[o..], partitions.Count); o += 4;

            foreach (var p in partitions)
            {
                BinaryPrimitives.WriteInt32BigEndian(span[o..], p.Partition); o += 4;
                BinaryPrimitives.WriteInt16BigEndian(span[o..], (short)p.Status); o += 2;
                BinaryPrimitives.WriteInt64BigEndian(span[o..], p.HighWatermark); o += 8;
                BinaryPrimitives.WriteInt64BigEndian(span[o..], -1L); o += 8;  // last stable offset
                BinaryPrimitives.WriteInt64BigEndian(span[o..], p.LogStartOffset); o += 8;
                BinaryPrimitives.WriteInt32BigEndian(span[o..], 0); o += 4;    // aborted transactions
                BinaryPrimitives.WriteInt32BigEndian(span[o..], -1); o += 4;   // preferred read replica
                BinaryPrimitives.WriteInt32BigEndian(span[o..], p.BatchBytes); o += 4;

                if (p.Records.Length == 0)
                    continue;
                p.Records.Span.CopyTo(span[o..]);
                o += p.Records.Length;
            }
        }

        return frame;
    }

    private static byte[] BuildErrorResponse(int correlationId, ClusterRpcStatus errorCode)
    {
        var frame = NewFrame(18, out var o);
        var span = frame.AsSpan();
        BinaryPrimitives.WriteInt32BigEndian(span[o..], correlationId);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 4)..], 0);              // throttle time
        BinaryPrimitives.WriteInt16BigEndian(span[(o + 8)..], (short)errorCode);
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 10)..], 0);             // session id
        BinaryPrimitives.WriteInt32BigEndian(span[(o + 14)..], 0);             // topics count
        return frame;
    }

    /// <summary>
    /// Allocate a response frame — <c>[int32 size][body]</c> — with the size prefix already filled in,
    /// so <see cref="WriteResponseAsync"/> hands the complete frame to the transport in one write
    /// (instead of a separate 4-byte prefix write per response). <paramref name="offset"/> points at
    /// the body start; every response builder in this class returns such a frame.
    /// </summary>
    private static byte[] NewFrame(int bodySize, out int offset)
    {
        var frame = new byte[4 + bodySize];
        BinaryPrimitives.WriteInt32BigEndian(frame, bodySize);
        offset = 4;
        return frame;
    }

    private static async Task WriteResponseAsync(Stream stream, byte[] frame, CancellationToken ct)
    {
        await stream.WriteAsync(frame, ct);
        await stream.FlushAsync(ct);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Replication server started on port {Port}")]
    private partial void LogReplicationServerStarted(int port);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Replica connected from {Endpoint}")]
    private partial void LogReplicaConnected(string endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Replica disconnected: {Endpoint}")]
    private partial void LogReplicaDisconnected(string endpoint);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error accepting replica connection")]
    private partial void LogAcceptError(Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error handling replica client {Endpoint}")]
    private partial void LogReplicaClientError(string endpoint, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected stale metadata update from controller {ControllerId} (epoch {ReceivedEpoch} < {CurrentEpoch})")]
    private partial void LogStaleMetadataUpdate(int controllerId, int receivedEpoch, int currentEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Applied metadata update: {CommandType} (version={Version}, controller={ControllerId})")]
    private partial void LogMetadataUpdateApplied(MetadataCommandType commandType, long version, int controllerId);
}

internal sealed class ReplicationRequest
{
    public short ApiKey { get; set; }
    public int CorrelationId { get; set; }
    public ReplicaFetchRequest? FetchRequest { get; set; }
    public HeartbeatRequest? HeartbeatRequest { get; set; }
    public RequestVoteRequest? RequestVoteRequest { get; set; }
    public AppendEntriesRequest? AppendEntriesRequest { get; set; }
    public MetadataUpdateRequest? MetadataUpdateRequest { get; set; }
    public PreVoteRequest? PreVoteRequest { get; set; }
}
