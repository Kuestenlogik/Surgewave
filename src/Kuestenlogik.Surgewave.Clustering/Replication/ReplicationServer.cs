using System.Buffers.Binary;
using System.Net;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
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

        var streamTasks = new List<Task>();
        try
        {
            await using (connection)
            {
                // Accept inbound streams from the peer. For QUIC each accepted
                // stream is an independent RPC channel (real parallelism). For
                // TCP this always returns the single shared stream serialised by
                // a lock, so it's effectively the same sequential loop as before.
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

                    streamTasks.Add(HandleSingleRpcAsync(lease, ct));
                }

                try { await Task.WhenAll(streamTasks); }
                catch { /* per-stream errors already logged */ }
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

    private async Task HandleSingleRpcAsync(IPeerStreamLease lease, CancellationToken ct)
    {
        try
        {
            await using (lease)
            {
                var stream = lease.Stream;

                var request = await ReadRequestAsync(stream, ct);
                if (request == null) return;

                var response = await ProcessRequestAsync(request, ct);
                await WriteResponseAsync(stream, response, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            LogReplicaClientError("stream-rpc", ex);
        }
    }

    private async Task<ReplicationRequest?> ReadRequestAsync(Stream stream, CancellationToken ct)
    {
        // Read size
        var sizeBuffer = new byte[4];
        var read = await stream.ReadAsync(sizeBuffer, ct);
        if (read == 0)
            return null;

        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        if (size <= 0 || size > 100 * 1024 * 1024) // Max 100MB
            return null;

        // Read body
        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        return ParseRequest(body);
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
            _ => BuildErrorResponse(request.CorrelationId, ErrorCode.UnsupportedVersion)
        };
    }

    private byte[] HandleMetadataUpdate(ReplicationRequest request)
    {
        if (_metadataStateMachine == null || request.MetadataUpdateRequest == null)
        {
            return SerializeMetadataUpdateResponse(request.CorrelationId,
                new MetadataUpdateResponse(_config.BrokerId, (short)ErrorCode.Unknown, 0));
        }

        var req = request.MetadataUpdateRequest;

        // Validate controller epoch - reject stale updates
        if (req.ControllerEpoch < _clusterState.ControllerEpoch)
        {
            LogStaleMetadataUpdate(req.ControllerId, req.ControllerEpoch, _clusterState.ControllerEpoch);
            return SerializeMetadataUpdateResponse(request.CorrelationId,
                new MetadataUpdateResponse(_config.BrokerId, (short)ErrorCode.StaleControllerEpoch, _clusterState.MetadataVersion));
        }

        // Update controller epoch if newer
        if (req.ControllerEpoch > _clusterState.ControllerEpoch)
        {
            _clusterState.ControllerEpoch = req.ControllerEpoch;
            _clusterState.ControllerId = req.ControllerId;
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
            new MetadataUpdateResponse(_config.BrokerId, (short)ErrorCode.None, _clusterState.MetadataVersion));
    }

    private static byte[] SerializeMetadataUpdateResponse(int correlationId, MetadataUpdateResponse response)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.BrokerId));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.ErrorCode));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.MetadataVersion));

        return ms.ToArray();
    }

    private async Task<byte[]> HandleRequestVoteAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.RequestVoteRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ErrorCode.Unknown);
        }

        var response = await _raftNode.HandleRequestVoteAsync(request.RequestVoteRequest, ct);
        return SerializeRequestVoteResponse(request.CorrelationId, response);
    }

    private async Task<byte[]> HandleAppendEntriesAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.AppendEntriesRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ErrorCode.Unknown);
        }

        var response = await _raftNode.HandleAppendEntriesAsync(request.AppendEntriesRequest, ct);
        return SerializeAppendEntriesResponse(request.CorrelationId, response);
    }

    private async Task<byte[]> HandlePreVoteAsync(ReplicationRequest request, CancellationToken ct)
    {
        if (_raftNode == null || request.PreVoteRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ErrorCode.Unknown);
        }

        var response = await _raftNode.HandlePreVoteAsync(request.PreVoteRequest, ct);
        return SerializePreVoteResponse(request.CorrelationId, response);
    }

    private static byte[] SerializePreVoteResponse(int correlationId, PreVoteResponse response)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.Term));
        writer.Write((byte)(response.VoteGranted ? 1 : 0));

        return ms.ToArray();
    }

    private static byte[] SerializeRequestVoteResponse(int correlationId, RequestVoteResponse response)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.Term));
        writer.Write((byte)(response.VoteGranted ? 1 : 0));

        return ms.ToArray();
    }

    private static byte[] SerializeAppendEntriesResponse(int correlationId, AppendEntriesResponse response)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.Term));
        writer.Write((byte)(response.Success ? 1 : 0));
        writer.Write(BinaryPrimitives.ReverseEndianness(response.MatchIndex));

        return ms.ToArray();
    }

    private byte[] HandleHeartbeat(ReplicationRequest request)
    {
        if (_heartbeatManager == null || request.HeartbeatRequest == null)
        {
            return BuildErrorResponse(request.CorrelationId, ErrorCode.Unknown);
        }

        var response = _heartbeatManager.ProcessHeartbeat(request.HeartbeatRequest);
        return SerializeHeartbeatResponse(request.CorrelationId, response);
    }

    private static byte[] SerializeHeartbeatResponse(int correlationId, HeartbeatResponse response)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Broker ID
        writer.Write(BinaryPrimitives.ReverseEndianness(response.BrokerId));

        // Broker Epoch
        writer.Write(BinaryPrimitives.ReverseEndianness(response.BrokerEpoch));

        // Timestamp
        writer.Write(BinaryPrimitives.ReverseEndianness(response.Timestamp));

        // Is Controller
        writer.Write((byte)(response.IsController ? 1 : 0));

        return ms.ToArray();
    }

    private async Task<byte[]> HandleFetchAsync(ReplicationRequest request, CancellationToken ct)
    {
        var fetchRequest = request.FetchRequest!;
        var replicaId = fetchRequest.ReplicaId;

        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(request.CorrelationId));

        // Throttle time
        writer.Write(BinaryPrimitives.ReverseEndianness(0));

        // Error code
        writer.Write(BinaryPrimitives.ReverseEndianness((short)0));

        // Session ID
        writer.Write(BinaryPrimitives.ReverseEndianness(0));

        // Topics
        writer.Write(BinaryPrimitives.ReverseEndianness(fetchRequest.Topics.Count));

        foreach (var topicData in fetchRequest.Topics)
        {
            // Topic name
            var topicBytes = System.Text.Encoding.UTF8.GetBytes(topicData.Topic);
            writer.Write(BinaryPrimitives.ReverseEndianness((short)topicBytes.Length));
            writer.Write(topicBytes);

            // Partitions
            writer.Write(BinaryPrimitives.ReverseEndianness(topicData.Partitions.Count));

            foreach (var partitionData in topicData.Partitions)
            {
                var tp = new TopicPartition { Topic = topicData.Topic, Partition = partitionData.Partition };

                // Check if we're the leader
                if (!_replicaManager.IsLeader(tp))
                {
                    WritePartitionError(writer, partitionData.Partition, ErrorCode.NotLeaderForPartition);
                    continue;
                }

                // Get batches from log
                var batches = await _logManager.ReadBatchesAsync(
                    tp, partitionData.FetchOffset, partitionData.PartitionMaxBytes, ct);

                // Update follower position for ISR tracking
                _replicaManager.UpdateFollowerFetchPosition(tp, replicaId, partitionData.FetchOffset);

                // Get high watermark
                var hw = _replicaManager.GetHighWatermark(tp);
                var log = _logManager.GetOrCreateLog(tp);
                var logStartOffset = log.LogStartOffset;

                // Write partition response
                writer.Write(BinaryPrimitives.ReverseEndianness(partitionData.Partition));
                writer.Write(BinaryPrimitives.ReverseEndianness((short)ErrorCode.None));
                writer.Write(BinaryPrimitives.ReverseEndianness(hw));
                writer.Write(BinaryPrimitives.ReverseEndianness(-1L)); // Last stable offset
                writer.Write(BinaryPrimitives.ReverseEndianness(logStartOffset));

                // Aborted transactions
                writer.Write(BinaryPrimitives.ReverseEndianness(0));

                // Preferred read replica
                writer.Write(BinaryPrimitives.ReverseEndianness(-1));

                // Record batches
                var totalSize = batches.Sum(b => b.Length);
                writer.Write(BinaryPrimitives.ReverseEndianness(totalSize));
                foreach (var batch in batches)
                {
                    writer.Write(batch);
                }
            }
        }

        return ms.ToArray();
    }

    private void WritePartitionError(BinaryWriter writer, int partition, ErrorCode errorCode)
    {
        writer.Write(BinaryPrimitives.ReverseEndianness(partition));
        writer.Write(BinaryPrimitives.ReverseEndianness((short)errorCode));
        writer.Write(BinaryPrimitives.ReverseEndianness(0L)); // High watermark
        writer.Write(BinaryPrimitives.ReverseEndianness(-1L)); // Last stable offset
        writer.Write(BinaryPrimitives.ReverseEndianness(0L)); // Log start offset
        writer.Write(BinaryPrimitives.ReverseEndianness(0)); // Aborted transactions
        writer.Write(BinaryPrimitives.ReverseEndianness(-1)); // Preferred read replica
        writer.Write(BinaryPrimitives.ReverseEndianness(0)); // Records length
    }

    private byte[] BuildErrorResponse(int correlationId, ErrorCode errorCode)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness(0)); // Throttle time
        writer.Write(BinaryPrimitives.ReverseEndianness((short)errorCode));
        writer.Write(BinaryPrimitives.ReverseEndianness(0)); // Session ID
        writer.Write(BinaryPrimitives.ReverseEndianness(0)); // Topics count

        return ms.ToArray();
    }

    private async Task WriteResponseAsync(Stream stream, byte[] response, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(sizeBuffer, response.Length);

        await stream.WriteAsync(sizeBuffer, ct);
        await stream.WriteAsync(response, ct);
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
