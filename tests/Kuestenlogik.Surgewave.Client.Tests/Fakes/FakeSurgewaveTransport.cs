using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;
using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Client.Tests.Fakes;

/// <summary>
/// Deterministic in-memory broker behind <see cref="ISurgewaveTransport"/> (#102).
/// Speaks the native wire format for the op-codes the client facades and the
/// performance components exercise (Produce, Fetch, ListOffsets, ListTopics,
/// consumer-group ops, Ping) so <c>SurgewaveNativeClient</c> runs unmodified
/// against it. Every Fetch/Produce/Commit request is recorded for assertions,
/// and awaitable hooks let tests gate requests to pin ordering and race
/// behaviour without real timing dependencies.
/// </summary>
internal sealed class FakeSurgewaveTransport : ISurgewaveTransport
{
    private sealed class PartitionLog
    {
        public readonly List<StoredMessage> Messages = [];
        public long EarliestOffset;
        public long NextOffset;
    }

    internal sealed record StoredMessage(
        long Offset,
        long Timestamp,
        byte[]? Key,
        byte[] Value,
        IReadOnlyDictionary<string, byte[]>? Headers);

    internal sealed record FetchRequest(string Topic, int Partition, long Offset, int MaxBytes, int MaxWaitMs);
    internal sealed record ProduceRequest(string Topic, int Partition, int MessageCount, long BaseOffset);
    internal sealed record CommitRequest(string GroupId, string Topic, int Partition, long Offset);

    private readonly Lock _gate = new();
    private readonly Dictionary<(string Topic, int Partition), PartitionLog> _logs = [];
    private readonly Dictionary<string, int> _topicPartitions = [];
    private readonly Dictionary<(string Group, string Topic, int Partition), long> _committed = [];
    private uint _requestId;
    private int _memberCounter;
    private int _generationId;
    private bool _connected;
    private bool _failNextRequest;

    private readonly List<FetchRequest> _fetchRequests = [];
    private readonly List<ProduceRequest> _produceRequests = [];
    private readonly List<CommitRequest> _commitRequests = [];

    // Snapshot copies: a background prefetch may append concurrently with a test
    // enumerating — handing out the live list would be an un-locked enumeration race.

    /// <summary>All Fetch requests the client issued, in order (snapshot).</summary>
    public IReadOnlyList<FetchRequest> FetchRequests { get { lock (_gate) return _fetchRequests.ToList(); } }

    /// <summary>All Produce requests the client issued, in order (snapshot).</summary>
    public IReadOnlyList<ProduceRequest> ProduceRequests { get { lock (_gate) return _produceRequests.ToList(); } }

    /// <summary>All offset commits the client issued, in order (snapshot).</summary>
    public IReadOnlyList<CommitRequest> CommitRequests { get { lock (_gate) return _commitRequests.ToList(); } }

    public int FetchCount { get { lock (_gate) return _fetchRequests.Count; } }
    public int ProduceCount { get { lock (_gate) return _produceRequests.Count; } }

    /// <summary>
    /// Awaited before each Fetch is served (outside the state lock). Lets tests
    /// gate background prefetches deterministically.
    /// </summary>
    public Func<FetchRequest, Task>? OnFetchAsync { get; set; }

    /// <summary>
    /// Awaited before each Produce is applied (outside the state lock). Lets tests
    /// hold a batch in flight to prove ordering under maxInFlight limits.
    /// </summary>
    public Func<ProduceRequest, Task>? OnProduceAsync { get; set; }

    /// <summary>
    /// Error code returned in the next Heartbeat PAYLOAD, then reset to 0. Set to
    /// RebalanceInProgress to drive the facade's background rejoin path.
    /// NOTE: synthetic trigger. The real broker currently never emits
    /// RebalanceInProgress on heartbeat, and real errors arrive in the response
    /// HEADER (which makes the client throw before payload parsing) — this knob
    /// exists to exercise the client-internal rejoin/discard logic
    /// deterministically. The broker-driven end-to-end flow is tracked in the
    /// rebalance-threading issue.
    /// </summary>
    public ushort NextHeartbeatErrorCode { get; set; }

    /// <summary>Number of JoinGroup requests served — a rejoin shows up as a second join.</summary>
    public int JoinGroupCount { get { lock (_gate) return _joinGroupCount; } }
    private int _joinGroupCount;

    public SurgewaveTransportType TransportType => SurgewaveTransportType.Tcp;
    public bool IsConnected => _connected;
    public bool ServerSupportsCompression => false;

    public ValueTask ConnectAsync(CancellationToken cancellationToken = default)
    {
        _connected = true;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Makes the next request fail with an <see cref="IOException"/> and marks the
    /// transport disconnected — drives the facade's reconnect path.
    /// </summary>
    public void SimulateConnectionLoss()
    {
        lock (_gate) _failNextRequest = true;
    }

    public void CreateTopic(string topic, int partitions = 1)
    {
        lock (_gate)
        {
            _topicPartitions[topic] = partitions;
            for (int p = 0; p < partitions; p++)
                _logs.TryAdd((topic, p), new PartitionLog());
        }
    }

    /// <summary>Seeds a message directly into the log (no Produce request recorded).</summary>
    public long Append(string topic, int partition, byte[]? key, byte[] value, IReadOnlyDictionary<string, byte[]>? headers = null)
    {
        lock (_gate)
        {
            var log = GetOrCreateLog(topic, partition);
            var offset = log.NextOffset++;
            log.Messages.Add(new StoredMessage(offset, 1_700_000_000_000 + offset, key, value, headers));
            return offset;
        }
    }

    /// <summary>
    /// Simulates retention deleting everything below <paramref name="earliestOffset"/>.
    /// </summary>
    public void SetEarliestOffset(string topic, int partition, long earliestOffset)
    {
        lock (_gate)
        {
            var log = GetOrCreateLog(topic, partition);
            log.EarliestOffset = earliestOffset;
            log.Messages.RemoveAll(m => m.Offset < earliestOffset);
            if (log.NextOffset < earliestOffset) log.NextOffset = earliestOffset;
        }
    }

    public IReadOnlyList<StoredMessage> GetLog(string topic, int partition)
    {
        lock (_gate) return GetOrCreateLog(topic, partition).Messages.ToList();
    }

    public long? GetCommitted(string group, string topic, int partition)
    {
        lock (_gate) return _committed.TryGetValue((group, topic, partition), out var o) ? o : null;
    }

    private PartitionLog GetOrCreateLog(string topic, int partition)
    {
        if (!_logs.TryGetValue((topic, partition), out var log))
        {
            log = new PartitionLog();
            _logs[(topic, partition)] = log;
            _topicPartitions.TryAdd(topic, partition + 1);
            if (_topicPartitions[topic] < partition + 1) _topicPartitions[topic] = partition + 1;
        }
        return log;
    }

    public async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress = true,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_failNextRequest)
            {
                _failNextRequest = false;
                _connected = false;
                throw new IOException("FakeSurgewaveTransport: simulated connection loss");
            }
        }

        if (!_connected)
            throw new IOException("FakeSurgewaveTransport: not connected");

        // Record + hook phase for gated ops. Parsing must stay synchronous
        // (SurgewavePayloadReader is a ref struct), so hooks run before the
        // synchronous handler.
        switch (opCode)
        {
            case SurgewaveOpCode.Fetch when OnFetchAsync != null:
                await OnFetchAsync(PeekFetchRequest(payload.Span));
                break;
            case SurgewaveOpCode.Produce when OnProduceAsync != null:
                await OnProduceAsync(PeekProduceRequest(payload.Span));
                break;
        }

        cancellationToken.ThrowIfCancellationRequested();

        byte[] response = Handle(opCode, payload.Span);
        var header = new SurgewaveResponseHeader
        {
            Flags = 0,
            RequestId = ++_requestId,
            OpCode = opCode,
            ErrorCode = SurgewaveErrorCode.None,
            PayloadLength = response.Length
        };
        return (header, response);
    }

    private static FetchRequest PeekFetchRequest(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        return new FetchRequest(
            reader.ReadString() ?? "",
            reader.ReadInt32(),
            reader.ReadInt64(),
            reader.ReadInt32(),
            reader.ReadInt32());
    }

    private static ProduceRequest PeekProduceRequest(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var count = reader.ReadInt32();
        return new ProduceRequest(topic, partition, count, BaseOffset: -1);
    }

    private byte[] Handle(SurgewaveOpCode opCode, ReadOnlySpan<byte> payload)
    {
        lock (_gate)
        {
            return opCode switch
            {
                SurgewaveOpCode.Produce => HandleProduce(payload),
                SurgewaveOpCode.Fetch => HandleFetch(payload),
                SurgewaveOpCode.ListOffsets => HandleListOffsets(payload),
                SurgewaveOpCode.ListTopics => HandleListTopics(),
                SurgewaveOpCode.JoinGroup => HandleJoinGroup(payload),
                SurgewaveOpCode.SyncGroup => HandleSyncGroup(payload),
                SurgewaveOpCode.Heartbeat => HandleHeartbeat(),
                SurgewaveOpCode.LeaveGroup => HandleLeaveGroup(),
                SurgewaveOpCode.CommitOffset => HandleCommitOffset(payload),
                SurgewaveOpCode.FetchOffset => HandleFetchOffset(payload),
                SurgewaveOpCode.Ping => HandlePing(),
                _ => throw new InvalidOperationException($"FakeSurgewaveTransport: unhandled op-code {opCode}")
            };
        }
    }

    private byte[] HandleProduce(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var count = reader.ReadInt32();

        var log = GetOrCreateLog(topic, partition);
        var baseOffset = log.NextOffset;

        for (int i = 0; i < count; i++)
        {
            var keyLength = reader.ReadInt32();
            byte[]? key = keyLength >= 0 ? reader.ReadRaw(keyLength).ToArray() : null;
            var valueLength = reader.ReadInt32();
            var value = valueLength > 0 ? reader.ReadRaw(valueLength).ToArray() : [];
            var headers = NativeMessageHeaderCodec.Decode(payload[reader.Position..], out var headerBytes);
            reader.Skip(headerBytes);

            var offset = log.NextOffset++;
            log.Messages.Add(new StoredMessage(offset, 1_700_000_000_000 + offset, key, value, headers));
        }

        _produceRequests.Add(new ProduceRequest(topic, partition, count, baseOffset));

        var response = new byte[8];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteInt64(baseOffset);
        return response;
    }

    private byte[] HandleFetch(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var offset = reader.ReadInt64();
        var maxBytes = reader.ReadInt32();
        var maxWaitMs = reader.ReadInt32();

        _fetchRequests.Add(new FetchRequest(topic, partition, offset, maxBytes, maxWaitMs));

        var log = GetOrCreateLog(topic, partition);

        // Retention gap: requested data no longer exists → empty result whose
        // high-watermark exceeds the requested offset, which drives the
        // facade's jump-to-latest path.
        var selected = new List<StoredMessage>();
        if (offset >= log.EarliestOffset)
        {
            var size = 0;
            foreach (var msg in log.Messages)
            {
                if (msg.Offset < offset) continue;
                var msgSize = 8 + 8 + 4 + (msg.Key?.Length ?? 0) + 4 + msg.Value.Length
                              + NativeMessageHeaderCodec.EncodedSize(msg.Headers);
                if (selected.Count > 0 && size + msgSize > maxBytes) break;
                selected.Add(msg);
                size += msgSize;
            }
        }

        var bufferSize = 8 + 4;
        foreach (var msg in selected)
            bufferSize += 8 + 8 + 4 + (msg.Key?.Length ?? 0) + 4 + msg.Value.Length
                          + NativeMessageHeaderCodec.EncodedSize(msg.Headers);

        var response = new byte[bufferSize];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteInt64(log.NextOffset); // high watermark = next offset to write
        writer.WriteInt32(selected.Count);
        foreach (var msg in selected)
        {
            writer.WriteInt64(msg.Offset);
            writer.WriteInt64(msg.Timestamp);
            if (msg.Key is { Length: > 0 })
            {
                writer.WriteInt32(msg.Key.Length);
                writer.WriteRaw(msg.Key);
            }
            else
            {
                writer.WriteInt32(-1);
            }
            writer.WriteBytes(msg.Value);
            var headerBytes = NativeMessageHeaderCodec.Encode(msg.Headers, response.AsSpan(writer.Position));
            writer.Advance(headerBytes);
        }
        return response[..writer.Position];
    }

    private byte[] HandleListOffsets(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var timestamp = reader.ReadInt64();

        // Auto-creates the topic like the real broker with AutoCreateTopics on.
        var log = GetOrCreateLog(topic, partition);
        var result = timestamp switch
        {
            -2 => log.EarliestOffset,
            -1 => log.NextOffset,
            _ => log.Messages.FirstOrDefault(m => m.Timestamp >= timestamp)?.Offset ?? log.NextOffset
        };

        var response = new byte[8];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteInt64(result);
        return response;
    }

    private byte[] HandleListTopics()
    {
        var topics = _topicPartitions
            .Select(kvp => new TopicInfoPayload { Name = kvp.Key, PartitionCount = kvp.Value, Strategy = default })
            .ToArray();
        var payload = new ListTopicsResponsePayload { Topics = topics };
        var response = new byte[payload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(response);
        payload.Write(ref writer);
        return response[..writer.Position];
    }

    private byte[] HandleHeartbeat()
    {
        var error = NextHeartbeatErrorCode;
        NextHeartbeatErrorCode = 0;
        var response = new byte[2];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteUInt16(error);
        return response;
    }

    private byte[] HandleJoinGroup(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var request = JoinGroupRequestPayload.Read(ref reader);
        _joinGroupCount++;

        var memberId = string.IsNullOrEmpty(request.MemberId)
            ? $"fake-member-{++_memberCounter}"
            : request.MemberId!;
        var generation = ++_generationId;
        var protocol = request.Protocols.Length > 0 ? request.Protocols[0] : new GroupProtocol("range", []);

        var responsePayload = new JoinGroupResponsePayload
        {
            ErrorCode = 0,
            GenerationId = generation,
            ProtocolName = protocol.Name,
            LeaderId = memberId,
            MemberId = memberId,
            Members = [new JoinGroupMemberPayload { MemberId = memberId, GroupInstanceId = null, Metadata = protocol.Metadata }]
        };
        var response = new byte[responsePayload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(response);
        responsePayload.Write(ref writer);
        return response[..writer.Position];
    }

    private static byte[] HandleSyncGroup(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var request = SyncGroupRequestPayload.Read(ref reader);

        // Single-member group: the joining member is always the leader, so its
        // own assignment is in the request. Mirror it back.
        var assignment = Array.Empty<byte>();
        foreach (var a in request.Assignments)
        {
            if (a.MemberId == request.MemberId)
            {
                assignment = a.Assignment;
                break;
            }
        }

        var responsePayload = new SyncGroupResponsePayload { ErrorCode = 0, Assignment = assignment };
        var response = new byte[responsePayload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(response);
        responsePayload.Write(ref writer);
        return response[..writer.Position];
    }

    private static byte[] HandleUInt16Ok()
    {
        var response = new byte[2];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteUInt16(0);
        return response;
    }

    private static byte[] HandleLeaveGroup()
    {
        var responsePayload = new LeaveGroupResponsePayload { ErrorCode = 0 };
        var response = new byte[responsePayload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(response);
        responsePayload.Write(ref writer);
        return response[..writer.Position];
    }

    private byte[] HandleCommitOffset(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var groupId = reader.ReadString() ?? "";
        _ = reader.ReadString(); // memberId
        _ = reader.ReadInt32();  // generationId
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();
        var offset = reader.ReadInt64();

        _committed[(groupId, topic, partition)] = offset;
        _commitRequests.Add(new CommitRequest(groupId, topic, partition, offset));
        return HandleUInt16Ok();
    }

    private byte[] HandleFetchOffset(ReadOnlySpan<byte> payload)
    {
        var reader = new SurgewavePayloadReader(payload);
        var groupId = reader.ReadString() ?? "";
        var topic = reader.ReadString() ?? "";
        var partition = reader.ReadInt32();

        var committed = _committed.TryGetValue((groupId, topic, partition), out var o) ? o : -1;

        var response = new byte[10];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteUInt16(0);
        writer.WriteInt64(committed);
        return response;
    }

    private static byte[] HandlePing()
    {
        var response = new byte[8];
        var writer = new SurgewavePayloadWriter(response);
        writer.WriteInt64(1_700_000_000_000);
        return response;
    }

    public void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
    {
        // No server-push in the fake; registration is a no-op.
    }

    public void UnregisterPushHandler(SurgewaveOpCode opCode)
    {
    }

    public ValueTask DisposeAsync()
    {
        _connected = false;
        return ValueTask.CompletedTask;
    }
}
