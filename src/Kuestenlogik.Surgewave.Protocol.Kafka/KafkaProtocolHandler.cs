using System.Buffers;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Kafka wire protocol handler implementation.
/// Handles parsing, serialization, and I/O for the Kafka binary protocol.
/// </summary>
public sealed class KafkaProtocolHandler : IProtocolHandler
{
    // Reusable 4-byte buffer for reading the message size prefix. Eliminates one
    // ArrayPool.Rent/Return per request (736K ops/sec at 368K msg/sec). Thread-safe
    // because each connection gets its own KafkaProtocolHandler instance via the
    // protocol handler factory.
    [ThreadStatic]
    private static byte[]? t_sizeBuffer;

    public string ProtocolName => "kafka";
    public string ProtocolVersion => "2.8.0"; // Compatible with Kafka 2.8.0+

    /// <summary>
    /// Parse a request from binary data
    /// </summary>
    public IProtocolRequest ParseRequest(ReadOnlySpan<byte> data)
    {
        var buffer = data.ToArray();
        using var memoryStream = new MemoryStream(buffer);
        using var reader = new BinaryReader(memoryStream);
        return ParseRequestFromReader(reader, buffer.Length);
    }

    /// <summary>
    /// Parse a response from binary data (not typically needed on broker side)
    /// </summary>
    public IProtocolResponse ParseResponse(ReadOnlySpan<byte> data)
    {
        // Response parsing is typically done on the client side
        throw new NotSupportedException("Response parsing is not supported on the broker side");
    }

    /// <summary>
    /// Read a complete request from the stream asynchronously
    /// </summary>
    public async Task<(int size, IProtocolRequest request)> ReadRequestAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        // Read size (4 bytes, big-endian) — thread-static buffer, no pool rent
        var sizeBuffer = t_sizeBuffer ??= new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer.AsMemory(0, 4), cancellationToken);
        var size = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan(0, 4));

        // Validate message size - must be positive and reasonable (max 100MB)
        const int MaxMessageSize = 100 * 1024 * 1024;
        if (size <= 0 || size > MaxMessageSize)
            throw new InvalidDataException($"Invalid Kafka request size: {size} bytes. Expected 1-{MaxMessageSize} bytes.");

        // Read request body using pooled buffer
        var requestBytes = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            await stream.ReadExactlyAsync(requestBytes.AsMemory(0, size), cancellationToken);

            // publiclyVisible: true so MemoryStream.TryGetBuffer() succeeds — zero-copy parsers
            // (ProduceRequest, FetchRequest) get a direct view into the pooled buffer.
            using var memoryStream = new MemoryStream(requestBytes, 0, size, writable: false, publiclyVisible: true);
            using var reader = new BinaryReader(memoryStream);

            var request = ParseRequestFromReader(reader, size);
            return (size, request);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(requestBytes);
        }
    }

    /// <summary>
    /// Read a complete request from the stream, retaining the rented buffer for later return.
    /// The caller is responsible for returning the buffer to ArrayPool after processing.
    /// Used by Channel-based pipeline for deferred buffer release.
    /// </summary>
    /// <returns>Tuple of (size, request, rentedBuffer). Caller must return rentedBuffer to ArrayPool.</returns>
    public async Task<(int size, IProtocolRequest request, byte[] rentedBuffer)> ReadRequestRetainingBufferAsync(
        Stream stream, CancellationToken cancellationToken = default)
    {
        // Read size (4 bytes, big-endian) — reuse thread-static buffer, no pool rent
        var sizeBuffer = t_sizeBuffer ??= new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer.AsMemory(0, 4), cancellationToken);
        var size = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(sizeBuffer.AsSpan(0, 4));

        // Validate message size
        const int MaxMessageSize = 100 * 1024 * 1024;
        if (size <= 0 || size > MaxMessageSize)
            throw new InvalidDataException($"Invalid Kafka request size: {size} bytes. Expected 1-{MaxMessageSize} bytes.");

        // Read request body - DO NOT return buffer, caller will handle it
        var requestBytes = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            await stream.ReadExactlyAsync(requestBytes.AsMemory(0, size), cancellationToken);

            // publiclyVisible: true so MemoryStream.TryGetBuffer() succeeds — zero-copy parsers
            // (ProduceRequest, FetchRequest) get a direct view into the pooled buffer.
            using var memoryStream = new MemoryStream(requestBytes, 0, size, writable: false, publiclyVisible: true);
            using var reader = new BinaryReader(memoryStream);

            var request = ParseRequestFromReader(reader, size);
            return (size, request, requestBytes);
        }
        catch
        {
            // Only return buffer on parse failure
            ArrayPool<byte>.Shared.Return(requestBytes);
            throw;
        }
    }

    /// <summary>
    /// Write a response to the stream asynchronously
    /// </summary>
    // Thread-local reusable KafkaProtocolWriter — eliminates 3M+ writer+ArrayBufferWriter
    // allocations/sec at high throughput. Reset between uses, pre-sized to 8KB.
    [ThreadStatic]
    private static KafkaProtocolWriter? t_responseWriter;

    /// <summary>
    /// Returns the thread-local KafkaProtocolWriter for response serialization.
    /// Caller must call <see cref="KafkaProtocolWriter.Reset"/> before use.
    /// </summary>
    internal static KafkaProtocolWriter GetThreadLocalWriter()
        => t_responseWriter ??= new KafkaProtocolWriter(8192);

    public async Task WriteResponseAsync(Stream stream, IProtocolResponse response, CancellationToken cancellationToken = default)
    {
        if (response is not KafkaResponse kafkaResponse)
        {
            throw new ArgumentException($"Expected KafkaResponse but got {response.GetType().Name}", nameof(response));
        }

        // Reuse thread-local writer — avoids per-response allocation
        var writer = t_responseWriter ??= new KafkaProtocolWriter(8192);
        writer.Reset();

        // Write response body into the pooled writer
        kafkaResponse.WriteTo(writer);

        // Write size prefix + response body in a single WriteAsync call.
        // No FlushAsync — TCP_NODELAY pushes data immediately; explicit flush
        // is a no-op on NetworkStream but adds an unnecessary async hop.
        var responseSpan = writer.WrittenSpan;
        var totalLength = 4 + responseSpan.Length;

        var combinedBuffer = ArrayPool<byte>.Shared.Rent(totalLength);
        try
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(combinedBuffer, responseSpan.Length);
            responseSpan.CopyTo(combinedBuffer.AsSpan(4));

            await stream.WriteAsync(combinedBuffer.AsMemory(0, totalLength), cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combinedBuffer);
        }
    }

    /// <summary>
    /// Parse the request header from a BinaryReader.
    ///
    /// Per Kafka protocol spec (RequestHeader.json), ClientId is ALWAYS serialized as a regular STRING
    /// (int16 length prefix), even in flexible header versions. This is for backward compatibility -
    /// older brokers must be able to read the header for any ApiVersionsRequest.
    ///
    /// Header v1: ApiKey, ApiVersion, CorrelationId, ClientId (STRING)
    /// Header v2: ApiKey, ApiVersion, CorrelationId, ClientId (STRING), TaggedFields
    /// </summary>
    private static (ApiKey apiKey, short apiVersion, int correlationId, string clientId) ReadRequestHeader(BinaryReader reader)
    {
        // Wrap everything in try-catch at the very top level
        string debugInfo = "not-yet-set";
        try
        {
            var stream = (MemoryStream)reader.BaseStream;
            var totalLength = stream.Length;
            var startPosition = stream.Position;

            // Build debug info first
            debugInfo = $"totalLength={totalLength}, startPosition={startPosition}";

            // Validate we have enough data for the minimum header (8 bytes: apiKey + apiVersion + correlationId)
            if (totalLength - startPosition < 8)
                throw new InvalidDataException($"Request too short for header: {totalLength - startPosition} bytes ({debugInfo})");

            var apiKey = (ApiKey)BinaryHelpers.ReadInt16BigEndian(reader);
            var apiVersion = BinaryHelpers.ReadInt16BigEndian(reader);
            var correlationId = BinaryHelpers.ReadInt32BigEndian(reader);

            // ClientId is ALWAYS a regular STRING (int16 length) in all header versions
            // per RequestHeader.json: "flexibleVersions": "none" for ClientId field
            // Need at least 2 bytes for the length prefix
            if (stream.Position + 2 > totalLength)
                throw new InvalidDataException($"Request too short for ClientId length: only {totalLength - stream.Position} bytes remaining ({debugInfo})");

            var clientId = BinaryHelpers.ReadString(reader);

            // Check if this request uses a flexible header (v2+) which has tagged fields
            // For ApiVersions v3+, the header IS flexible (has tagged fields), but ClientId is still a STRING.
            // The special handling is that ClientId remains a regular STRING for backward compatibility,
            // but the header still includes tagged fields at the end.
            bool usesFlexibleHeader = (apiKey == ApiKey.ApiVersions && apiVersion >= 3) ||
                                     (apiKey != ApiKey.ApiVersions && ProtocolVersions.IsFlexible(apiKey, apiVersion));

            if (usesFlexibleHeader)
            {
                // Read and skip tagged fields in header v2+
                var positionBefore = stream.Position;
                var remainingSize = (int)(stream.Length - stream.Position);

                // If there's data remaining, parse the tagged fields
                if (remainingSize > 0)
                {
                    // Use ArrayPool to avoid allocation
                    var bytesForHeader = ArrayPool<byte>.Shared.Rent(remainingSize);
                    try
                    {
                        stream.Read(bytesForHeader, 0, remainingSize);
                        var headerReader = new KafkaProtocolReader(bytesForHeader, remainingSize);

                        // Skip tagged fields in header
                        var headerTags = headerReader.ReadVarInt();
                        for (int i = 0; i < headerTags; i++)
                        {
                            var tag = headerReader.ReadVarInt();
                            var tagSize = headerReader.ReadVarInt();
                            // Bounds check before skipping
                            if (tagSize < 0 || tagSize > headerReader.Remaining)
                                throw new InvalidDataException($"Invalid tagged field size {tagSize}, only {headerReader.Remaining} bytes remaining");
                            headerReader.Skip(tagSize);
                        }

                        // Calculate bytes consumed for header tagged fields
                        var bytesConsumed = remainingSize - headerReader.Remaining;
                        if (bytesConsumed < 0 || bytesConsumed > remainingSize)
                            throw new InvalidDataException($"Invalid header parsing state: consumed {bytesConsumed} of {remainingSize} bytes");

                        // Set stream position to after tagged fields
                        stream.Position = positionBefore + bytesConsumed;
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(bytesForHeader);
                    }
                }
                // If remainingSize is 0, there are no tagged fields - nothing to skip
            }

            return (apiKey, apiVersion, correlationId, clientId);
        }
        catch (Exception ex)
        {
            if (ex is InvalidDataException)
                throw;
            throw new InvalidDataException($"Error parsing request header: {ex.GetType().Name}: {ex.Message} ({debugInfo})", ex);
        }
    }

    /// <summary>
    /// Parse a request from a BinaryReader
    /// </summary>
    internal static KafkaRequest ParseRequestFromReader(BinaryReader reader, int messageSize)
    {
        var (apiKey, apiVersion, correlationId, clientId) = ReadRequestHeader(reader);

        return apiKey switch
        {
            ApiKey.Produce => ProduceRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.Fetch => FetchRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.ListOffsets => ListOffsetsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.Metadata => MetadataRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.OffsetCommit => OffsetCommitRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.OffsetFetch => OffsetFetchRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.SaslHandshake => SaslHandshakeRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.ApiVersions => ReadApiVersionsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.SaslAuthenticate => SaslAuthenticateRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.InitProducerId => InitProducerIdRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.FindCoordinator => FindCoordinatorRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.JoinGroup => ReadJoinGroupRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.SyncGroup => ReadSyncGroupRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.Heartbeat => HeartbeatRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.LeaveGroup => ReadLeaveGroupRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeGroups => DescribeGroupsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.ListGroups => ListGroupsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteGroups => ReadDeleteGroupsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.CreateTopics => CreateTopicsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteTopics => DeleteTopicsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.AddPartitionsToTxn => AddPartitionsToTxnRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.AddOffsetsToTxn => AddOffsetsToTxnRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.EndTxn => EndTxnRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.TxnOffsetCommit => TxnOffsetCommitRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteRecords => DeleteRecordsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.CreatePartitions => CreatePartitionsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeConfigs => DescribeConfigsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.AlterConfigs => AlterConfigsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.IncrementalAlterConfigs => ReadIncrementalAlterConfigsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeAcls => DescribeAclsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.CreateAcls => CreateAclsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteAcls => DeleteAclsRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.CreateDelegationToken => ReadCreateDelegationTokenRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.RenewDelegationToken => ReadRenewDelegationTokenRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ExpireDelegationToken => ReadExpireDelegationTokenRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeDelegationToken => ReadDescribeDelegationTokenRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeClientQuotas => ReadDescribeClientQuotasRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.AlterClientQuotas => ReadAlterClientQuotasRequest(reader, apiVersion, correlationId, clientId),

            // Replication / Leader Epoch
            ApiKey.OffsetForLeaderEpoch => ReadOffsetForLeaderEpochRequest(reader, apiVersion, correlationId, clientId),

            // Transaction coordinator (inter-broker)
            ApiKey.WriteTxnMarkers => ReadWriteTxnMarkersRequest(reader, apiVersion, correlationId, clientId),

            // Log directory management
            ApiKey.AlterReplicaLogDirs => ReadAlterReplicaLogDirsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeLogDirs => ReadDescribeLogDirsRequest(reader, apiVersion, correlationId, clientId),

            // Leader election
            ApiKey.ElectLeaders => ReadElectLeadersRequest(reader, apiVersion, correlationId, clientId),

            // Partition reassignment
            ApiKey.AlterPartitionReassignments => ReadAlterPartitionReassignmentsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ListPartitionReassignments => ReadListPartitionReassignmentsRequest(reader, apiVersion, correlationId, clientId),

            // Consumer group offset management
            ApiKey.OffsetDelete => ReadOffsetDeleteRequest(reader, apiVersion, correlationId, clientId),

            // SCRAM credential management
            ApiKey.DescribeUserScramCredentials => ReadDescribeUserScramCredentialsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.AlterUserScramCredentials => ReadAlterUserScramCredentialsRequest(reader, apiVersion, correlationId, clientId),

            // KRaft quorum protocol
            ApiKey.Vote => ReadVoteRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.BeginQuorumEpoch => ReadBeginQuorumEpochRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.EndQuorumEpoch => ReadEndQuorumEpochRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeQuorum => ReadDescribeQuorumRequest(reader, apiVersion, correlationId, clientId),

            // KRaft broker-controller APIs
            ApiKey.AlterPartition => ReadAlterPartitionRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.UpdateFeatures => ReadUpdateFeaturesRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.Envelope => ReadEnvelopeRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.FetchSnapshot => ReadFetchSnapshotRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeCluster => DescribeClusterRequest.ReadFrom(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeProducers => ReadDescribeProducersRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.BrokerRegistration => ReadBrokerRegistrationRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.BrokerHeartbeat => ReadBrokerHeartbeatRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.UnregisterBroker => ReadUnregisterBrokerRequest(reader, apiVersion, correlationId, clientId),

            // Transaction introspection
            ApiKey.DescribeTransactions => ReadDescribeTransactionsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ListTransactions => ReadListTransactionsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.AllocateProducerIds => ReadAllocateProducerIdsRequest(reader, apiVersion, correlationId, clientId),

            // KIP-848: Next-gen consumer group protocol
            ApiKey.ConsumerGroupHeartbeat => ReadConsumerGroupHeartbeatRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ConsumerGroupDescribe => ReadConsumerGroupDescribeRequest(reader, apiVersion, correlationId, clientId),

            // KIP-714: Client telemetry
            ApiKey.GetTelemetrySubscriptions => ReadGetTelemetrySubscriptionsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.PushTelemetry => ReadPushTelemetryRequest(reader, apiVersion, correlationId, clientId),

            // JBOD / directory assignment
            ApiKey.AssignReplicasToDirs => ReadAssignReplicasToDirsRequest(reader, apiVersion, correlationId, clientId),

            // Config resources
            ApiKey.ListConfigResources => ReadListConfigResourcesRequest(reader, apiVersion, correlationId, clientId),

            // Topic partition description (paginated)
            ApiKey.DescribeTopicPartitions => ReadDescribeTopicPartitionsRequest(reader, apiVersion, correlationId, clientId),

            // KIP-932: Share Groups (Kafka 4.2)
            ApiKey.ShareGroupHeartbeat => ReadShareGroupHeartbeatRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ShareGroupDescribe => ReadShareGroupDescribeRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ShareFetch => ReadShareFetchRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ShareAcknowledge => ReadShareAcknowledgeRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DescribeShareGroupOffsets => ReadDescribeShareGroupOffsetsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.AlterShareGroupOffsets => ReadAlterShareGroupOffsetsRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteShareGroupOffsets => ReadDeleteShareGroupOffsetsRequest(reader, apiVersion, correlationId, clientId),

            // KIP-932: Share Group State (inter-broker)
            ApiKey.InitializeShareGroupState => ReadInitializeShareGroupStateRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ReadShareGroupState => ReadReadShareGroupStateRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.WriteShareGroupState => ReadWriteShareGroupStateRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.DeleteShareGroupState => ReadDeleteShareGroupStateRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.ReadShareGroupStateSummary => ReadReadShareGroupStateSummaryRequest(reader, apiVersion, correlationId, clientId),

            // KIP-853: Dynamic Raft Voters (Kafka 4.1)
            ApiKey.AddRaftVoter => ReadAddRaftVoterRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.RemoveRaftVoter => ReadRemoveRaftVoterRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.UpdateRaftVoter => ReadUpdateRaftVoterRequest(reader, apiVersion, correlationId, clientId),

            // KIP-1071: Streams Group Protocol (Kafka 4.2)
            ApiKey.StreamsGroupHeartbeat => ReadStreamsGroupHeartbeatRequest(reader, apiVersion, correlationId, clientId),
            ApiKey.StreamsGroupDescribe => ReadStreamsGroupDescribeRequest(reader, apiVersion, correlationId, clientId),

            // Controller Registration (KRaft)
            ApiKey.ControllerRegistration => ReadControllerRegistrationRequest(reader, apiVersion, correlationId, clientId),

            _ => throw new NotSupportedException($"API key {apiKey} is not supported")
        };
    }

    private static ApiVersionsRequest ReadApiVersionsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ApiVersionsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static JoinGroupRequest ReadJoinGroupRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return JoinGroupRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static SyncGroupRequest ReadSyncGroupRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return SyncGroupRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static LeaveGroupRequest ReadLeaveGroupRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return LeaveGroupRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DeleteGroupsRequest ReadDeleteGroupsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DeleteGroupsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static CreateDelegationTokenRequest ReadCreateDelegationTokenRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return CreateDelegationTokenRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static RenewDelegationTokenRequest ReadRenewDelegationTokenRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return RenewDelegationTokenRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ExpireDelegationTokenRequest ReadExpireDelegationTokenRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ExpireDelegationTokenRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeDelegationTokenRequest ReadDescribeDelegationTokenRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeDelegationTokenRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeClientQuotasRequest ReadDescribeClientQuotasRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeClientQuotasRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AlterClientQuotasRequest ReadAlterClientQuotasRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AlterClientQuotasRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static IncrementalAlterConfigsRequest ReadIncrementalAlterConfigsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return IncrementalAlterConfigsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static OffsetForLeaderEpochRequest ReadOffsetForLeaderEpochRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return OffsetForLeaderEpochRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static WriteTxnMarkersRequest ReadWriteTxnMarkersRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return WriteTxnMarkersRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AlterReplicaLogDirsRequest ReadAlterReplicaLogDirsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AlterReplicaLogDirsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeLogDirsRequest ReadDescribeLogDirsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeLogDirsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ElectLeadersRequest ReadElectLeadersRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ElectLeadersRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AlterPartitionReassignmentsRequest ReadAlterPartitionReassignmentsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AlterPartitionReassignmentsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ListPartitionReassignmentsRequest ReadListPartitionReassignmentsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ListPartitionReassignmentsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static OffsetDeleteRequest ReadOffsetDeleteRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return OffsetDeleteRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeUserScramCredentialsRequest ReadDescribeUserScramCredentialsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeUserScramCredentialsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AlterUserScramCredentialsRequest ReadAlterUserScramCredentialsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AlterUserScramCredentialsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static VoteRequest ReadVoteRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return VoteRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static BeginQuorumEpochRequest ReadBeginQuorumEpochRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return BeginQuorumEpochRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static EndQuorumEpochRequest ReadEndQuorumEpochRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return EndQuorumEpochRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeQuorumRequest ReadDescribeQuorumRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeQuorumRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AlterPartitionRequest ReadAlterPartitionRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AlterPartitionRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static UpdateFeaturesRequest ReadUpdateFeaturesRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return UpdateFeaturesRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static EnvelopeRequest ReadEnvelopeRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return EnvelopeRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static FetchSnapshotRequest ReadFetchSnapshotRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return FetchSnapshotRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeProducersRequest ReadDescribeProducersRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeProducersRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static BrokerRegistrationRequest ReadBrokerRegistrationRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return BrokerRegistrationRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static BrokerHeartbeatRequest ReadBrokerHeartbeatRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return BrokerHeartbeatRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static UnregisterBrokerRequest ReadUnregisterBrokerRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return UnregisterBrokerRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeTransactionsRequest ReadDescribeTransactionsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeTransactionsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ListTransactionsRequest ReadListTransactionsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ListTransactionsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AllocateProducerIdsRequest ReadAllocateProducerIdsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AllocateProducerIdsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ConsumerGroupHeartbeatRequest ReadConsumerGroupHeartbeatRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ConsumerGroupHeartbeatRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ConsumerGroupDescribeRequest ReadConsumerGroupDescribeRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ConsumerGroupDescribeRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static GetTelemetrySubscriptionsRequest ReadGetTelemetrySubscriptionsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return GetTelemetrySubscriptionsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static PushTelemetryRequest ReadPushTelemetryRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return PushTelemetryRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static AssignReplicasToDirsRequest ReadAssignReplicasToDirsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return AssignReplicasToDirsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static ListConfigResourcesRequest ReadListConfigResourcesRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return ListConfigResourcesRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    private static DescribeTopicPartitionsRequest ReadDescribeTopicPartitionsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);

        var protocolReader = new KafkaProtocolReader(remainingBytes);
        return DescribeTopicPartitionsRequest.ReadFrom(protocolReader, apiVersion, correlationId, clientId);
    }

    // KIP-932: Share Groups
    private static ShareGroupHeartbeatRequest ReadShareGroupHeartbeatRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ShareGroupHeartbeatRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static ShareGroupDescribeRequest ReadShareGroupDescribeRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ShareGroupDescribeRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static ShareFetchRequest ReadShareFetchRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ShareFetchRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static ShareAcknowledgeRequest ReadShareAcknowledgeRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ShareAcknowledgeRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static DescribeShareGroupOffsetsRequest ReadDescribeShareGroupOffsetsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return DescribeShareGroupOffsetsRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static AlterShareGroupOffsetsRequest ReadAlterShareGroupOffsetsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return AlterShareGroupOffsetsRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static DeleteShareGroupOffsetsRequest ReadDeleteShareGroupOffsetsRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return DeleteShareGroupOffsetsRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    // KIP-932: Share Group State (inter-broker)
    private static InitializeShareGroupStateRequest ReadInitializeShareGroupStateRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return InitializeShareGroupStateRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static ReadShareGroupStateRequest ReadReadShareGroupStateRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ReadShareGroupStateRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static WriteShareGroupStateRequest ReadWriteShareGroupStateRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return WriteShareGroupStateRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static DeleteShareGroupStateRequest ReadDeleteShareGroupStateRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return DeleteShareGroupStateRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static ReadShareGroupStateSummaryRequest ReadReadShareGroupStateSummaryRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ReadShareGroupStateSummaryRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    // KIP-853: Dynamic Raft Voters
    private static AddRaftVoterRequest ReadAddRaftVoterRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return AddRaftVoterRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static RemoveRaftVoterRequest ReadRemoveRaftVoterRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return RemoveRaftVoterRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static UpdateRaftVoterRequest ReadUpdateRaftVoterRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return UpdateRaftVoterRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    // KIP-1071: Streams Group Protocol
    private static StreamsGroupHeartbeatRequest ReadStreamsGroupHeartbeatRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return StreamsGroupHeartbeatRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    private static StreamsGroupDescribeRequest ReadStreamsGroupDescribeRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return StreamsGroupDescribeRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }

    // Controller Registration (KRaft)
    private static ControllerRegistrationRequest ReadControllerRegistrationRequest(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        var stream = (MemoryStream)reader.BaseStream;
        var remainingBytes = reader.ReadBytes((int)(stream.Length - stream.Position));
        return ControllerRegistrationRequest.ReadFrom(new KafkaProtocolReader(remainingBytes), apiVersion, correlationId, clientId);
    }
}
