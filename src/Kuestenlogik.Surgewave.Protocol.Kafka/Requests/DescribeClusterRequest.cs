namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka DescribeCluster request (v0-2)
/// API Key: 60
/// Returns cluster metadata including brokers, controller, and cluster ID.
/// All versions are flexible (use compact arrays and tagged fields).
///
/// Version 1 adds EndpointType for KIP-919 support.
/// Version 2 adds IncludeFencedBrokers for KIP-1073 support.
/// </summary>
public sealed class DescribeClusterRequest : KafkaRequest
{
    /// <summary>
    /// Whether to include cluster authorized operations (v0+)
    /// </summary>
    public bool IncludeClusterAuthorizedOperations { get; init; }

    /// <summary>
    /// The endpoint type to describe. 1=brokers, 2=controllers (v1+)
    /// </summary>
    public sbyte EndpointType { get; init; } = 1;

    /// <summary>
    /// Whether to include fenced brokers when listing brokers (v2+)
    /// </summary>
    public bool IncludeFencedBrokers { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Request Header v2 (flexible)
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteCompactString(ClientId);
        writer.WriteVarInt(0); // Header tagged fields

        // IncludeClusterAuthorizedOperations
        writer.WriteInt8((sbyte)(IncludeClusterAuthorizedOperations ? 1 : 0));

        // EndpointType (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt8(EndpointType);
        }

        // IncludeFencedBrokers (v2+)
        if (ApiVersion >= 2)
        {
            writer.WriteInt8((sbyte)(IncludeFencedBrokers ? 1 : 0));
        }

        // Body tagged fields
        writer.WriteVarInt(0);
    }

    public static DescribeClusterRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        // Get remaining bytes for KafkaProtocolReader
        var stream = (System.IO.MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);
        var protocolReader = new KafkaProtocolReader(remainingBytes);

        // IncludeClusterAuthorizedOperations
        var includeClusterAuthorizedOperations = protocolReader.ReadInt8() != 0;

        // EndpointType (v1+)
        sbyte endpointType = 1; // Default to brokers
        if (apiVersion >= 1)
        {
            endpointType = protocolReader.ReadInt8();
        }

        // IncludeFencedBrokers (v2+)
        bool includeFencedBrokers = false;
        if (apiVersion >= 2)
        {
            includeFencedBrokers = protocolReader.ReadInt8() != 0;
        }

        // Body tagged fields
        var tagCount = protocolReader.ReadVarInt();
        for (int i = 0; i < tagCount; i++)
        {
            var tag = protocolReader.ReadVarInt();
            var size = protocolReader.ReadVarInt();
            protocolReader.Skip(size);
        }

        return new DescribeClusterRequest
        {
            ApiKey = ApiKey.DescribeCluster,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            IncludeClusterAuthorizedOperations = includeClusterAuthorizedOperations,
            EndpointType = endpointType,
            IncludeFencedBrokers = includeFencedBrokers
        };
    }
}

/// <summary>
/// Kafka DescribeCluster response (v0-2)
///
/// Version 1 adds the EndpointType field.
/// Version 2 adds IsFenced field to Brokers for KIP-1073 support.
/// </summary>
public sealed class DescribeClusterResponse : KafkaResponse
{
    /// <summary>
    /// Duration in milliseconds for which the request was throttled (v0+)
    /// </summary>
    public int ThrottleTimeMs { get; init; }

    /// <summary>
    /// The top-level error code, or 0 if there was no error (v0+)
    /// </summary>
    public ErrorCode ErrorCode { get; init; } = ErrorCode.None;

    /// <summary>
    /// The top-level error message, or null if there was no error (v0+)
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// The endpoint type that was described. 1=brokers, 2=controllers (v1+)
    /// </summary>
    public sbyte EndpointType { get; init; } = 1;

    /// <summary>
    /// The cluster ID that responding broker belongs to (v0+)
    /// </summary>
    public required string ClusterId { get; init; }

    /// <summary>
    /// The ID of the controller broker (v0+)
    /// </summary>
    public int ControllerId { get; init; } = -1;

    /// <summary>
    /// Each broker in the response (v0+)
    /// </summary>
    public required List<DescribeClusterBroker> Brokers { get; init; }

    /// <summary>
    /// 32-bit bitfield to represent authorized operations for this cluster (v0+)
    /// </summary>
    public int ClusterAuthorizedOperations { get; init; } = int.MinValue;

    public sealed class DescribeClusterBroker
    {
        /// <summary>
        /// The broker ID (v0+)
        /// </summary>
        public required int BrokerId { get; init; }

        /// <summary>
        /// The broker hostname (v0+)
        /// </summary>
        public required string Host { get; init; }

        /// <summary>
        /// The broker port (v0+)
        /// </summary>
        public required int Port { get; init; }

        /// <summary>
        /// The rack of the broker, or null if it has not been assigned to a rack (v0+)
        /// </summary>
        public string? Rack { get; init; }

        /// <summary>
        /// Whether the broker is fenced (v2+)
        /// </summary>
        public bool IsFenced { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response Header v1 (flexible)
        writer.WriteInt32(CorrelationId);
        writer.WriteVarInt(0); // Response header tagged fields

        // ThrottleTimeMs
        writer.WriteInt32(ThrottleTimeMs);

        // ErrorCode
        writer.WriteInt16((short)ErrorCode);

        // ErrorMessage (nullable compact string)
        writer.WriteCompactString(ErrorMessage);

        // EndpointType (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt8(EndpointType);
        }

        // ClusterId
        writer.WriteCompactString(ClusterId);

        // ControllerId
        writer.WriteInt32(ControllerId);

        // Brokers (compact array)
        writer.WriteVarInt(Brokers.Count + 1);
        foreach (var broker in Brokers)
        {
            writer.WriteInt32(broker.BrokerId);
            writer.WriteCompactString(broker.Host);
            writer.WriteInt32(broker.Port);
            writer.WriteCompactString(broker.Rack);

            // IsFenced (v2+)
            if (ApiVersion >= 2)
            {
                writer.WriteInt8((sbyte)(broker.IsFenced ? 1 : 0));
            }

            // Broker tagged fields
            writer.WriteVarInt(0);
        }

        // ClusterAuthorizedOperations
        writer.WriteInt32(ClusterAuthorizedOperations);

        // Body tagged fields
        writer.WriteVarInt(0);
    }
}
