namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// FindCoordinator request (v0-6) - Find the coordinator for a consumer group or transaction
/// </summary>
public sealed class FindCoordinatorRequest : KafkaRequest
{
    /// <summary>Key to look up (v0-3), ignored for v4+ batch requests</summary>
    public string? Key { get; init; }
    /// <summary>0 = Group, 1 = Transaction (v1+)</summary>
    public byte KeyType { get; init; }
    /// <summary>Batch lookup keys (v4+)</summary>
    public List<string>? CoordinatorKeys { get; init; }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteCompactString(ClientId);
            writer.WriteVarInt(0); // Header tagged fields
        }
        else
        {
            writer.WriteString(ClientId);
        }

        // Key field (v0-3 only, ignored in v4+ when CoordinatorKeys is used)
        if (ApiVersion < 4)
        {
            if (isFlexible)
                writer.WriteCompactString(Key);
            else
                writer.WriteString(Key ?? string.Empty);
        }

        // KeyType (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt8((sbyte)KeyType);
        }

        // CoordinatorKeys array (v4+)
        if (ApiVersion >= 4)
        {
            var keys = CoordinatorKeys ?? (Key != null ? [Key] : []);
            writer.WriteVarInt(keys.Count + 1); // COMPACT_ARRAY
            foreach (var key in keys)
            {
                writer.WriteCompactString(key);
            }
        }

        // Body tagged fields (v3+)
        if (isFlexible)
        {
            writer.WriteVarInt(0);
        }
    }

    public static FindCoordinatorRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.FindCoordinator, apiVersion);

        // Get remaining bytes for KafkaProtocolReader
        var stream = (MemoryStream)reader.BaseStream;
        var remainingSize = (int)(stream.Length - stream.Position);
        var remainingBytes = reader.ReadBytes(remainingSize);
        var protocolReader = new KafkaProtocolReader(remainingBytes);

        string? key = null;
        byte keyType = 0; // Default to Group
        List<string>? coordinatorKeys = null;

        // Key field (v0-3 only)
        if (apiVersion < 4)
        {
            key = isFlexible ? protocolReader.ReadCompactString() : protocolReader.ReadString();
        }

        // KeyType (v1+)
        if (apiVersion >= 1)
        {
            keyType = (byte)protocolReader.ReadInt8();
        }

        // CoordinatorKeys array (v4+)
        if (apiVersion >= 4)
        {
            var keyCount = protocolReader.ReadVarInt() - 1; // COMPACT_ARRAY
            if (keyCount >= 0)
            {
                coordinatorKeys = new List<string>(keyCount);
                for (int i = 0; i < keyCount; i++)
                {
                    var k = protocolReader.ReadCompactString();
                    if (k != null)
                        coordinatorKeys.Add(k);
                }
            }
        }

        // Body tagged fields (v3+)
        if (isFlexible)
        {
            var tagCount = protocolReader.ReadVarInt();
            for (int i = 0; i < tagCount; i++)
            {
                var tag = protocolReader.ReadVarInt();
                var size = protocolReader.ReadVarInt();
                protocolReader.Skip(size);
            }
        }

        return new FindCoordinatorRequest
        {
            ApiKey = ApiKey.FindCoordinator,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            Key = key ?? coordinatorKeys?.FirstOrDefault(),
            KeyType = keyType,
            CoordinatorKeys = coordinatorKeys
        };
    }

}

/// <summary>
/// FindCoordinator response (v0-6)
/// </summary>
public sealed class FindCoordinatorResponse : KafkaResponse
{
    /// <summary>Error code (v0-3, deprecated in v4+)</summary>
    public ErrorCode ErrorCode { get; init; }
    /// <summary>Error message (v1-3, deprecated in v4+)</summary>
    public string? ErrorMessage { get; init; }
    /// <summary>Node ID (v0-3, deprecated in v4+)</summary>
    public int NodeId { get; init; }
    /// <summary>Host (v0-3, deprecated in v4+)</summary>
    public string? Host { get; init; }
    /// <summary>Port (v0-3, deprecated in v4+)</summary>
    public int Port { get; init; }
    public int ThrottleTimeMs { get; init; }
    /// <summary>Batch coordinator results (v4+)</summary>
    public List<Coordinator>? Coordinators { get; init; }

    public sealed class Coordinator
    {
        public required string Key { get; init; }
        public required int NodeId { get; init; }
        public required string Host { get; init; }
        public required int Port { get; init; }
        public ErrorCode ErrorCode { get; init; }
        public string? ErrorMessage { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        // Response header
        writer.WriteInt32(CorrelationId);
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Header tagged fields
        }

        // ThrottleTimeMs (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        // For v4+, use Coordinators array
        if (ApiVersion >= 4)
        {
            var coordinators = Coordinators ?? [];

            // If no Coordinators provided but legacy fields are set, create one
            if (coordinators.Count == 0 && Host != null)
            {
                coordinators =
                [
                    new Coordinator
                    {
                        Key = string.Empty,
                        NodeId = NodeId,
                        Host = Host,
                        Port = Port,
                        ErrorCode = ErrorCode,
                        ErrorMessage = ErrorMessage
                    }
                ];
            }

            writer.WriteVarInt(coordinators.Count + 1); // COMPACT_ARRAY
            foreach (var coord in coordinators)
            {
                writer.WriteCompactString(coord.Key);
                writer.WriteInt32(coord.NodeId);
                writer.WriteCompactString(coord.Host);
                writer.WriteInt32(coord.Port);
                writer.WriteInt16((short)coord.ErrorCode);
                writer.WriteCompactString(coord.ErrorMessage);
                writer.WriteVarInt(0); // Coordinator tagged fields
            }
        }
        else
        {
            // Legacy format (v0-3)
            writer.WriteInt16((short)ErrorCode);

            if (ApiVersion >= 1)
            {
                if (isFlexible)
                    writer.WriteCompactString(ErrorMessage);
                else
                    writer.WriteString(ErrorMessage ?? string.Empty);
            }

            writer.WriteInt32(NodeId);

            if (isFlexible)
                writer.WriteCompactString(Host);
            else
                writer.WriteString(Host ?? string.Empty);

            writer.WriteInt32(Port);
        }

        // Body tagged fields (v3+)
        if (isFlexible)
        {
            writer.WriteVarInt(0);
        }
    }
}
