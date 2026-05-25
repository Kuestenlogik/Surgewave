namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// Kafka ListGroups request (API Key 16)
/// Lists all consumer groups on the broker.
/// </summary>
public sealed class ListGroupsRequest : KafkaRequest
{
    public List<string>? StatesFilter { get; init; } // v4+

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);
        // No body for v0-v3
    }

    public static ListGroupsRequest ReadFrom(BinaryReader reader, short apiVersion, int correlationId, string clientId)
    {
        bool isFlexible = apiVersion >= 3;
        List<string>? statesFilter = null;

        if (isFlexible)
        {
            var stream = (MemoryStream)reader.BaseStream;
            var remainingBytes = new byte[stream.Length - stream.Position];
            stream.ReadExactly(remainingBytes, 0, remainingBytes.Length);
            var protocolReader = new KafkaProtocolReader(remainingBytes);

            // v4+ has StatesFilter
            if (apiVersion >= 4)
            {
                var stateCount = protocolReader.ReadVarInt() - 1;
                if (stateCount > 0)
                {
                    statesFilter = new List<string>();
                    for (int i = 0; i < stateCount; i++)
                    {
                        var state = protocolReader.ReadCompactString();
                        if (state != null)
                        {
                            statesFilter.Add(state);
                        }
                    }
                }
            }

            protocolReader.ReadVarInt(); // tagged fields
        }
        // v0-v2 have no body

        return new ListGroupsRequest
        {
            ApiKey = ApiKey.ListGroups,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            StatesFilter = statesFilter
        };
    }
}

/// <summary>
/// Kafka ListGroups response
/// </summary>
public sealed class ListGroupsResponse : KafkaResponse
{
    public int ThrottleTimeMs { get; init; } // v1+
    public required ErrorCode ErrorCode { get; init; }
    public required List<ListedGroup> Groups { get; init; }

    public sealed class ListedGroup
    {
        public required string GroupId { get; init; }
        public required string ProtocolType { get; init; }
        public string? GroupState { get; init; } // v4+
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        bool isFlexible = ApiVersion >= 3;

        writer.WriteInt32(CorrelationId);

        if (isFlexible)
        {
            writer.WriteVarInt(0); // header tagged fields
        }

        // ThrottleTimeMs (v1+)
        if (ApiVersion >= 1)
        {
            writer.WriteInt32(ThrottleTimeMs);
        }

        writer.WriteInt16((short)ErrorCode);

        if (isFlexible)
        {
            writer.WriteVarInt(Groups.Count + 1); // COMPACT_ARRAY
            foreach (var group in Groups)
            {
                writer.WriteCompactString(group.GroupId);
                writer.WriteCompactString(group.ProtocolType);
                if (ApiVersion >= 4)
                {
                    writer.WriteCompactString(group.GroupState);
                }
                writer.WriteVarInt(0); // group tagged fields
            }
        }
        else
        {
            writer.WriteInt32(Groups.Count);
            foreach (var group in Groups)
            {
                writer.WriteString(group.GroupId);
                writer.WriteString(group.ProtocolType);
            }
        }

        if (isFlexible)
        {
            writer.WriteVarInt(0); // response tagged fields
        }
    }
}
