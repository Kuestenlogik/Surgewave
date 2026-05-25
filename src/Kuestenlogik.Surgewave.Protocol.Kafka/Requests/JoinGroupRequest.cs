namespace Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

/// <summary>
/// JoinGroup request - Join a consumer group
/// </summary>
public sealed class JoinGroupRequest : KafkaRequest
{
    public required string GroupId { get; init; }
    public required int SessionTimeoutMs { get; init; }
    public required int RebalanceTimeoutMs { get; init; }
    public required string MemberId { get; init; }
    public string? GroupInstanceId { get; init; }
    public required string ProtocolType { get; init; }
    public required GroupProtocol[] Protocols { get; init; }

    public sealed class GroupProtocol
    {
        public required string Name { get; init; }
        public required byte[] Metadata { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        writer.WriteInt16((short)ApiKey);
        writer.WriteInt16(ApiVersion);
        writer.WriteInt32(CorrelationId);
        writer.WriteString(ClientId);

        writer.WriteString(GroupId);
        writer.WriteInt32(SessionTimeoutMs);

        if (ApiVersion >= 1)
        {
            writer.WriteInt32(RebalanceTimeoutMs);
        }

        writer.WriteString(MemberId);

        if (ApiVersion >= 5)
        {
            writer.WriteString(GroupInstanceId);
        }

        writer.WriteString(ProtocolType);

        writer.WriteArray(Protocols, protocol =>
        {
            writer.WriteString(protocol.Name);
            writer.WriteBytes(protocol.Metadata);
        });
    }

    public static JoinGroupRequest ReadFrom(KafkaProtocolReader reader, short apiVersion, int correlationId, string clientId)
    {
        // Version 6+ uses flexible format (compact strings/arrays)
        bool isFlexible = ProtocolVersions.IsFlexible(ApiKey.JoinGroup, apiVersion);

        var groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        var sessionTimeoutMs = reader.ReadInt32();
        var rebalanceTimeoutMs = apiVersion >= 1 ? reader.ReadInt32() : sessionTimeoutMs;
        var memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        string? groupInstanceId = apiVersion >= 5
            ? (isFlexible ? reader.ReadCompactString() : reader.ReadString())
            : null;
        var protocolType = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        GroupProtocol[] protocols;
        if (isFlexible)
        {
            // Compact array: length+1 as varint, then elements
            protocols = reader.ReadCompactArray(() =>
            {
                var name = reader.ReadCompactString();
                var metadata = reader.ReadCompactBytes();

                // Skip tagged fields for each protocol
                var protocolTags = reader.ReadVarInt();
                for (int j = 0; j < protocolTags; j++)
                {
                    var tag = reader.ReadVarInt();
                    var size = reader.ReadVarInt();
                    reader.Skip(size);
                }

                return new GroupProtocol
                {
                    Name = name ?? string.Empty,
                    Metadata = metadata ?? Array.Empty<byte>()
                };
            });
        }
        else
        {
            // Regular array: length as int32, then elements
            protocols = reader.ReadArray(() =>
            {
                var name = reader.ReadString();
                var metadata = reader.ReadBytes();
                return new GroupProtocol
                {
                    Name = name ?? string.Empty,
                    Metadata = metadata ?? Array.Empty<byte>()
                };
            });
        }

        // Skip tagged fields for flexible versions
        if (isFlexible)
        {
            var taggedFieldCount = reader.ReadVarInt();
            for (int i = 0; i < taggedFieldCount; i++)
            {
                var tag = reader.ReadVarInt();
                var size = reader.ReadVarInt();
                reader.Skip(size);
            }
        }

        return new JoinGroupRequest
        {
            ApiKey = ApiKey.JoinGroup,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
            GroupId = groupId ?? string.Empty,
            SessionTimeoutMs = sessionTimeoutMs,
            RebalanceTimeoutMs = rebalanceTimeoutMs,
            MemberId = memberId ?? string.Empty,
            GroupInstanceId = groupInstanceId,
            ProtocolType = protocolType ?? string.Empty,
            Protocols = protocols
        };
    }
}

/// <summary>
/// JoinGroup response
/// </summary>
public sealed class JoinGroupResponse : KafkaResponse
{
    public required ErrorCode ErrorCode { get; init; }
    public required int GenerationId { get; init; }
    public required string ProtocolName { get; init; }
    public required string Leader { get; init; }
    public required string MemberId { get; init; }
    public required JoinGroupMember[] Members { get; init; }

    public sealed class JoinGroupMember
    {
        public required string MemberId { get; init; }
        public string? GroupInstanceId { get; init; }
        public required byte[] Metadata { get; init; }
    }

    public override void WriteTo(KafkaProtocolWriter writer)
    {
        // Response header
        writer.WriteInt32(CorrelationId);
        bool isFlexible = ApiVersion >= 6;
        if (isFlexible)
        {
            writer.WriteVarInt(0); // Response header tagged fields
        }

        // Response body per Kafka protocol spec:
        // Version 0-1: error_code, generation_id, protocol_name, leader, member_id, members
        // Version 2+: throttle_time_ms, error_code, generation_id, protocol_name, leader, member_id, members
        // Version 6+: Flexible format with compact strings and tagged fields

        if (ApiVersion >= 2)
        {
            writer.WriteInt32(0); // ThrottleTimeMs
        }

        writer.WriteInt16((short)ErrorCode);
        writer.WriteInt32(GenerationId);

        // Protocol name
        if (ApiVersion >= 6)
        {
            writer.WriteCompactString(ProtocolName);
        }
        else
        {
            writer.WriteString(ProtocolName);
        }

        // Leader
        if (ApiVersion >= 6)
        {
            writer.WriteCompactString(Leader);
        }
        else
        {
            writer.WriteString(Leader);
        }

        // Member ID
        if (ApiVersion >= 6)
        {
            writer.WriteCompactString(MemberId);
        }
        else
        {
            writer.WriteString(MemberId);
        }

        // Members array
        if (ApiVersion >= 6)
        {
            // Compact array for flexible versions
            writer.WriteVarInt(Members.Length + 1);
            foreach (var member in Members)
            {
                writer.WriteCompactString(member.MemberId);

                if (ApiVersion >= 5)
                {
                    writer.WriteCompactString(member.GroupInstanceId);
                }

                writer.WriteCompactBytes(member.Metadata);

                // Tagged fields for each member
                writer.WriteVarInt(0);
            }
        }
        else
        {
            writer.WriteArray(Members, member =>
            {
                writer.WriteString(member.MemberId);

                if (ApiVersion >= 5)
                {
                    writer.WriteString(member.GroupInstanceId);
                }

                writer.WriteBytes(member.Metadata);
            });
        }

        // Tagged fields for flexible versions (v6+)
        if (ApiVersion >= 6)
        {
            writer.WriteVarInt(0); // No tagged fields
        }
    }
}
